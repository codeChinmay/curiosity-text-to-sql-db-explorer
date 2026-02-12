import weaviate
import weaviate.classes.config as wvc
from typing import List, Dict, Any
import httpx
import uuid

class IngestionPipeline:
    def __init__(self, use_openai: bool = False):
        self.use_openai = use_openai
        try:
            self.client = weaviate.connect_to_local()
        except Exception as e:
            print(f"Failed to connect to Weaviate: {e}")
            self.client = None
        self.collection_name = "TableSchema"

    async def fetch_schema_from_executor(self, executor_url: str):
        async with httpx.AsyncClient() as client:
            # Assuming the endpoint is correct based on Java implementation
            response = await client.post(f"{executor_url}/mcp/refresh_schema_metadata", json={})
            response.raise_for_status()
            return response.json()

    def create_collection(self):
        if not self.client:
            return
        
        if self.client.collections.exists(self.collection_name):
            self.client.collections.delete(self.collection_name)
        
        self.client.collections.create(
            name=self.collection_name,
            # Use OpenAI vectorizer if enabled, else none
            vectorizer_config=wvc.Configure.Vectorizer.text2vec_openai() if self.use_openai else wvc.Configure.Vectorizer.none(),
            properties=[
                wvc.Property(name="name", data_type=wvc.DataType.TEXT, skip_vectorization=True),
                wvc.Property(name="description", data_type=wvc.DataType.TEXT),
                wvc.Property(name="ddl_minimal", data_type=wvc.DataType.TEXT),
                wvc.Property(name="ddl_raw", data_type=wvc.DataType.TEXT),
            ],
            references=[
                wvc.ReferenceProperty(name="relatedTables", target_collection=self.collection_name)
            ]
        )

    async def run(self, executor_url: str = "http://localhost:8082"):
        if not self.client:
            return {"status": "error", "message": "Weaviate not connected"}
            
        try:
            # 1. Fetch raw schema from Java Executor
            raw_schema = await self.fetch_schema_from_executor(executor_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch schema from executor: {e}"}
        
        # 2. Reset Collection
        try:
            self.create_collection()
        except Exception as e:
            return {"status": "error", "message": f"Failed to create collection: {e}"}
        
        collection = self.client.collections.get(self.collection_name)
        
        # 3. Insert Tables
        with collection.batch.dynamic() as batch:
            for table in raw_schema:
                table_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, table["name"]))
                # Minimal DDL generation (simplified)
                columns = table.get("columns", [])
                col_str = ", ".join([f"{c['name']} {c['type']}" for c in columns])
                ddl_minimal = f"TABLE {table['name']} ({col_str})"
                
                batch.add_object(
                    properties={
                        "name": table["name"],
                        "description": f"Table {table['name']}", # Placeholder for LLM enrichment
                        "ddl_minimal": ddl_minimal,
                        "ddl_raw": f"CREATE TABLE {table['name']} ..." # Placeholder
                    },
                    uuid=table_uuid
                )
        
        # 4. Add References (Foreign Keys)
        # We need to perform this outside the batch context for references or use batch reference add
        # Batch reference add is preferred for performance
        
        # Re-iterate to add references
        with collection.batch.dynamic() as batch:
             for table in raw_schema:
                source_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, table["name"]))
                fks = table.get("foreign_keys", [])
                for fk in fks:
                    target_table = fk.get("target_table")
                    if target_table:
                        target_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, target_table))
                        # Add reference using batch
                        batch.add_reference(
                            from_uuid=source_uuid,
                            from_property="relatedTables",
                            to=target_uuid
                        )
                    
        return {"status": "success", "tables_ingested": len(raw_schema)}
