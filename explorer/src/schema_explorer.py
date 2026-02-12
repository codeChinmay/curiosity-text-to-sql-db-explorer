import os
import weaviate
import weaviate.classes.query as wvq
import weaviate.classes.config as wvc
import httpx
from typing import List, Dict, Any, Optional

class SchemaExplorer:
    def __init__(self):
        # Allow configuration via env vars
        weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080")
        openai_key = os.getenv("OPENAI_API_KEY")
        
        headers = {}
        if openai_key:
            headers["X-OpenAI-Api-Key"] = openai_key
            
        # Connect to Weaviate
        try:
            self.client = weaviate.connect_to_custom(
                http_host=weaviate_url.replace("http://", "").split(":")[0],
                http_port=int(weaviate_url.split(":")[-1]),
                http_secure=False,
                grpc_host=weaviate_url.replace("http://", "").split(":")[0],
                grpc_port=50051,
                grpc_secure=False,
                headers=headers
            )
            self.collection_name = "TableSchema"
        except Exception as e:
            print(f"Failed to connect to Weaviate: {e}")
            self.client = None

    def search_schema(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Hybrid search for tables based on query.
        """
        if not self.client:
            return []
            
        collection = self.client.collections.get(self.collection_name)
        response = collection.query.hybrid(
            query=query,
            limit=limit,
            return_metadata=wvq.MetadataQuery(score=True)
        )
        
        results = []
        for obj in response.objects:
            results.append({
                "table_name": obj.properties["name"],
                "description": obj.properties.get("description"),
                "relevance_score": obj.metadata.score
            })
        return results

    def get_table_neighbors(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieves related tables via Foreign Key references.
        """
        if not self.client:
            return []

        collection = self.client.collections.get(self.collection_name)
        
        # Find the specific table object
        response = collection.query.fetch_objects(
            filters=weaviate.classes.query.Filter.by_property("name").equal(table_name),
            limit=1,
            return_references=[weaviate.classes.query.QueryReference(link_on="relatedTables", return_properties=["name"])]
        )

        if not response.objects:
            return []

        table_obj = response.objects[0]
        neighbors = []
        
        # Check for outgoing references
        if table_obj.references.get("relatedTables"):
            for ref in table_obj.references["relatedTables"].objects:
                 neighbors.append({
                     "related_table": ref.properties["name"],
                     "relationship_type": "FK", # Simplified
                     "join_condition": "unknown" 
                 })
        
        return neighbors

    def get_table_ddl(self, table_names: List[str], minimal: bool = True) -> Dict[str, str]:
        """
        Retrieves DDL for specified tables.
        """
        if not self.client:
            return {}

        collection = self.client.collections.get(self.collection_name)
        results = {}
        
        for name in table_names:
            response = collection.query.fetch_objects(
                filters=weaviate.classes.query.Filter.by_property("name").equal(name),
                limit=1
            )
            if response.objects:
                prop = "ddl_minimal" if minimal else "ddl_raw"
                results[name] = response.objects[0].properties.get(prop, "")
        
        return results

    async def get_column_samples(self, table_name: str, column_name: str) -> List[Any]:
        """
        Retrieves column samples by querying the Executor Service.
        """
        executor_url = os.getenv("EXECUTOR_URL", "http://localhost:8082")
        
        sql = f"SELECT {column_name} FROM {table_name} LIMIT 5"
        
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(f"{executor_url}/mcp/execute_sql_query", json={"sql": sql})
                resp.raise_for_status()
                data = resp.json() 
                return [row.get(column_name) for row in data]
            except Exception as e:
                print(f"Error fetching samples: {e}")
                return []

    async def sync_schema(self) -> Dict[str, Any]:
        """
        Fetches schema from Executor Service and indexes it into Weaviate.
        """
        executor_url = os.getenv("EXECUTOR_URL", "http://localhost:8082")
        
        # 1. Fetch Schema
        try:
             async with httpx.AsyncClient() as client:
                resp = await client.get(f"{executor_url}/inspect_schema")
                resp.raise_for_status()
                schema_data = resp.json() # List[Map]
        except Exception as e:
             return {"status": "error", "message": f"Failed to fetch schema from Executor: {str(e)}"}
             
        if not self.client:
             return {"status": "error", "message": "Weaviate client not connected"}
             
        # 2. Re-create Collection
        try:
            if self.client.collections.exists(self.collection_name):
                self.client.collections.delete(self.collection_name)
                
            self.client.collections.create(
                name=self.collection_name,
                properties=[
                    wvc.Property(name="name", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="description", data_type=wvc.DataType.TEXT),
                    wvc.Property(name="ddl_minimal", data_type=wvc.DataType.TEXT), 
                    wvc.Property(name="ddl_raw", data_type=wvc.DataType.TEXT), 
                ],
                vectorizer_config=wvc.Configure.Vectorizer.text2vec_openai(),
                generative_config=wvc.Configure.Generative.openai()
            )
        except Exception as e:
             return {"status": "error", "message": f"Failed to create Weaviate collection: {str(e)}"}

        # 3. Index Tables
        collection = self.client.collections.get(self.collection_name)
        indexed_count = 0
        
        for table in schema_data:
             table_name = table.get("name")
             columns = table.get("columns", [])
             
             # Construct DDL
             ddl_minimal = self._construct_ddl(table_name, columns, minimal=True)
             ddl_raw = self._construct_ddl(table_name, columns, minimal=False)
             
             # Insert
             collection.data.insert(
                 properties={
                     "name": table_name,
                     "description": f"Table {table_name} with columns: " + ", ".join([c['name'] for c in columns]),
                     "ddl_minimal": ddl_minimal,
                     "ddl_raw": ddl_raw
                 }
             )
             indexed_count += 1
             
        return {"status": "success", "indexed_tables": indexed_count}

    def _construct_ddl(self, table_name: str, columns: List[Dict[str, str]], minimal: bool) -> str:
        """Helper to construct CREATE TABLE statement."""
        lines = [f"CREATE TABLE {table_name} ("]
        for col in columns:
            col_def = f"  {col['name']} {col['type']}"
            lines.append(col_def)
        lines.append(");")
        return "\n".join(lines)
