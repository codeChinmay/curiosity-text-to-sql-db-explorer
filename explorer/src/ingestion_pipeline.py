import os
import json
import weaviate
import weaviate.classes.config as wvc
from typing import List, Dict, Any, Optional
import httpx
import uuid
from openai import AsyncOpenAI


class IngestionPipeline:
    def __init__(self, use_openai: bool = False):
        self.use_openai = use_openai
        try:
            self.client = weaviate.connect_to_local()
        except Exception as e:
            print(f"Failed to connect to Weaviate: {e}")
            self.client = None
        self.collection_name = "TableSchema"

        openai_api_key = os.getenv("OPENAI_API_KEY")
        self.llm_client = AsyncOpenAI(api_key=openai_api_key) if openai_api_key else None

    # ------------------------------------------------------------------
    # LLM Enrichment
    # ------------------------------------------------------------------

    async def _enrich_table_description(
        self,
        table: Dict[str, Any],
        ddl_raw: str,
    ) -> str:
        """
        Call the OpenAI LLM to generate a rich, human-readable description of a
        database table.  The description goes beyond the raw DDL by inferring:
          - the table's business / domain purpose
          - the semantics of each column
          - data patterns visible in example rows
          - relationships implied by foreign keys
          - how an analyst or query-writer should think about using this table

        Falls back to a structured plain-text summary when no API key is available.
        """
        table_name: str = table["name"]
        columns: List[Dict[str, Any]] = table.get("columns", [])
        foreign_keys: List[Dict[str, Any]] = table.get("foreign_keys", [])

        # --- Sample rows provided by the executor service ---
        sample_rows: List[Dict[str, Any]] = table.get("sample_rows", [])

        # --- Build column details string ---
        col_lines = []
        for col in columns:
            constraints = []
            if col.get("primaryKey"):
                constraints.append("PRIMARY KEY")
            if col.get("notNull") or col.get("nullable") is False:
                constraints.append("NOT NULL")
            if col.get("unique"):
                constraints.append("UNIQUE")
            constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
            col_lines.append(f"  - {col['name']} ({col['type']}){constraint_str}")

        col_section = "\n".join(col_lines) if col_lines else "  (no column info)"

        # --- Build FK details string ---
        fk_lines = [
            f"  - {fk.get('column', '?')} → {fk.get('target_table', '?')}.{fk.get('target_column', '?')}"
            for fk in foreign_keys
        ]
        fk_section = "\n".join(fk_lines) if fk_lines else "  (none)"

        # --- Build sample-rows string ---
        if sample_rows:
            try:
                sample_section = json.dumps(sample_rows, indent=2, default=str)
            except Exception:
                sample_section = str(sample_rows)
        else:
            sample_section = "(no sample rows available)"

        # --- Fallback: no LLM available ---
        if not self.llm_client:
            col_names = ", ".join(c["name"] for c in columns)
            fk_summary = (
                "; ".join(
                    f"{fk.get('column')} references {fk.get('target_table')}"
                    for fk in foreign_keys
                )
                if foreign_keys
                else "none"
            )
            return (
                f"Table '{table_name}' stores records with columns: {col_names}. "
                f"Foreign keys: {fk_summary}. "
                f"(Set OPENAI_API_KEY for LLM-enriched descriptions.)"
            )

        # --- Prompt construction ---
        system_prompt = (
            "You are a database documentation expert. "
            "Given a database table's DDL, column definitions, foreign-key relationships, "
            "and a small set of example rows, produce a concise but information-rich "
            "description of the table. Your description MUST include:\n"
            "1. **Purpose**: What real-world entity or concept this table represents and "
            "its role in the overall system (infer from names, types, and sample data).\n"
            "2. **Key columns**: A brief semantic explanation of the most important columns "
            "(especially PKs, FKs, status/type enums, timestamps).\n"
            "3. **Data patterns**: Any patterns you observe from the example rows "
            "(e.g. value ranges, enum values, date ranges, null frequency).\n"
            "4. **Relationships**: How this table links to others via foreign keys and what "
            "those relationships mean in business terms.\n"
            "5. **Query guidance**: Practical tips for an SQL analyst—typical join paths, "
            "important filters or aggregations, and columns to avoid or handle carefully.\n\n"
            "Write in clear prose (2-5 short paragraphs). Do NOT simply repeat the DDL."
        )

        user_prompt = (
            f"Table name: {table_name}\n\n"
            f"DDL:\n{ddl_raw}\n\n"
            f"Columns:\n{col_section}\n\n"
            f"Foreign keys:\n{fk_section}\n\n"
            f"Example rows (up to 5):\n{sample_section}\n\n"
            "Write the table description now."
        )

        try:
            response = await self.llm_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.2,
                max_tokens=600,
            )
            description = response.choices[0].message.content.strip()
            print(f"[Enrichment] ✓ '{table_name}' description generated ({len(description)} chars)")
            return description
        except Exception as e:
            print(f"[Enrichment] LLM call failed for '{table_name}': {e}. Falling back.")
            col_names = ", ".join(c["name"] for c in columns)
            return (
                f"Table '{table_name}' contains columns: {col_names}. "
                f"Foreign keys: {', '.join(fk_lines) or 'none'}."
            )

    # ------------------------------------------------------------------
    # Schema fetch & collection management
    # ------------------------------------------------------------------

    async def fetch_schema_from_executor(self, executor_url: str):
        async with httpx.AsyncClient() as client:
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
            vectorizer_config=(
                wvc.Configure.Vectorizer.text2vec_openai()
                if self.use_openai
                else wvc.Configure.Vectorizer.none()
            ),
            properties=[
                wvc.Property(name="name", data_type=wvc.DataType.TEXT, skip_vectorization=True),
                wvc.Property(name="description", data_type=wvc.DataType.TEXT),
                wvc.Property(name="ddl_minimal", data_type=wvc.DataType.TEXT),
                wvc.Property(name="ddl_raw", data_type=wvc.DataType.TEXT),
            ],
            references=[
                wvc.ReferenceProperty(name="relatedTables", target_collection=self.collection_name)
            ],
        )

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    async def run(self, executor_url: str = "http://localhost:8082"):
        if not self.client:
            return {"status": "error", "message": "Weaviate not connected"}

        # 1. Fetch raw schema from Java Executor
        try:
            raw_schema = await self.fetch_schema_from_executor(executor_url)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch schema from executor: {e}"}

        # 2. Reset Collection
        try:
            self.create_collection()
        except Exception as e:
            return {"status": "error", "message": f"Failed to create collection: {e}"}

        collection = self.client.collections.get(self.collection_name)

        # 3. Enrich and Insert Tables
        with collection.batch.dynamic() as batch:
            for table in raw_schema:
                table_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, table["name"]))

                # Build DDL strings
                columns = table.get("columns", [])
                col_str = ", ".join(
                    [f"{c['name']} {c['type']}" for c in columns]
                )
                ddl_minimal = f"TABLE {table['name']} ({col_str})"

                # Full DDL with constraints
                col_full_lines = []
                for c in columns:
                    parts = [c["name"], c["type"]]
                    if c.get("primaryKey"):
                        parts.append("PRIMARY KEY")
                    if c.get("notNull") or c.get("nullable") is False:
                        parts.append("NOT NULL")
                    col_full_lines.append("  " + " ".join(parts))
                fks = table.get("foreign_keys", [])
                for fk in fks:
                    col_full_lines.append(
                        f"  FOREIGN KEY ({fk.get('column')}) "
                        f"REFERENCES {fk.get('target_table')}({fk.get('target_column', 'id')})"
                    )
                ddl_raw = (
                    f"CREATE TABLE {table['name']} (\n"
                    + ",\n".join(col_full_lines)
                    + "\n);"
                )

                # LLM-enriched description (sample_rows come from executor)
                description = await self._enrich_table_description(
                    table=table,
                    ddl_raw=ddl_raw,
                )

                batch.add_object(
                    properties={
                        "name": table["name"],
                        "description": description,
                        "ddl_minimal": ddl_minimal,
                        "ddl_raw": ddl_raw,
                    },
                    uuid=table_uuid,
                )

        # 4. Add References (Foreign Keys)
        with collection.batch.dynamic() as batch:
            for table in raw_schema:
                source_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, table["name"]))
                fks = table.get("foreign_keys", [])
                for fk in fks:
                    target_table = fk.get("target_table")
                    if target_table:
                        target_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, target_table))
                        batch.add_reference(
                            from_uuid=source_uuid,
                            from_property="relatedTables",
                            to=target_uuid,
                        )

        return {"status": "success", "tables_ingested": len(raw_schema)}
