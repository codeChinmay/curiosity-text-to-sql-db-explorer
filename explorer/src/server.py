from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from .schema_explorer import SchemaExplorer
from .ingestion_pipeline import IngestionPipeline

app = FastAPI()

# Initialize components
explorer = SchemaExplorer()
pipeline = IngestionPipeline()

class SearchSchemaRequest(BaseModel):
    query: str
    limit: int = 5

class TableNeighborsRequest(BaseModel):
    table_name: str

class TableDDLRequest(BaseModel):
    table_names: List[str]
    minimal: bool = True

class ColumnSamplesRequest(BaseModel):
    table_name: str
    column_name: str

class IngestionRequest(BaseModel):
    executor_url: str = "http://localhost:8082"

@app.get("/")
def read_root():
    return {"status": "Schema Explorer Service is Running"}

@app.post("/tools/search_schema_index")
def search_schema_index(request: SearchSchemaRequest):
    """
    Search schema index (Weaviate).
    """
    return explorer.search_schema(request.query, request.limit)

@app.post("/tools/get_table_neighbors")
def get_table_neighbors(request: TableNeighborsRequest):
    """
    Get table neighbors (Graph Traversal).
    """
    return explorer.get_table_neighbors(request.table_name)

@app.post("/tools/get_table_ddl")
def get_table_ddl(request: TableDDLRequest):
    """
    Get table DDL.
    """
    return explorer.get_table_ddl(request.table_names, request.minimal)

@app.post("/tools/get_column_samples")
def get_column_samples(request: ColumnSamplesRequest):
    """
    Get column samples.
    """
    return explorer.get_column_samples(request.table_name, request.column_name)

@app.post("/ingestion/trigger")
async def trigger_ingestion(request: IngestionRequest):
    """
    Trigger ingestion pipeline.
    """
    return await pipeline.run(request.executor_url)
