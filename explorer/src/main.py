import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from src.schema_explorer import SchemaExplorer

app = FastAPI(title="Schema Explorer Service")
explorer = SchemaExplorer()

class SearchRequest(BaseModel):
    query: str
    limit: int = 5

class TableDDLRequest(BaseModel):
    table_names: List[str]
    minimal: bool = True

class NeighborRequest(BaseModel):
    table_name: str

class ColumnSampleRequest(BaseModel):
    table_name: str
    column_name: str

@app.get("/")
async def root():
    return {"status": "Schema Explorer Service is Running"}

@app.post("/tools/search_schema_index")
async def search_schema_index(request: SearchRequest):
    try:
        results = explorer.search_schema(request.query, request.limit)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/get_table_ddl")
async def get_table_ddl(request: TableDDLRequest):
    try:
        results = explorer.get_table_ddl(request.table_names, request.minimal)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/get_table_neighbors")
async def get_table_neighbors(request: NeighborRequest):
    try:
        results = explorer.get_table_neighbors(request.table_name)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/get_column_samples")
async def get_column_samples(request: ColumnSampleRequest):
    try:
        results = await explorer.get_column_samples(request.table_name, request.column_name)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/sync_schema")
async def sync_schema():
    try:
        results = await explorer.sync_schema()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
