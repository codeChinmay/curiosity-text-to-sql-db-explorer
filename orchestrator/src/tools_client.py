import httpx
import os
from typing import List, Optional, Dict, Any

EXPLORER_URL = os.getenv("EXPLORER_URL", "http://localhost:8081")
EXECUTOR_URL = os.getenv("EXECUTOR_URL", "http://localhost:8082")

async def search_schema(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Search schema index via Explorer Service"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{EXPLORER_URL}/tools/search_schema_index", json={"query": query, "limit": limit})
        resp.raise_for_status()
        return resp.json()

async def get_table_ddl(table_names: List[str], minimal: bool = True) -> Dict[str, str]:
    """Get DDL via Explorer Service"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{EXPLORER_URL}/tools/get_table_ddl", json={"table_names": table_names, "minimal": minimal})
        resp.raise_for_status()
        return resp.json()

async def get_neighbors(table_name: str) -> List[Dict[str, Any]]:
    """Get table neighbors via Explorer Service"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{EXPLORER_URL}/tools/get_table_neighbors", json={"table_name": table_name})
        resp.raise_for_status()
        return resp.json()

async def get_column_samples(table_name: str, column_name: str) -> List[Any]:
    """Get column samples via Explorer Service"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{EXPLORER_URL}/tools/get_column_samples", json={"table_name": table_name, "column_name": column_name})
        resp.raise_for_status()
        return resp.json()

async def execute_query(sql: str) -> List[Dict[str, Any]]:
    """Execute SQL query via Executor Service"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{EXECUTOR_URL}/mcp/execute_sql_query", json={"sql": sql})
        resp.raise_for_status()
        return resp.json()
