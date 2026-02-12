# Schema Explorer Service

The **Schema Explorer Service** is the "Librarian" of Project Antigravity. It wraps the Weaviate Vector Database and exposes tools to navigate the database schema as a Knowledge Graph.

## Responsibilities
- Semantic search (Vector + Keyword)
- Foreign Key graph traversal
- Retrieving column-level samples and DDL

## Setup
1. Create a virtual environment: `python -m venv .venv`
2. Install dependencies: `pip install -e .`
3. Run the service: `uvicorn src.server:app --reload`
