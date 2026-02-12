# Orchestrator Service

The **Orchestrator Service** is the "Brain" of Project Curiosity. It holds the user session, maintains the "Schema Scratchpad," and drives the reasoning loop using LangGraph.

## Responsibilities
- Reasoning about user queries
- Managing the "Schema Scratchpad" state
- Coordinating with the Schema Explorer and Data Executor services via MCP

## Setup
1. Create a virtual environment: `python -m venv .venv`
2. Install dependencies: `pip install -e .`
3. Run the service: `python src/main.py`
