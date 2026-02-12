# Text-to-SQL DB Explorer

Project Curiosity: A microservices-based system to analyze database schemas and generate SQL from natural language questions using AI agents.

## Architecture

- **Orchestrator**: Python (LangGraph) service that manages the AI agent workflow (Plan, Explore, Generate, Execute).
- **Explorer**: Python (FastAPI + Weaviate) service for semantic schema search and exploration.
- **Executor**: Java (Spring Boot) service for direct database interaction and schema introspection.
- **Weaviate**: Vector database for schema indexing.
- **PostgreSQL**: Target database for analysis.

## Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Java 21+ (Maven)
- OpenAI API Key

## Getting Started

### 1. Start Infrastructure

Start Weaviate and PostgreSQL (with sample data):

```bash
docker-compose up -d
```

### 2. Configure Environment

Copy `.env.example` to `.env` in `orchestrator/` and `explorer/` directories and add your `OPENAI_API_KEY`.

### 3. Run Services

**Executor (Terminal 1):**
```bash
cd executor
./mvnw spring-boot:run
# or
mvn spring-boot:run
```
(Ensure port 8082 is free)

**Explorer (Terminal 2):**
```bash
cd explorer
pip install -e .
uvicorn src.main:app --reload --port 8081
```

**Initialize Schema Index (One-time):**
Once Executor and Explorer are running, initialize the schema index:
```bash
curl -X POST http://localhost:8081/tools/sync_schema
```

### 4. Run Agent (Orchestrator)

**Orchestrator (Terminal 3):**
```bash
cd orchestrator
pip install -e .
python -m src.main "Show me all users who bought a Laptop"
```

## Development Workflow

1.  **Modify Schema**: Edit `postgres_init/init.sql` and restart postgres container.
2.  **Re-index**: Call `/tools/sync_schema` to update Weaviate.
3.  **Test Queries**: Run the orchestrator CLI.

## Troubleshooting

-   **Weaviate Connection Refused**: Ensure `docker-compose` is running and `WEAVIATE_URL` is correct.
-   **Postgres Connection Refused**: Ensure `POSTGRES_HOST` is implicitly localhost for Executor running on host, or configure if running in Docker.
