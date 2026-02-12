# Data Executor Service

The **Data Executor Service** is the "Muscle" of Project Antigravity. It safely executes SQL against enterprise databases (Postgres, Oracle, SQL Server) and handles schema ingestion.

## Responsibilities
- JDBC connectivity
- Safe, read-only SQL execution
- Schema metadata extraction

## Setup
1. Configure database connection in `src/main/resources/application.properties`.
2. Build the project: `mvn clean install`
3. Run the service: `mvn spring-boot:run`
