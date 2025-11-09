# Distributed FastMCP + LangGraph Multi-Agent Example

This project demonstrates a distributed setup where each agent (metadata, sql, executor, analyst)
runs as its own microservice and registers with a central orchestrator. The orchestrator keeps a
service registry and invokes agents over HTTP. The pipeline is orchestrated using LangGraph.

WARNING: This is an example for local development and demonstration. Harden validation, auth,
and secrets handling before production use.

Services:
- orchestrator: central service + LangGraph orchestrator + registry
- metadata_agent: registers tool 'metadata.get_metadata'
- sql_agent: registers tool 'sql.generate_sql'
- exec_agent: registers tool 'executor.run_sql'
- analyst_agent: registers tool 'analyst.answer_from_data'

Run with:
- Set env vars in docker-compose.yml or your environment (Databricks and Azure OpenAI credentials).
- `docker-compose up --build`
- POST to orchestrator `/ask` to run a pipeline.

Example:
curl -X POST http://localhost:8000/ask -H "Content-Type: application/json" -d '{"user_question":"Total sales last month by region?","catalog":"sales","schema":"public"}'
