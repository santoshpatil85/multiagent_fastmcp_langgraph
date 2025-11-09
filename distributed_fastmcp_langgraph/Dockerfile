FROM python:3.10-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir fastapi uvicorn requests databricks-sql-connector langchain fastmcp langgraph
EXPOSE 8000 8101 8102 8103 8104
