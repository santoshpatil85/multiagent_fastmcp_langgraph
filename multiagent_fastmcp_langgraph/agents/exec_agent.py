import os
from databricks import sql
from fastmcp import tool

DATABRICKS_SQL_SERVER_HOSTNAME = os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME")
DATABRICKS_SQL_HTTP_PATH = os.getenv("DATABRICKS_SQL_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

@tool(name="executor.run_sql", description="Execute SQL on Databricks and return result rows")
def run_sql(sql_query: str, max_rows: int = 200) -> list:
    if ";" in sql_query or not sql_query.strip().lower().startswith("select"):
        return []
    with sql.connect(
        server_hostname=DATABRICKS_SQL_SERVER_HOSTNAME,
        http_path=DATABRICKS_SQL_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchmany(max_rows)
            return [dict(zip(cols, r)) for r in rows]
