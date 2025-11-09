import os
import requests
from fastmcp import tool

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

@tool(name="metadata.get_metadata", description="Fetch a compact metadata snippet from Unity Catalog")
def get_metadata(catalog: str = None, schema: str = None, max_tables: int = 8) -> str:
    endpoint = f"{DATABRICKS_HOST.rstrip('/')}/api/2.1/unity-catalog/tables"
    params = {}
    if catalog: params["catalog_name"] = catalog
    if schema: params["schema_name"] = schema
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    resp = requests.get(endpoint, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    tables = resp.json().get("tables", [])
    lines = [f"- {t.get('full_name')}" for t in tables[:max_tables]]
    return "Unity Catalog tables:\n" + "\n".join(lines) if lines else "No tables found."
