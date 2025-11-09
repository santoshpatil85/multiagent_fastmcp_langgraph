import os, time, requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
ORCH_REG = os.getenv("ORCHESTRATOR_REG_URL")

app = FastAPI(title="Metadata Agent")

class MetaReq(BaseModel):
    catalog: str = None
    schema: str = None
    max_tables: int = 8

@app.post("/metadata")
def get_metadata(req: MetaReq):
    endpoint = f"{DATABRICKS_HOST.rstrip('/')}/api/2.1/unity-catalog/tables"
    params = {}
    if req.catalog: params['catalog_name'] = req.catalog
    if req.schema: params['schema_name'] = req.schema
    headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}'}
    r = requests.get(endpoint, headers=headers, params=params, timeout=20)
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=r.text)
    tables = r.json().get('tables', [])
    lines = []
    for t in tables[:req.max_tables]:
        fq = t.get('full_name') or f"{t.get('catalog_name')}.{t.get('schema_name')}.{t.get('name')}"
        lines.append(f"- {fq}")
    return {'snippet': 'Unity Catalog snippet:\n' + '\n'.join(lines)} if lines else {'snippet': 'No tables found'}

def register_with_orchestrator():
    if not ORCH_REG: return
    try:
        requests.post(ORCH_REG, json={'tool_name': 'metadata.get_metadata', 'base_url': 'http://metadata_agent:8101'})
    except Exception as e:
        print('Register failed', e)

# try register on startup (best-effort)
time.sleep(1)
register_with_orchestrator()
