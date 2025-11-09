import os, time, requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from databricks import sql

DATABRICKS_SQL_SERVER_HOSTNAME = os.getenv('DATABRICKS_SQL_SERVER_HOSTNAME')
DATABRICKS_SQL_HTTP_PATH = os.getenv('DATABRICKS_SQL_HTTP_PATH')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
ORCH_REG = os.getenv('ORCHESTRATOR_REG_URL')

app = FastAPI(title='Exec Agent')

class ExecReq(BaseModel):
    sql: str
    max_rows: int = 200

@app.post('/execute')
def execute(req: ExecReq):
    if ';' in req.sql or not req.sql.strip().lower().startswith('select'):
        raise HTTPException(status_code=400, detail='Unsafe SQL')
    try:
        with sql.connect(
            server_hostname=DATABRICKS_SQL_SERVER_HOSTNAME,
            http_path=DATABRICKS_SQL_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(req.sql)
                cols = [c[0] for c in cur.description] if cur.description else []
                rows = cur.fetchmany(req.max_rows)
                return {'rows':[dict(zip(cols,r)) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def register():
    if not ORCH_REG: return
    try:
        requests.post(ORCH_REG, json={'tool_name':'executor.run_sql','base_url':'http://exec_agent:8103'})
    except Exception as e:
        print('reg fail', e)

time.sleep(1)
register()
