import os, time, requests, json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts import PromptTemplate

AZURE_OPENAI_API_KEY = os.getenv('AZURE_OPENAI_API_KEY')
AZURE_OPENAI_BASE = os.getenv('AZURE_OPENAI_BASE')
AZURE_OPENAI_DEPLOYMENT = os.getenv('AZURE_OPENAI_DEPLOYMENT')
AZURE_OPENAI_API_VERSION = os.getenv('AZURE_OPENAI_API_VERSION','2024-12-01')
ORCH_REG = os.getenv('ORCHESTRATOR_REG_URL')

app = FastAPI(title='Analyst Agent')

class InterpretReq(BaseModel):
    user_question: str
    sql: str
    rows: list

PROMPT = """User question: {user_question}
SQL: {sql}
Rows: {rows_json}

Provide a concise answer and a short interpretation."""

@app.post('/interpret')
def interpret(req: InterpretReq):
    llm = AzureChatOpenAI(
        deployment_name=AZURE_OPENAI_DEPLOYMENT,
        openai_api_key=AZURE_OPENAI_API_KEY,
        openai_api_base=AZURE_OPENAI_BASE,
        openai_api_version=AZURE_OPENAI_API_VERSION,
        temperature=0
    )
    rows_json = json.dumps(req.rows[:50], default=str)
    prompt = PromptTemplate(input_variables=['user_question','sql','rows_json'], template=PROMPT)
    text = llm.invoke(prompt.format(user_question=req.user_question, sql=req.sql, rows_json=rows_json)).content.strip()
    return {'answer': text}

def register():
    if not ORCH_REG: return
    try:
        requests.post(ORCH_REG, json={'tool_name':'analyst.answer_from_data','base_url':'http://analyst_agent:8104'})
    except Exception as e:
        print('reg fail', e)

time.sleep(1)
register()
