import os, re, time, requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts import PromptTemplate

AZURE_OPENAI_API_KEY = os.getenv('AZURE_OPENAI_API_KEY')
AZURE_OPENAI_BASE = os.getenv('AZURE_OPENAI_BASE')
AZURE_OPENAI_DEPLOYMENT = os.getenv('AZURE_OPENAI_DEPLOYMENT')
AZURE_OPENAI_API_VERSION = os.getenv('AZURE_OPENAI_API_VERSION','2024-12-01')
ORCH_REG = os.getenv('ORCHESTRATOR_REG_URL')

app = FastAPI(title='SQL Agent')

class GenReq(BaseModel):
    user_question: str
    metadata_snippet: str

SQL_PROMPT = """You are an expert SQL developer.
Use only the tables in the metadata snippet.

User question:
{user_question}

Metadata:
{metadata_snippet}

Return one SELECT statement only."""

@app.post('/generate_sql')
def generate_sql(req: GenReq):
    llm = AzureChatOpenAI(
        deployment_name=AZURE_OPENAI_DEPLOYMENT,
        openai_api_key=AZURE_OPENAI_API_KEY,
        openai_api_base=AZURE_OPENAI_BASE,
        openai_api_version=AZURE_OPENAI_API_VERSION,
        temperature=0
    )
    prompt = PromptTemplate(input_variables=['user_question','metadata_snippet'], template=SQL_PROMPT)
    text = llm.invoke(prompt.format(user_question=req.user_question, metadata_snippet=req.metadata_snippet)).content.strip()
    if ';' in text: text = text.split(';')[0]
    if not re.match(r'(?i)^select', text): raise HTTPException(status_code=400, detail='Invalid SQL')
    return {'sql': text}

def register():
    if not ORCH_REG: return
    try:
        requests.post(ORCH_REG, json={'tool_name':'sql.generate_sql','base_url':'http://sql_agent:8102'})
    except Exception as e:
        print('reg fail', e)

time.sleep(1)
register()
