import json
import os
from fastmcp import tool
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts import PromptTemplate

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_BASE = os.getenv("AZURE_OPENAI_BASE")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01")

PROMPT = '''User question: {user_question}
SQL: {sql}
Rows: {rows_json}

Give a concise answer and a short interpretation.'''

@tool(name="analyst.answer_from_data", description="Interpret SQL results and answer user's question")
def answer_from_data(user_question: str, sql: str, rows: list) -> str:
    llm = AzureChatOpenAI(
        deployment_name=AZURE_OPENAI_DEPLOYMENT,
        openai_api_key=AZURE_OPENAI_API_KEY,
        openai_api_base=AZURE_OPENAI_BASE,
        openai_api_version=AZURE_OPENAI_API_VERSION,
        temperature=0
    )
    rows_json = json.dumps(rows[:50], default=str)
    prompt = PromptTemplate(input_variables=["user_question","sql","rows_json"], template=PROMPT)
    return llm.invoke(prompt.format(user_question=user_question, sql=sql, rows_json=rows_json)).content.strip()
