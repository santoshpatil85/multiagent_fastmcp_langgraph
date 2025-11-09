import os
import re
from fastmcp import tool
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts import PromptTemplate

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_BASE = os.getenv("AZURE_OPENAI_BASE")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01")

SQL_PROMPT = '''You are an expert SQL developer.
Use only the tables in the metadata snippet.

User question:
{user_question}

Metadata:
{metadata_snippet}

Return one SELECT statement only.'''

@tool(name="sql.generate_sql", description="Generate SQL query from user question and metadata")
def generate_sql(user_question: str, metadata_snippet: str) -> str:
    llm = AzureChatOpenAI(
        deployment_name=AZURE_OPENAI_DEPLOYMENT,
        openai_api_key=AZURE_OPENAI_API_KEY,
        openai_api_base=AZURE_OPENAI_BASE,
        openai_api_version=AZURE_OPENAI_API_VERSION,
        temperature=0
    )
    prompt = PromptTemplate(input_variables=["user_question", "metadata_snippet"], template=SQL_PROMPT)
    sql_text = llm.invoke(prompt.format(user_question=user_question, metadata_snippet=metadata_snippet)).content.strip()
    if ";" in sql_text: sql_text = sql_text.split(";")[0]
    if not re.match(r"(?i)^select", sql_text): sql_text = "-- invalid SQL"
    return sql_text
