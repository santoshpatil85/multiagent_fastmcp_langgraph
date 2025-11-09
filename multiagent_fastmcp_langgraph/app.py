from fastapi import FastAPI
from pydantic import BaseModel
from fastmcp import FastMCPApp, call_tool
from langgraph.graph import StateGraph, END

import agents.metadata_agent
import agents.sql_agent
import agents.exec_agent
import agents.analyst_agent

app = FastMCPApp(title="Databricks FastMCP Multi-Agent")

class PipelineState(dict): pass
graph = StateGraph(PipelineState)

def metadata_node(state):
    state["metadata_snippet"] = call_tool("metadata.get_metadata", catalog=state.get("catalog"), schema=state.get("schema"))
    return state

def sql_node(state):
    state["sql"] = call_tool("sql.generate_sql", user_question=state["user_question"], metadata_snippet=state["metadata_snippet"])
    return state

def exec_node(state):
    state["rows"] = call_tool("executor.run_sql", sql_query=state["sql"])
    return state

def analyst_node(state):
    state["answer"] = call_tool("analyst.answer_from_data", user_question=state["user_question"], sql=state["sql"], rows=state["rows"])
    return state

graph.add_node("metadata", metadata_node)
graph.add_node("sql", sql_node)
graph.add_node("exec", exec_node)
graph.add_node("analyst", analyst_node)

graph.set_entry_point("metadata")
graph.add_edge("metadata", "sql")
graph.add_edge("sql", "exec")
graph.add_edge("exec", "analyst")
graph.add_edge("analyst", END)

workflow = graph.compile()

fastapi_app = FastAPI(title="FastMCP LangGraph Orchestrator")

class AskRequest(BaseModel):
    user_question: str
    catalog: str = None
    schema: str = None

@fastapi_app.post("/ask")
def ask(req: AskRequest):
    init = {"user_question": req.user_question, "catalog": req.catalog, "schema": req.schema}
    result = workflow.invoke(init)
    return {"sql": result.get("sql"), "answer": result.get("answer"), "rows": result.get("rows")}

app.mount_to(fastapi_app)
