import os
import requests
import json
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langgraph.graph.state import StateGraph, END

app = FastAPI(title="Distributed Orchestrator")

# Simple in-memory registry: tool_name -> {"url": base_url, "health": True}
registry = {}

@app.post("/register")
def register(service_info: Dict[str, str]):
    # service_info expects: {"tool_name": "...", "base_url": "http://host:port"}
    tool_name = service_info.get("tool_name")
    base_url = service_info.get("base_url")
    if not tool_name or not base_url:
        raise HTTPException(status_code=400, detail="tool_name and base_url required")
    registry[tool_name] = {"base_url": base_url}
    return {"status": "registered", "tool_name": tool_name, "base_url": base_url}

def call_agent_tool(tool_name: str, endpoint: str, payload: dict, timeout=60):
    entry = registry.get(tool_name)
    if not entry:
        raise HTTPException(status_code=500, detail=f"Tool {tool_name} not registered")
    url = entry["base_url"].rstrip('/') + endpoint
    r = requests.post(url, json=payload, timeout=timeout)
    if r.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Agent {tool_name} error: {r.text}")
    return r.json()

# Agent call wrappers
def fetch_metadata_node(state):
    payload = {"catalog": state.get("catalog"), "schema": state.get("schema"), "max_tables": 8}
    res = call_agent_tool("metadata.get_metadata", "/metadata", payload)
    state["metadata_snippet"] = res.get("snippet")
    return state

def generate_sql_node(state):
    payload = {"user_question": state["user_question"], "metadata_snippet": state["metadata_snippet"]}
    res = call_agent_tool("sql.generate_sql", "/generate_sql", payload)
    state["sql"] = res.get("sql")
    return state

def execute_sql_node(state):
    payload = {"sql": state["sql"], "max_rows": state.get("max_rows", 200)}
    res = call_agent_tool("executor.run_sql", "/execute", payload)
    state["rows"] = res.get("rows", [])
    return state

def interpret_node(state):
    payload = {"user_question": state["user_question"], "sql": state["sql"], "rows": state.get("rows", [])}
    res = call_agent_tool("analyst.answer_from_data", "/interpret", payload)
    state["answer"] = res.get("answer")
    return state

# Build LangGraph state graph
graph = StateGraph()
graph.add_node("fetch_metadata", fetch_metadata_node)
graph.add_node("generate_sql", generate_sql_node)
graph.add_node("execute_sql", execute_sql_node)
graph.add_node("interpret", interpret_node)
graph.set_entry_point("fetch_metadata")
graph.add_edge("fetch_metadata", "generate_sql")
graph.add_edge("generate_sql", "execute_sql")
graph.add_edge("execute_sql", "interpret")
graph.add_edge("interpret", END)
workflow = graph.compile()

class AskReq(BaseModel):
    user_question: str
    catalog: str = None
    schema: str = None
    max_rows: int = 200

@app.post("/ask")
def ask(req: AskReq):
    init = {"user_question": req.user_question, "catalog": req.catalog, "schema": req.schema, "max_rows": req.max_rows}
    result = workflow.invoke(init)
    return {"sql": result.get("sql"), "rows": result.get("rows"), "answer": result.get("answer")}

@app.get("/registry")
def get_registry():
    return registry
