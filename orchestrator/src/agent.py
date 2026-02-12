import os
import asyncio
from langgraph.graph import StateGraph, END
from typing import Dict, Any, List
from .state import AgentState
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from .tools_client import search_schema, get_table_ddl, execute_query

class Agent:
    def __init__(self):
        self.llm = ChatOpenAI(
            model=os.getenv("OPENAI_MODEL_NAME", "gpt-4o"),
            temperature=0
        )
        self.workflow = self._build_graph()

    def _build_graph(self):
        workflow = StateGraph(AgentState)
        
        workflow.add_node("planner", self.plan_step)
        workflow.add_node("explorer", self.explore_step)
        workflow.add_node("generator", self.generate_step)
        workflow.add_node("executor", self.execute_step)

        workflow.set_entry_point("planner")
        workflow.add_edge("planner", "explorer")
        workflow.add_edge("explorer", "generator")
        workflow.add_edge("generator", "executor")
        workflow.add_edge("executor", END)
        
        return workflow.compile()

    async def plan_step(self, state: AgentState):
        query = state.get('input_query')
        print(f"Planning for query: {query}")
        
        # Simple planning: just identifying intent, but for now passing through.
        # Ideally we ask LLM to extract keywords for search.
        messages = [
            SystemMessage(content="You are a database expert. Extract the key terms from the user query to search for relevant database tables. Return just the terms as a comma-separated list."),
            HumanMessage(content=query)
        ]
        
        try:
            response = await self.llm.ainvoke(messages)
            plan = response.content
            print(f"Plan keywords: {plan}")
            return {"reasoning_log": [f"Plan: Search for {plan}"]}
        except Exception as e:
            print(f"Planning failed: {e}")
            return {"reasoning_log": [f"Planning failed: {e}"]}

    async def explore_step(self, state: AgentState):
        print("Searching schema...")
        query = state.get('input_query')
        try:
            # We search using the original query + potential plan context
            # For simplicity, just use the query directly for hybrid search
            # (In a real app, use the keywords from plan)
            results = await search_schema(query, limit=5)
            
            # Filter solely based on some heuristic or take top N
            # Here we take everything returned by search
            table_names = [r['table_name'] for r in results]
            
            if not table_names:
                return {"relevant_tables": [], "error_message": "No relevant tables found."}

            ddl_map = await get_table_ddl(table_names, minimal=True)
            
            # Format context as structured data
            relevant_tables = []
            for name, ddl in ddl_map.items():
                relevant_tables.append({"name": name, "ddl_minimal": ddl})
            
            return {"relevant_tables": relevant_tables}
        except Exception as e:
            print(f"Explorer step failed: {e}")
            return {"error_message": f"Explorer failed: {str(e)}"}

    async def generate_step(self, state: AgentState):
        print("Generating SQL...")
        if state.get("error_message"):
            return {}

        query = state.get('input_query')
        relevant_tables = state.get('relevant_tables', [])
        
        # Format context from structured state
        context_lines = []
        for t in relevant_tables:
            context_lines.append(f"Table: {t.get('name')}\nDDL:\n{t.get('ddl_minimal')}\n")
        context = "\n".join(context_lines)
        
        if not context:
             return {"error_message": "No context available to generate SQL."}

        prompt = f"""You are a PostgreSQL expert. Write a SQL query to answer the user's request.
        
        Schema Context:
        {context}
        
        User Request: {query}
        
        Rules:
        1. Return ONLY the valid SQL query. Do not include markdown formatting (```sql ... ```).
        2. Use correct PostgreSQL syntax.
        3. Do not invent columns that are not in the schema.
        """
        
        messages = [
            SystemMessage(content="You are a strict SQL generator. Return only SQL."),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = await self.llm.ainvoke(messages)
            sql = response.content.replace("```sql", "").replace("```", "").strip()
            print(f"Generated SQL: {sql}")
            return {"sql_query": sql}
        except Exception as e:
             return {"error_message": f"Generation failed: {str(e)}"}

    async def execute_step(self, state: AgentState):
        if state.get("error_message"):
            return {}
            
        print("Executing SQL...")
        sql = state.get('sql_query')
        try:
             res = await execute_query(sql)
             return {"execution_result": res}
        except Exception as e:
             return {"error_message": f"Execution failed: {str(e)}"}

    async def run(self, input_query: str):
        inputs = {
            "input_query": input_query, 
            "relevant_tables": [], 
            "reasoning_log": [], 
            "sql_query": "", 
            "execution_result": [], 
            "error_message": ""
        }
        
        final_state = inputs
        async for output in self.workflow.astream(inputs):
            for key, value in output.items():
                print(f"Finished step: {key}")
                final_state.update(value)
        return final_state
