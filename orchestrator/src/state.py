from typing import TypedDict, List, Dict, Any, Optional

class TableSchema(TypedDict):
    name: str
    description: Optional[str]
    ddl_minimal: Optional[str]
    ddl_raw: Optional[str]

class AgentState(TypedDict):
    """
    The 'Schema Scratchpad' state for the orchestration agent.
    """
    input_query: str
    
    # Scratchpad
    relevant_tables: List[TableSchema]
    reasoning_log: List[str]
    
    # Validation
    sql_query: Optional[str]
    execution_result: Optional[List[Dict[str, Any]]]
    error_message: Optional[str]
