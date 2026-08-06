"""
LangGraph self-healing agent.

Graph flow:
  START → diagnose → [tool loop] → extract_decision → execute_fix → record → END

Decision framework:
  auto_fix       — safe, reversible fix applied immediately (e.g. delete duplicates)
  needs_approval — fix could affect analytics; Telegram alert sent, waits for human
  escalate       — data loss or unknown cause; immediate alert, no auto-action
"""

import json
import re
from typing import Annotated, TypedDict

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode

from agent.config import ANTHROPIC_API_KEY
from agent.db import get_conn
from agent.fixes import fix_duplicate_rows, fix_schema_drift_uid
from agent.telegram import format_agent_message, send_message
from agent.tools import check_source_file, get_schema_diff, read_dq_results, sample_bad_rows

TOOLS = [read_dq_results, get_schema_diff, sample_bad_rows, check_source_file]

# Hoisted at module level — reused across every tool-loop iteration
_llm = ChatAnthropic(
    model='claude-haiku-4-5-20251001',
    api_key=ANTHROPIC_API_KEY,
).bind_tools(TOOLS)

SYSTEM_PROMPT = """You are a self-healing ETL agent for a crypto trading analytics pipeline.

Your job:
1. Use the available tools to investigate data quality failures
2. Identify the root cause
3. Choose a course of action

Decision framework:
- auto_fix: issue is well-understood and fix is safe + reversible
  (e.g. removing duplicates, confirming a column rename was handled at load time)
- needs_approval: fix could alter analytical results and requires human sign-off
  (e.g. backfilling missing values, changing column semantics)
- escalate: data loss, unknown root cause, or irreversible situation
  (e.g. 90% volume drop, unparseable quantity strings)

When you have enough information, end your response with exactly this JSON block:
```json
{"decision": "auto_fix", "confidence": 0.92, "diagnosis": "15% duplicate rows from double-send", "action": "Delete duplicates keeping lowest id"}
```
"""


class AgentState(TypedDict):
    source_file: str
    date: str
    failed_checks: list[dict]
    messages: Annotated[list[BaseMessage], add_messages]
    decision: str
    confidence: float
    diagnosis: str
    action_taken: str


def diagnose(state: AgentState) -> dict:
    messages = [SystemMessage(content=SYSTEM_PROMPT)] + state['messages']
    response = _llm.invoke(messages)
    return {'messages': [response]}


def should_continue(state: AgentState) -> str:
    last = state['messages'][-1]
    if hasattr(last, 'tool_calls') and last.tool_calls:
        return 'tools'
    return 'extract_decision'


def extract_decision(state: AgentState) -> dict:
    last = state['messages'][-1]
    content = last.content
    if isinstance(content, list):
        text = ' '.join(b.get('text', '') for b in content if isinstance(b, dict))
    else:
        text = str(content)

    match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
    if not match:
        # Fallback: find outermost {...} containing "decision" via brace counting
        start = text.find('{"decision"')
        if start == -1:
            start = text.find('{ "decision"')
        if start != -1:
            depth, end = 0, start
            for i, ch in enumerate(text[start:], start):
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            match = type('m', (), {'group': lambda self, n: text[start:end]})()

    if match:
        raw = match.group(1)
        try:
            data = json.loads(raw)
            return {
                'decision': data.get('decision', 'escalate'),
                'confidence': float(data.get('confidence', 0.5)),
                'diagnosis': data.get('diagnosis', text[:300]),
                'action_taken': data.get('action', ''),
            }
        except (json.JSONDecodeError, ValueError):
            pass

    return {
        'decision': 'escalate',
        'confidence': 0.3,
        'diagnosis': text[:300] or 'No structured response from agent',
        'action_taken': 'Manual review required',
    }


def execute_fix(state: AgentState) -> dict:
    if state['decision'] != 'auto_fix':
        return {}

    results = []
    for check in state['failed_checks']:
        name = check.get('check_name')
        if name == 'duplicate_rows':
            results.append(fix_duplicate_rows(state['source_file']))
        elif name == 'schema_drift':
            results.append(fix_schema_drift_uid(state['source_file']))

    if results:
        return {'action_taken': ' | '.join(results)}
    return {}


def record_and_notify(state: AgentState) -> dict:
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO monitoring.agent_actions
                (trigger_type, diagnosis, decision, action_taken, confidence_score)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                'dq_failure',
                state['diagnosis'],
                state['decision'],
                state['action_taken'],
                state['confidence'],
            ),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    try:
        send_message(format_agent_message(
            state['date'], state['decision'], state['diagnosis'],
            state['action_taken'], state['confidence'],
        ))
    except Exception as e:
        print(f"Telegram notification failed (non-fatal): {e}")
    return {}


def _compile() -> StateGraph:
    g = StateGraph(AgentState)
    g.add_node('diagnose', diagnose)
    g.add_node('tools', ToolNode(TOOLS))
    g.add_node('extract_decision', extract_decision)
    g.add_node('execute_fix', execute_fix)
    g.add_node('record_and_notify', record_and_notify)

    g.set_entry_point('diagnose')
    g.add_conditional_edges('diagnose', should_continue, {
        'tools': 'tools',
        'extract_decision': 'extract_decision',
    })
    g.add_edge('tools', 'diagnose')
    g.add_edge('extract_decision', 'execute_fix')
    g.add_edge('execute_fix', 'record_and_notify')
    g.add_edge('record_and_notify', END)
    return g.compile()


_agent = None


def run_agent(source_file: str, date: str, failed_checks: list[dict]) -> dict:
    global _agent
    if _agent is None:
        _agent = _compile()

    context = (
        f"Investigate DQ failures for source_file='{source_file}' (date: {date}):\n\n"
        f"{json.dumps(failed_checks, indent=2)}\n\n"
        "Use tools to investigate, then output your decision as a JSON block."
    )
    result = _agent.invoke({
        'source_file': source_file,
        'date': date,
        'failed_checks': failed_checks,
        'messages': [HumanMessage(content=context)],
        'decision': '',
        'confidence': 0.0,
        'diagnosis': '',
        'action_taken': '',
    })
    return {
        'decision': result['decision'],
        'confidence': result['confidence'],
        'diagnosis': result['diagnosis'],
        'action_taken': result['action_taken'],
    }
