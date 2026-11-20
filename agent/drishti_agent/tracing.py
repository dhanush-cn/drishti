"""LangSmith tracing of model calls (design F7; docs/agent.md section 4).

Off unless LANGSMITH_TRACING is true and LANGSMITH_API_KEY is set: then traced(llm) returns the model object itself,
nothing is imported, and nothing leaves the machine (the evals' default). On, every llm.chat becomes an "llm" run
with its inputs (system prompt, messages, tools offered), its output (text, tool calls) and token usage, nested under
the graph's node runs, which LangGraph traces by itself. What is sent is demo telemetry and prompts, nothing else.

Model calls stay in llm.py (Gemini's thought signatures go back verbatim), so this wraps them instead of using a
LangChain chat model. Cost per incident still comes from our own token counts, not from LangSmith.
"""
from __future__ import annotations

import os


def enabled() -> bool:
    return os.environ.get("LANGSMITH_TRACING", "").lower() in ("true", "1") and bool(os.environ.get("LANGSMITH_API_KEY"))


def traced(llm):
    """The model, with each chat call traced when tracing is on; the same object when it is off."""
    if not enabled():
        return llm
    from langsmith import traceable

    class Traced:
        name = getattr(llm, "name", type(llm).__name__)

        def __init__(self, inner):
            self.inner = inner

        def chat(self, system, messages, tools=None):
            box = {}

            @traceable(run_type="llm", name=f"chat {self.name}",
                       metadata={"ls_provider": self.name.split(":")[0], "ls_model_name": self.name})
            def call(system, messages, tools):
                reply = self.inner.chat(system, messages, tools)
                box["reply"] = reply
                return {"text": reply.text,
                        "tool_calls": [{"name": c.name, "arguments": c.arguments} for c in reply.tool_calls],
                        "usage_metadata": {"input_tokens": reply.usage.input_tokens, "output_tokens": reply.usage.output_tokens,
                                           "total_tokens": reply.usage.input_tokens + reply.usage.output_tokens}}

            call(system, messages, tools)
            return box["reply"]

    return Traced(llm)


def flush() -> None:
    """Send buffered traces before a short-lived process exits (the eval runner, the CLI)."""
    if not enabled():
        return
    try:
        # LangGraph's node runs go through LangChain's tracer queue; the model runs through langsmith's client.
        from langchain_core.tracers.langchain import wait_for_all_tracers
        wait_for_all_tracers()
        from langsmith.run_trees import get_cached_client
        get_cached_client().flush()
    except Exception:  # tracing must never fail an investigation
        pass
