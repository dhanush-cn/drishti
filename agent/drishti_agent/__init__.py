"""Drishti's root-cause agent (docs/evals.md).

agent.py     the tool-calling investigation loop (LangGraph comes in Week 6)
baseline.py  same model, one prompt, a fixed dump, no tools
api.py       the query API client and the tool definitions the model sees
llm.py       model providers: OpenAI-compatible (Ollama, hosted), Anthropic, scripted (tests)
models.py    typed answers and tool results
evals/       scenarios, labels, fault gate, scoring, the runner and its report
"""
