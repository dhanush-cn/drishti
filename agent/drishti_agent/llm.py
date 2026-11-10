"""Model providers behind one small interface.

    reply = llm.chat(system, messages, tools)   # tools=None: no tool use

Messages are kept in one neutral form and translated per provider:

    {"role": "user", "content": "..."}
    {"role": "assistant", "content": "...", "tool_calls": [{"id", "name", "arguments": {...}}]}
    {"role": "tool", "tool_call_id": "...", "name": "...", "content": "..."}

Providers:
  OpenAICompat  /v1/chat/completions with tools: Ollama (http://localhost:11434/v1),
                and hosted APIs that speak it.
  Anthropic     the Messages API.
  Scripted      replays canned replies, for tests (no network).

Standard library HTTP only. Retries: provider errors (429, 5xx, timeouts)
are retried with backoff a few times, then raised; the harness records the
run as failed, it never invents an answer.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

from .models import Usage


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict


@dataclass
class Reply:
    text: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)


class LLMError(Exception):
    pass


def _post(url: str, headers: dict, body: dict, timeout: float, retries: int = 4) -> dict:
    data = json.dumps(body).encode()
    last = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json", **headers}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            msg = e.read().decode(errors="replace")[:500]
            last = LLMError(f"HTTP {e.code}: {msg}")
            if e.code not in (408, 409, 429, 500, 502, 503, 504, 529):
                raise last from None
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = LLMError(f"request failed: {e}")
        time.sleep(min(2 ** attempt, 20))
    raise last


def _args(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else {}
    except ValueError:
        return {"_unparsed": str(raw)[:500]}


class OpenAICompat:
    """Chat Completions with tools (OpenAI's wire format, which Ollama serves at /v1)."""

    def __init__(self, model: str, base_url: str, api_key: str = "", temperature: float = 0.0,
                 timeout: float = 300.0, max_tokens: int = 2048, seed: Optional[int] = None):
        self.model, self.base, self.key = model, base_url.rstrip("/"), api_key
        self.temperature, self.timeout, self.max_tokens, self.seed = temperature, timeout, max_tokens, seed
        self.name = f"openai-compat:{model}"

    def chat(self, system: str, messages: list[dict], tools: Optional[list[dict]] = None) -> Reply:
        msgs = [{"role": "system", "content": system}]
        for m in messages:
            if m["role"] == "assistant" and m.get("tool_calls"):
                msgs.append({"role": "assistant", "content": m.get("content") or "",
                             "tool_calls": [{"id": c["id"], "type": "function",
                                             "function": {"name": c["name"], "arguments": json.dumps(c["arguments"])}}
                                            for c in m["tool_calls"]]})
            elif m["role"] == "tool":
                msgs.append({"role": "tool", "tool_call_id": m["tool_call_id"], "content": m["content"]})
            else:
                msgs.append({"role": m["role"], "content": m["content"]})
        body: dict = {"model": self.model, "messages": msgs, "temperature": self.temperature, "max_tokens": self.max_tokens}
        if self.seed is not None:
            body["seed"] = self.seed
        if tools:
            body["tools"] = [{"type": "function", "function": t} for t in tools]
        headers = {"Authorization": f"Bearer {self.key}"} if self.key else {}
        r = _post(f"{self.base}/chat/completions", headers, body, self.timeout)
        try:
            msg = r["choices"][0]["message"]
        except (KeyError, IndexError):
            raise LLMError(f"unexpected response: {str(r)[:300]}") from None
        calls = [ToolCall(id=c.get("id") or f"call_{i}", name=c["function"]["name"], arguments=_args(c["function"].get("arguments")))
                 for i, c in enumerate(msg.get("tool_calls") or [])]
        u = r.get("usage") or {}
        return Reply(text=msg.get("content") or "", tool_calls=calls,
                     usage=Usage(input_tokens=u.get("prompt_tokens", 0), output_tokens=u.get("completion_tokens", 0)))


class Anthropic:
    """The Messages API with tools."""

    def __init__(self, model: str, api_key: str, base_url: str = "https://api.anthropic.com", temperature: float = 0.0,
                 timeout: float = 300.0, max_tokens: int = 2048):
        self.model, self.key, self.base = model, api_key, base_url.rstrip("/")
        self.temperature, self.timeout, self.max_tokens = temperature, timeout, max_tokens
        self.name = f"anthropic:{model}"

    def chat(self, system: str, messages: list[dict], tools: Optional[list[dict]] = None) -> Reply:
        out: list[dict] = []
        for m in messages:
            if m["role"] == "assistant":
                blocks: list[dict] = []
                if m.get("content"):
                    blocks.append({"type": "text", "text": m["content"]})
                for c in m.get("tool_calls") or []:
                    blocks.append({"type": "tool_use", "id": c["id"], "name": c["name"], "input": c["arguments"]})
                out.append({"role": "assistant", "content": blocks})
            elif m["role"] == "tool":
                block = {"type": "tool_result", "tool_use_id": m["tool_call_id"], "content": m["content"]}
                if out and out[-1]["role"] == "user" and isinstance(out[-1]["content"], list):
                    out[-1]["content"].append(block)  # consecutive tool results: one user turn
                else:
                    out.append({"role": "user", "content": [block]})
            else:
                out.append({"role": "user", "content": m["content"]})
        body: dict = {"model": self.model, "system": system, "messages": out, "max_tokens": self.max_tokens,
                      "temperature": self.temperature}
        if tools:
            body["tools"] = [{"name": t["name"], "description": t["description"], "input_schema": t["parameters"]} for t in tools]
        r = _post(f"{self.base}/v1/messages", {"x-api-key": self.key, "anthropic-version": "2023-06-01"}, body, self.timeout)
        text, calls = "", []
        for b in r.get("content", []):
            if b.get("type") == "text":
                text += b.get("text", "")
            elif b.get("type") == "tool_use":
                calls.append(ToolCall(id=b["id"], name=b["name"], arguments=b.get("input") or {}))
        u = r.get("usage") or {}
        return Reply(text=text, tool_calls=calls, usage=Usage(input_tokens=u.get("input_tokens", 0), output_tokens=u.get("output_tokens", 0)))


class Scripted:
    """Replays canned replies in order (tests). Records what it was sent."""

    def __init__(self, replies: list[Reply]):
        self.replies = list(replies)
        self.sent: list[tuple[str, list[dict], Optional[list[dict]]]] = []
        self.name = "scripted"

    def chat(self, system: str, messages: list[dict], tools: Optional[list[dict]] = None) -> Reply:
        self.sent.append((system, json.loads(json.dumps(messages)), tools))
        if not self.replies:
            raise LLMError("scripted model ran out of replies")
        return self.replies.pop(0)


def from_args(provider: str, model: str, base_url: str = "", api_key_env: str = "", temperature: float = 0.0, seed: Optional[int] = None):
    key = os.environ.get(api_key_env, "") if api_key_env else ""
    if provider == "heuristic":
        return Heuristic()
    if provider == "ollama":
        return OpenAICompat(model, base_url or "http://localhost:11434/v1", temperature=temperature, seed=seed)
    if provider == "openai":
        if not base_url:
            raise LLMError("--base-url is required for an OpenAI-compatible hosted provider")
        return OpenAICompat(model, base_url, key, temperature=temperature, seed=seed)
    if provider == "anthropic":
        if not key:
            raise LLMError(f"set the API key in ${api_key_env or 'ANTHROPIC_API_KEY'}")
        return Anthropic(model, key, base_url or "https://api.anthropic.com", temperature=temperature)
    raise LLMError(f"unknown provider {provider!r} (ollama, openai, anthropic, heuristic)")


class Heuristic:
    """Not a model: names the first service the triage overview lists with
    changes, the fault type read from the change text. A floor for the evals
    (an LLM that can't beat "pick the top changed service" isn't earning its
    place) and a way to run the whole harness without a model."""

    name = "heuristic:top-changed-service"

    _KINDS = (("error ratio", "error"), ("memory rising", "memory"), ("CPU", "cpu"), ("p95", "latency"),
              ("server calls", "traffic"), ("log lines", "error"))

    def chat(self, system: str, messages: list[dict], tools: Optional[list[dict]] = None) -> Reply:
        first = messages[0]["content"]
        # The overview's text form (api.format_overview): "- service: change; change" lines.
        changed = []
        i = first.find("services with changes")
        if i >= 0:
            for line in first[i:].splitlines()[1:]:
                if not line.startswith("- "):
                    break
                svc, _, rest = line[2:].partition(": ")
                changed.append({"service": svc, "changes": rest.split("; ")})
        if not changed:
            d = {"root_cause_service": None, "fault_type": "unknown", "confidence": 0.0, "ranked_services": [],
                 "evidence": [], "summary": "no service crossed a threshold"}
        else:
            top = changed[0]
            text = " ".join(top["changes"])
            kind = next((k for needle, k in self._KINDS if needle in text), "unknown")
            d = {"root_cause_service": top["service"], "fault_type": kind, "confidence": 0.5,
                 "ranked_services": [s["service"] for s in changed[:3]], "evidence": top["changes"], "summary": "top changed service"}
        if tools:
            return Reply("", [ToolCall("h1", "submit_diagnosis", d)])
        return Reply(json.dumps(d))
