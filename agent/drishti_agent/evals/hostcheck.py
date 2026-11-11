"""Two-turn tool-call check against a hosted model, before a long eval run.

    python -m drishti_agent.evals.hostcheck --provider openai --model M --base-url URL --api-key-env KEYVAR [--rpm N]

Turn 1 must come back as a tool call; turn 2 sends the tool result back with
the assistant turn exactly as received (thought signatures included) and must
come back as text. Fails in seconds on a wrong key, a wrong model id, a model
that won't call tools, or a provider that rejects the returned turn: the
problems that would otherwise surface one scenario into an hour-long run.
"""
from __future__ import annotations

import argparse
import json
import sys

from ..llm import LLMError, from_args

TOOL = {"name": "get_status", "description": "Current status of one service.",
        "parameters": {"type": "object", "properties": {"service": {"type": "string"}}, "required": ["service"]}}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--provider", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--base-url", default="")
    ap.add_argument("--api-key-env", default="")
    ap.add_argument("--rpm", type=float, default=0.0)
    ap.add_argument("--reasoning-effort", default="")
    ap.add_argument("--max-tokens", type=int, default=2048)
    a = ap.parse_args(argv)
    llm = from_args(a.provider, a.model, a.base_url, a.api_key_env, 0.0, rpm=a.rpm,
                    reasoning_effort=a.reasoning_effort, max_tokens=a.max_tokens)
    msgs = [{"role": "user", "content": "Call get_status for the service named ad. After you see the result, reply with its status word only."}]
    try:
        r1 = llm.chat("You check service status with the tool.", msgs, [TOOL])
        if not r1.tool_calls:
            print(f"FAIL: turn 1 did not call the tool; text: {r1.text[:200]!r}")
            return 1
        c = r1.tool_calls[0]
        extra = sorted({k for tc in (r1.raw or {}).get("tool_calls", []) for k in tc} - {"id", "type", "function", "index"})
        print(f"turn 1: {c.name}({json.dumps(c.arguments)}), tokens {r1.usage.input_tokens}/{r1.usage.output_tokens}"
              f"{', extra fields kept: ' + ', '.join(extra) if extra else ''}")
        msgs.append({"role": "assistant", "content": r1.text,
                     "tool_calls": [{"id": t.id, "name": t.name, "arguments": t.arguments} for t in r1.tool_calls],
                     **({"raw": r1.raw} if r1.raw else {})})
        for t in r1.tool_calls:
            msgs.append({"role": "tool", "tool_call_id": t.id, "name": t.name, "content": json.dumps({"service": "ad", "status": "degraded"})})
        r2 = llm.chat("You check service status with the tool.", msgs, [TOOL])
    except LLMError as e:
        print(f"FAIL: {e}")
        return 1
    print(f"turn 2: {r2.text.strip()[:80]!r}, tokens {r2.usage.input_tokens}/{r2.usage.output_tokens}")
    if r2.tool_calls and not r2.text.strip():
        print("FAIL: turn 2 called a tool again instead of answering")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
