#!/usr/bin/env bash
# The agent service (docs/agent.md section 8), natively in WSL: start it, open the dashboard with
# DRISHTI_AGENT_URL=http://127.0.0.1:8092, and every open alert gets an Investigate link.
#
#   scripts/agent-service.sh                 # Ollama (qwen2.5:7b) by default
#   AGENT_PROVIDER=gemini AGENT_MODEL=<id> GEMINI_API_KEY=... scripts/agent-service.sh
#
# Natively, not in a container: Docker Desktop's host gateway reaches Windows, not the WSL distro (Week 1), and Ollama
# runs in WSL. Here the query API (8090), Postgres (5434), the demo's flag API (8080) and Ollama (11434) are all on
# localhost. Needs: docker compose up -d postgres queryapi (and the demo, for remediation).
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -x agent/.venv/bin/python ]] || ! cmp -s agent/pyproject.toml agent/.venv/pyproject.installed; then
  [[ -x agent/.venv/bin/python ]] || python3 -m venv agent/.venv
  agent/.venv/bin/pip install -q -e agent
  cp agent/pyproject.toml agent/.venv/pyproject.installed
fi
if [[ "${AGENT_PROVIDER:-ollama}" == "gemini" ]]; then
  export AGENT_PROVIDER=openai AGENT_BASE_URL="${AGENT_BASE_URL:-https://generativelanguage.googleapis.com/v1beta/openai}"
  export AGENT_API_KEY_ENV="${AGENT_API_KEY_ENV:-GEMINI_API_KEY}" AGENT_RPM="${AGENT_RPM:-5}" AGENT_REASONING="${AGENT_REASONING:-low}"
  export AGENT_MAX_TOKENS="${AGENT_MAX_TOKENS:-8192}"
fi
export AGENT_PROVIDER="${AGENT_PROVIDER:-ollama}" AGENT_MODEL="${AGENT_MODEL:-qwen2.5:7b}"
if [[ "$AGENT_PROVIDER" == "ollama" ]]; then export AGENT_BASE_URL="${AGENT_BASE_URL:-http://localhost:11434/v1}"; fi
export DRISHTI_QUERYAPI_URL="${DRISHTI_QUERYAPI_URL:-http://127.0.0.1:8090}"
export DRISHTI_AGENT_POSTGRES="${DRISHTI_AGENT_POSTGRES:-postgresql://drishti:drishti_dev@127.0.0.1:5434/drishti_agent}"
export DRISHTI_FLAGS_URL="${DRISHTI_FLAGS_URL:-http://localhost:8080}"
echo "agent service on http://127.0.0.1:${DRISHTI_AGENT_PORT:-8092} (model $AGENT_PROVIDER:$AGENT_MODEL)"
exec agent/.venv/bin/python -m drishti_agent.service
