"""Typed answers, so a diagnosis is checked before it is scored.

The model's final answer must parse into Diagnosis. Anything that doesn't
is recorded as an invalid answer (scored wrong), never repaired by guessing.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


def _as_list(v) -> list:
    """Lists of observations, accepted in the shapes models actually write.
    Qwen 2.5 wrote ruled_out as {"email": "no errors", ...} in all 60 baseline
    replies of the first capture 2 run; rejecting that threw away valid answers
    (scored 0/30 for the wrong reason). The content is kept, never invented."""
    if v is None:
        return []
    if isinstance(v, str):
        return [v] if v.strip() else []
    if isinstance(v, dict):
        return [f"{k}: {x}" for k, x in v.items()]
    if isinstance(v, (list, tuple)):
        return [x if isinstance(x, str) else str(x) for x in v]
    return [str(v)]


class FaultType(str, Enum):
    error = "error"          # requests fail
    latency = "latency"      # requests slow
    cpu = "cpu"              # CPU saturation
    memory = "memory"        # memory growth / leak
    traffic = "traffic"      # load surge or drop, nothing broken
    unknown = "unknown"


class Diagnosis(BaseModel):
    """The answer the agent submits (and the baseline writes)."""

    root_cause_service: Optional[str] = Field(
        None, description="The service whose fault caused the incident; null if unsure.")
    fault_type: FaultType = Field(FaultType.unknown, description="What kind of fault.")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="0..1: how sure, given the evidence.")
    ranked_services: list[str] = Field(
        default_factory=list, description="Most likely root-cause services, best first (up to 3).")
    evidence: list[str] = Field(default_factory=list, description="Observations that support the answer.")
    ruled_out: list[str] = Field(default_factory=list, description="Hypotheses checked and rejected, with why.")
    summary: str = Field("", description="One or two sentences for the on-call engineer.")

    @field_validator("root_cause_service")
    @classmethod
    def _norm_service(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip()
        if v.lower() in ("", "none", "null", "unknown", "unsure"):
            return None
        return v

    @field_validator("evidence", "ruled_out", mode="before")
    @classmethod
    def _lists(cls, v):
        return _as_list(v)

    @field_validator("confidence", mode="before")
    @classmethod
    def _conf(cls, v):
        # "0.8" and "80%" are 0.8. A bare number above 1 stays invalid (is 5
        # "5%" or a 0-10 scale?): ambiguous answers are recorded, not repaired.
        if isinstance(v, str):
            t = v.strip()
            try:
                return float(t.rstrip("%")) / 100 if t.endswith("%") else float(t)
            except ValueError:
                return v
        return v

    @field_validator("ranked_services", mode="before")
    @classmethod
    def _ranked_shape(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",")]
        if isinstance(v, dict):
            return list(v.keys())
        return v

    @field_validator("ranked_services")
    @classmethod
    def _norm_ranked(cls, v: list[str]) -> list[str]:
        out = []
        for s in v:
            s = (s or "").strip()
            if s and s not in out:
                out.append(s)
        return out[:3]

    def top3(self) -> list[str]:
        """The answer's top 3: root cause first, then the ranked list."""
        out = [self.root_cause_service] if self.root_cause_service else []
        for s in self.ranked_services:
            if s not in out:
                out.append(s)
        return out[:3]

    @classmethod
    def unsure(cls, why: str, ruled_out: list[str] | None = None) -> "Diagnosis":
        return cls(root_cause_service=None, fault_type=FaultType.unknown, confidence=0.0,
                   ruled_out=ruled_out or [], summary=why)


class Usage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0

    def add(self, other: "Usage") -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens


class ToolRecord(BaseModel):
    name: str
    arguments: dict
    ok: bool
    chars: int  # size of the result the model saw
    ms: int


class RunResult(BaseModel):
    """One investigation (agent or baseline), as recorded by the harness."""

    system: str                      # agent | baseline
    diagnosis: Diagnosis
    valid_answer: bool               # the final answer parsed into Diagnosis
    budget_exhausted: bool = False
    tool_calls: list[ToolRecord] = Field(default_factory=list)
    llm_calls: int = 0
    usage: Usage = Field(default_factory=Usage)
    seconds: float = 0.0
    error: Optional[str] = None      # provider or harness failure (scored wrong, reported)
    transcript: list[dict] = Field(default_factory=list)
