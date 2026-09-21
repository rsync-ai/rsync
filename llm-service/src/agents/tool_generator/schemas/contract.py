"""
Generation Contract — per-dimension confidence model for connector generation.

The contract is the data shape exchanged between the discovery phase and the
generation phase. Each `Fact` carries:

  - `value`:      what we discovered (or None if missing)
  - `confidence`: 0.0..1.0 score for this dimension
  - `source`:     where the value came from (introspection / vendor_yaml / llm / user)
  - `evidence`:   short human-readable explanation

When `can_generate` is False, the caller surfaces `questions[]` to the user
and re-submits with answers via `/v1/discover/{id}/confirm`.

VERSION: 1.0.0
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FactSource(str, enum.Enum):
    """Provenance of a discovered fact (highest trust → lowest)."""
    USER_PROVIDED = "user_provided"
    VENDOR_YAML = "vendor_yaml"          # Tier 1 curated
    LEARNED_API = "learned_api"          # Tier 2 community
    INTROSPECTION = "introspection"      # GraphQL `__schema` probe
    OPENAPI_SPEC = "openapi_spec"
    DOC_PARSE = "doc_parse"              # Scraped from docs HTML
    LLM_INFERENCE = "llm_inference"
    HEURISTIC = "heuristic"
    DEFAULT = "default"
    MISSING = "missing"


class Dimension(str, enum.Enum):
    """The fixed set of dimensions a complete contract describes."""
    VENDOR = "vendor"                    # e.g., "shopify"
    API_VARIANT = "api_variant"          # e.g., "admin-graphql"
    PROTOCOL = "protocol"                # rest | graphql | openapi
    BASE_URL = "base_url"                # endpoint template (with {placeholders})
    AUTH_TYPE = "auth_type"              # bearer | api_key | basic | oauth2 | none
    AUTH_HEADER = "auth_header"          # header name carrying the token
    OPERATIONS = "operations"            # list of operation names to generate
    PAGINATION = "pagination"            # cursor | offset | page | none
    RUNTIME_FIELDS = "runtime_fields"    # ConfigField[] declared, filled at connection time


# Dimensions that MUST be present (with confidence ≥ MIN_CRITICAL_CONF) before
# we'll allow generation. Everything else is best-effort.
CRITICAL_DIMENSIONS = frozenset({
    Dimension.PROTOCOL,
    Dimension.BASE_URL,
    Dimension.AUTH_TYPE,
    Dimension.OPERATIONS,
})

MIN_CRITICAL_CONFIDENCE = 0.6


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Fact(BaseModel):
    """A single discovered fact about the API being generated."""
    dimension: Dimension
    value: Optional[Any] = None
    # Confidence in [0,1]. We clamp silently rather than raising — LLM/probe
    # outputs are noisy and we'd rather coerce than fail.
    confidence: float = 0.0
    source: FactSource = FactSource.MISSING
    evidence: str = ""

    @field_validator("confidence", mode="before")
    @classmethod
    def _clamp(cls, v: Any) -> float:
        try:
            f = float(v)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(1.0, f))

    @property
    def is_present(self) -> bool:
        return self.value is not None and self.source != FactSource.MISSING


class Question(BaseModel):
    """An open question the user must answer before generation can proceed."""
    dimension: Dimension
    prompt: str                          # Human-readable question
    kind: str = "free_text"              # free_text | choice | multi_choice | bool
    choices: Optional[List[Dict[str, str]]] = None  # [{"value": "...", "label": "..."}]
    default: Optional[Any] = None
    required: bool = True
    help_text: Optional[str] = None


class GenerationContract(BaseModel):
    """The full state of a discovery session.

    Workflow:
      1. POST /v1/discover           → contract with facts[] + questions[]
      2. user answers questions
      3. POST /v1/discover/{id}/confirm with answers → updated contract
      4. when can_generate=True → call /v1/generate (uses session_id)
    """
    session_id: str
    api_name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    facts: Dict[Dimension, Fact] = Field(default_factory=dict)
    questions: List[Question] = Field(default_factory=list)

    # Computed by service layer based on facts + critical-dimension thresholds
    can_generate: bool = False
    refusal_reason: Optional[str] = None

    # Free-form metadata (vendor_id, docs_url echo, etc.)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # ---- Helpers --------------------------------------------------------

    def get(self, dim: Dimension) -> Optional[Fact]:
        return self.facts.get(dim)

    def set_fact(self, fact: Fact) -> None:
        self.facts[fact.dimension] = fact
        self.updated_at = datetime.now(timezone.utc)

    def evaluate(self) -> None:
        """Recompute can_generate + refusal_reason from current facts.

        Phase 12: AUTH_TYPE is satisfied when either:
          - the fact is present (legacy single-auth path), OR
          - metadata['supported_auth_methods'] declares ≥1 method
            (multi-auth path; runtime dispatcher handles selection)
        """
        missing: List[str] = []
        low_conf: List[str] = []
        has_supported_methods = bool(self.metadata.get("supported_auth_methods"))

        for dim in CRITICAL_DIMENSIONS:
            # Multi-auth bypass: auth_type isn't required when the connector
            # already declares multiple supported methods at runtime.
            if dim is Dimension.AUTH_TYPE and has_supported_methods:
                continue

            fact = self.facts.get(dim)
            if fact is None or not fact.is_present:
                missing.append(dim.value)
            elif fact.confidence < MIN_CRITICAL_CONFIDENCE:
                low_conf.append(f"{dim.value}({fact.confidence:.2f})")

        if missing or low_conf:
            self.can_generate = False
            parts = []
            if missing:
                parts.append(f"missing: {', '.join(missing)}")
            if low_conf:
                parts.append(f"low confidence: {', '.join(low_conf)}")
            self.refusal_reason = "; ".join(parts)
        else:
            self.can_generate = True
            self.refusal_reason = None

    def to_redis_payload(self) -> str:
        """Serialize for Redis (Pydantic handles enum→str + datetime→iso8601)."""
        return self.model_dump_json()

    @classmethod
    def from_redis_payload(cls, payload: str) -> "GenerationContract":
        return cls.model_validate_json(payload)


# ---------------------------------------------------------------------------
# Convenience factory for a fresh contract
# ---------------------------------------------------------------------------

def empty_contract(session_id: str, api_name: str) -> GenerationContract:
    """Create an empty contract with all dimensions populated as MISSING facts.

    This guarantees a stable shape: callers can always .get(dim) without None-checks.
    """
    contract = GenerationContract(session_id=session_id, api_name=api_name)
    for dim in Dimension:
        contract.set_fact(Fact(dimension=dim))
    contract.evaluate()
    return contract
