"""Robust JSON extraction from LLM responses.

LLMs occasionally wrap their JSON output in markdown code fences or include
prose around it, even when prompted otherwise. This helper recovers the JSON
payload from common wrappers before passing it to ``json.loads``.

Use this everywhere LLM output is decoded, especially when ``response_format``
is not set to ``json_object``.
"""

from __future__ import annotations

import json
import re
from typing import Any


_FENCE_JSON = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)
_FENCE_ANY = re.compile(r"```\s*(.*?)\s*```", re.DOTALL)


def extract_json_text(raw: str) -> str:
    """Return the most likely JSON payload string from LLM output.

    Falls back to the original string if no obvious wrapping is detected.
    """
    if not raw:
        return ""

    text = raw.strip()

    if "```" in text:
        m = _FENCE_JSON.search(text)
        if m:
            return m.group(1).strip()
        m = _FENCE_ANY.search(text)
        if m:
            return m.group(1).strip()

    first_obj = text.find("{")
    last_obj = text.rfind("}")
    first_arr = text.find("[")
    last_arr = text.rfind("]")

    candidates: list[str] = []
    if first_obj != -1 and last_obj > first_obj:
        candidates.append(text[first_obj : last_obj + 1])
    if first_arr != -1 and last_arr > first_arr:
        arr = text[first_arr : last_arr + 1]
        if text.lstrip().startswith("[") or len(arr) > 50:
            candidates.append(arr)

    if candidates:
        return max(candidates, key=len).strip()

    return text


def loads(raw: str) -> Any:
    """``json.loads`` with markdown unwrapping and lightweight repair.

    Raises ``json.JSONDecodeError`` if recovery is impossible.
    """
    extracted = extract_json_text(raw)
    try:
        return json.loads(extracted)
    except json.JSONDecodeError:
        repaired = _repair(extracted)
        return json.loads(repaired)


def _repair(text: str) -> str:
    """Best-effort repair of common LLM JSON mistakes."""
    t = text
    # Strip line comments and block comments
    t = re.sub(r"(?m)^\s*//.*?$", "", t)
    t = re.sub(r"/\*.*?\*/", "", t, flags=re.DOTALL)
    # Strip trailing commas before closing brackets
    t = re.sub(r",\s*}", "}", t)
    t = re.sub(r",\s*]", "]", t)
    return t.strip()
