"""
Python literal sanitizer

The tool-generator occasionally embeds JSON example payloads directly into generated
Python code (e.g., mock responses). JSON literals use `true/false/null`, which are
valid *identifiers* in Python and therefore pass `ast.parse`, but crash at runtime
with `NameError: name 'false' is not defined`.

We fix this safely by tokenizing the code and replacing only NAME tokens:
  - true  -> True
  - false -> False
  - null  -> None

This does NOT touch strings/comments, so JSON examples inside docstrings remain intact.
"""

from __future__ import annotations

import io
import tokenize
from typing import Dict


_NAME_REPLACEMENTS: Dict[str, str] = {
    "true": "True",
    "false": "False",
    "null": "None",
}


def sanitize_python_json_literals(code: str) -> str:
    """
    Replace JSON-style literals with Python equivalents using tokenization.

    Args:
        code: Python code as text

    Returns:
        Sanitized code. If tokenization fails, returns the original code.
    """
    if not code:
        return code

    try:
        tokens = []
        reader = io.StringIO(code).readline
        for tok in tokenize.generate_tokens(reader):
            if tok.type == tokenize.NAME and tok.string in _NAME_REPLACEMENTS:
                tok = tokenize.TokenInfo(tok.type, _NAME_REPLACEMENTS[tok.string], tok.start, tok.end, tok.line)
            tokens.append(tok)
        return tokenize.untokenize(tokens)
    except Exception:
        # Best-effort: never fail generation because the sanitizer failed.
        return code

