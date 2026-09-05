"""RED regression for defect d11 (cursor) — generation-time inference must
recognize URL-style next-page fields (``next_page_uri`` / ``next_page_url``, e.g.
Twilio) so ``cursor_path`` is populated deterministically instead of relying only
on runtime autodiscovery.

``_find_cursor_path`` matches top-level fields in ``_CURSOR_FIELD_NAMES`` and
container fields (inside meta/paging/…) in ``_CONTAINER_CURSOR_FIELDS``
(= ``_CURSOR_FIELD_NAMES`` + extras). Twilio's cursor lives at
``meta.next_page_uri`` — a full URL. Today ``next_page_uri`` / ``next_page_url``
are in neither set, so ``_find_cursor_path`` returns "" and the generated
connector has an empty cursor_path.

RED today: next_page_uri / next_page_url are not in the cursor-field vocabulary.
GREEN after fix: both are added to ``_CURSOR_FIELD_NAMES`` and ``_find_cursor_path``
returns ``meta.next_page_uri`` / ``next_page_url``.

Runs fully in-process (no network, no docker).
"""

from __future__ import annotations

import os
import sys

# tool_generator package root: .../llm-service/src/agents/tool_generator
_TOOLGEN = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),  # .../llm-service/tests
        "..",                       # .../llm-service
        "src",
        "agents",
        "tool_generator",
    )
)
if _TOOLGEN not in sys.path:
    sys.path.insert(0, _TOOLGEN)

from utils.openapi_discovery import (  # noqa: E402
    _CURSOR_FIELD_NAMES,
    _find_cursor_path,
)


def test_cursor_field_vocab_includes_url_next_page_fields():
    assert "next_page_uri" in _CURSOR_FIELD_NAMES
    assert "next_page_url" in _CURSOR_FIELD_NAMES


def test_find_cursor_path_recognizes_meta_next_page_uri():
    # Twilio-style response: records under "data", cursor URL under meta.next_page_uri.
    props = {
        "data": {"type": "array", "items": {"type": "object"}},
        "meta": {
            "type": "object",
            "properties": {
                "next_page_uri": {"type": "string"},
                "page": {"type": "integer"},
                "page_size": {"type": "integer"},
            },
        },
    }
    assert _find_cursor_path({}, props) == "meta.next_page_uri"


def test_find_cursor_path_recognizes_top_level_next_page_url():
    props = {
        "next_page_url": {"type": "string"},
        "data": {"type": "array", "items": {"type": "object"}},
    }
    assert _find_cursor_path({}, props) == "next_page_url"
