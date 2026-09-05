"""
Test runner stability shim.

Why this exists:
- Some environments auto-load third-party pytest plugins via entrypoints.
- In this repo, the `langsmith` pytest plugin can crash under Python 3.12 due to
  a pydantic v1 forward-ref incompatibility, preventing *any* tests from running.

Goal:
- Keep `pytest` runnable out-of-the-box by disabling plugin autoload *only* when
  pytest is being invoked.
- Developers can still explicitly enable plugins by setting
  `PYTEST_DISABLE_PLUGIN_AUTOLOAD=0` (or unsetting it) in their environment.
"""

from __future__ import annotations

import os
import sys


def _looks_like_pytest_invocation(argv: list[str]) -> bool:
    # Common invocations:
    # - `pytest ...`
    # - `python -m pytest ...`
    # - `py.test ...`
    joined = " ".join(argv or [])
    return ("pytest" in joined) or ("py.test" in joined) or ("PYTEST_CURRENT_TEST" in os.environ)


if _looks_like_pytest_invocation(sys.argv):
    # Prevent auto-loading setuptools-entrypoint plugins (e.g., langsmith) that can
    # crash before pytest processes our repo config.
    os.environ.setdefault("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")

