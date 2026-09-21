"""
Security lint for generated connector artifacts.

Two layers, both invoked from `generator/builder.py` before any LLM-produced
content is written to disk or fed to ``docker build``:

1. ``lint_connector_code`` walks the AST of a generated ``connector.py`` and
   rejects patterns that would let a compromised model exfiltrate
   credentials, escape the connector sandbox, or grant runtime arbitrary
   code execution. The connector container runs with the user's credentials
   for the configured data source, so a malicious ``os.system``,
   ``subprocess(shell=True)``, dynamic ``eval``/``exec``/``compile``, or
   ``__import__`` of a non-literal name is a real exfiltration path.

2. ``sanitize_dockerfile_field`` validates strings that the Dockerfile
   template embeds into ``LABEL`` / ``ENV`` directives. Jinja2's
   ``select_autoescape`` only escapes HTML and XML — it does NOT escape
   Dockerfile syntax. A ``display_name`` like ``"foo\nRUN curl …"`` would
   inject a new build instruction. We reject control characters and shell
   metacharacters outright and length-cap the value so a single field can't
   take over the Dockerfile.

The audit that prompted this module flagged these as the highest-impact
attack paths in the tool-generator. They are intentionally narrow guards
that fail closed; broader hardening (build-time network isolation, an
out-of-process build worker that owns docker.sock, mcp.auto.generated as
an orchestrator pull-time policy) is tracked as follow-up PRs.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional


# Modules whose top-level functions are dangerous regardless of arguments.
# Importing them is OK (legitimate connectors may need pathlib, json, etc.);
# CALLING `os.system`, `subprocess.call/run/check_*` with shell=True, etc.
# is what we reject.
_DENY_CALLS: dict[str, set[str]] = {
    "os": {"system", "popen", "execv", "execve", "execvp", "execvpe", "spawnv", "spawnve", "spawnvp", "spawnvpe", "spawnl", "spawnle", "spawnlp", "spawnlpe"},
    "subprocess": {"getoutput", "getstatusoutput"},
    "ctypes": {"CDLL", "WinDLL", "PyDLL", "OleDLL"},
    "pickle": {"loads", "load"},
    "marshal": {"loads", "load"},
    "shelve": {"open"},
}

# Builtins that take arbitrary code/data: reject when called with anything
# other than a string literal (and even then, prefer reject — we have no
# legitimate use case for these in a generated connector).
_DENY_BUILTIN_CALLS: set[str] = {"eval", "exec", "compile"}

# Reflection bypasses for the eval/exec ban look like
# ``getattr(__builtins__, "eval")(...)`` or
# ``globals()["__builtins__"]["eval"](...)``. The common ingredient is a
# direct textual reference to ``__builtins__`` — no legitimate connector
# code path needs to name it. Banning the name is far more precise than
# banning ``getattr``/``globals`` (which connectors legitimately use for
# safe attribute access and config introspection).
_DENY_DUNDER_NAMES: frozenset[str] = frozenset({"__builtins__"})

# Dynamic import attack: __import__("os") with a non-literal name lets an
# attacker route around static analysis.
_DYNAMIC_IMPORT: str = "__import__"

# Deny-list of dotted names that are dangerous regardless of how they are
# referenced (called directly, used as a decorator, assigned to a variable
# and later invoked, etc.). Built from _DENY_CALLS for free; flagging on
# *any* AST occurrence catches decorator-form bypasses such as
# ``@os.system\ndef foo(): ...`` where the AST sees an Attribute node, not
# a Call. Limited to functions whose execution is unambiguously dangerous.
_DENY_BARE_REFERENCES: frozenset[str] = frozenset(
    f"{mod}.{fn}" for mod, fns in _DENY_CALLS.items() for fn in fns
)


@dataclass(frozen=True)
class SecurityFinding:
    """A single security violation flagged by lint_connector_code."""

    rule: str
    message: str
    line: int

    def __str__(self) -> str:  # pragma: no cover - trivial formatting
        return f"[{self.rule}] line {self.line}: {self.message}"


class _SecurityVisitor(ast.NodeVisitor):
    """AST visitor that records SecurityFinding instances."""

    def __init__(self) -> None:
        self.findings: List[SecurityFinding] = []
        # Track import aliases so we catch ``import subprocess as sp`` then
        # ``sp.run(..., shell=True)``. Maps alias-name -> real-module-name.
        self._aliases: dict[str, str] = {}

    # -- import tracking ----------------------------------------------------

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._aliases[alias.asname or alias.name] = alias.name
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        # `from subprocess import run` makes "run" resolve to subprocess.run.
        if node.module:
            for alias in node.names:
                self._aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        self.generic_visit(node)

    # -- the core call check -----------------------------------------------

    def visit_Call(self, node: ast.Call) -> None:
        func_name = self._resolve_call_target(node.func)

        if func_name in _DENY_BUILTIN_CALLS:
            self.findings.append(SecurityFinding(
                rule="dynamic_code_exec",
                message=f"{func_name}() is forbidden in generated connectors",
                line=getattr(node, "lineno", 0),
            ))

        if func_name == _DYNAMIC_IMPORT:
            self.findings.append(SecurityFinding(
                rule="dynamic_import",
                message="__import__() is forbidden; use a top-level `import` statement",
                line=getattr(node, "lineno", 0),
            ))

        # Resolved attribute-call ("os.system", "subprocess.run", ...).
        for mod, denied in _DENY_CALLS.items():
            for fn in denied:
                if func_name == f"{mod}.{fn}":
                    self.findings.append(SecurityFinding(
                        rule=f"{mod}_call",
                        message=f"{mod}.{fn}() is forbidden in generated connectors",
                        line=getattr(node, "lineno", 0),
                    ))

        # subprocess.run/call/check_*: reject if shell=True regardless of
        # argv shape — connectors have no legitimate need to spawn a shell.
        if func_name.startswith("subprocess.") or func_name in {"run", "call", "check_call", "check_output", "Popen"}:
            for kw in node.keywords:
                if kw.arg == "shell" and self._truthy_literal(kw.value):
                    self.findings.append(SecurityFinding(
                        rule="subprocess_shell",
                        message="subprocess.*(shell=True) is forbidden in generated connectors",
                        line=getattr(node, "lineno", 0),
                    ))

        self.generic_visit(node)

    # -- decorators ---------------------------------------------------------
    #
    # `@os.system\ndef foo(): pass` is desugared to `foo = os.system(foo)` at
    # runtime, but the AST stores the decorator as a bare Attribute node
    # (not a Call), so visit_Call alone misses it. We explicitly walk the
    # decorator_list for every def/async-def/class and flag any reference
    # to a dotted name on our deny list — whether the decorator is in
    # `@os.system` form (Attribute) or `@os.system()` form (Call).

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_decorators(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_decorators(node)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._check_decorators(node)
        self.generic_visit(node)

    def _check_decorators(self, node: ast.AST) -> None:
        for dec in getattr(node, "decorator_list", []) or []:
            target = self._resolve_call_target(dec.func if isinstance(dec, ast.Call) else dec)
            if target in _DENY_BARE_REFERENCES or target in _DENY_BUILTIN_CALLS or target == _DYNAMIC_IMPORT:
                self.findings.append(SecurityFinding(
                    rule="dangerous_decorator",
                    message=f"@{target} is forbidden as a decorator in generated connectors",
                    line=getattr(dec, "lineno", 0),
                ))

    # -- forbidden dunder names --------------------------------------------
    #
    # Any textual reference to ``__builtins__`` is suspicious. The eval/exec
    # ban is routed around primarily by reaching into __builtins__ via
    # getattr / globals subscript / dictionary access — every such bypass
    # has to name __builtins__ somewhere in the AST. Flag it as a Name and
    # as an Attribute so both spellings (``__builtins__["eval"]`` and
    # ``module.__builtins__``) are caught.

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in _DENY_DUNDER_NAMES:
            self.findings.append(SecurityFinding(
                rule="builtins_reference",
                message=f"reference to {node.id} is forbidden in generated connectors",
                line=getattr(node, "lineno", 0),
            ))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in _DENY_DUNDER_NAMES:
            self.findings.append(SecurityFinding(
                rule="builtins_reference",
                message=f"reference to {node.attr} is forbidden in generated connectors",
                line=getattr(node, "lineno", 0),
            ))
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        # Catch the subscript-based bypass: ``globals()["__builtins__"]``
        # stores ``__builtins__`` as a string Constant inside a Subscript,
        # not as a Name. No legitimate connector should name this string.
        if isinstance(node.value, str) and node.value in _DENY_DUNDER_NAMES:
            self.findings.append(SecurityFinding(
                rule="builtins_reference",
                message=f"string literal {node.value!r} is forbidden in generated connectors",
                line=getattr(node, "lineno", 0),
            ))
        self.generic_visit(node)

    # -- helpers ------------------------------------------------------------

    def _resolve_call_target(self, node: ast.AST) -> str:
        """Return a dotted name for the call target, or "" if not resolvable."""
        if isinstance(node, ast.Name):
            # `run(...)` after `from subprocess import run` -> "subprocess.run".
            return self._aliases.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            base = self._resolve_call_target(node.value)
            if base:
                return f"{base}.{node.attr}"
            return node.attr
        return ""

    @staticmethod
    def _truthy_literal(node: ast.AST) -> bool:
        # `shell=True` is the only thing we treat as definitely shell-mode.
        # Anything else (variables, function calls) we leave alone — those
        # cases are vanishingly rare in connector code and false-positives
        # would block the legitimate generation pipeline.
        if isinstance(node, ast.Constant):
            return node.value is True
        return False


def lint_connector_code(code: str) -> List[SecurityFinding]:
    """Return security findings for a generated connector.py.

    Empty list means the code is acceptable. Callers should treat any
    non-empty list as a hard failure and refuse to persist the code.
    """
    if not code or not code.strip():
        return []
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        # Syntax errors are caught elsewhere (post_generation validator).
        # If the code doesn't parse, AST-based denial would yield false
        # negatives — flag explicitly so callers can choose.
        return [SecurityFinding(
            rule="parse_error",
            message=f"connector.py does not parse: {e.msg}",
            line=e.lineno or 0,
        )]
    visitor = _SecurityVisitor()
    visitor.visit(tree)
    return visitor.findings


# ---------------------------------------------------------------------------
# Dockerfile field sanitization
# ---------------------------------------------------------------------------

# Reject any character that can break out of a LABEL value or ENV value into
# a new Dockerfile instruction.
#
# - C0 / C1 control characters (\x00-\x1f, \x7f-\x9f) cover newline, carriage
#   return, tab, NEL (U+0085), form feed, file/group/record/unit separators,
#   and anything else that line-oriented Dockerfile parsing might split on.
# - `"` closes a LABEL value; `\` is the line-continuation char.
# - `` ` `` and `$` are shell substitution surprises in derived images that
#   re-quote these labels.
# - `;` is a shell statement separator (defensive — Dockerfile LABEL values
#   are not shell, but downstream consumers often treat them as such).
# - `#` is a Dockerfile comment introducer. Mid-line `#` is literal today,
#   but defense-in-depth keeps it banned in case a template change pulls
#   the field onto its own line.
_DOCKERFILE_FIELD_BAD_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f\"\\`$;#]")

# Cap to a reasonable display length. Anything over this in a LABEL is
# either an LLM hallucination or a deliberate attempt to bury a payload
# past a casual reviewer; clamp before it reaches the build context.
_DOCKERFILE_FIELD_MAX_LEN = 200


class DockerfileFieldRejected(ValueError):
    """Raised when a spec field cannot be safely embedded in the Dockerfile."""


def sanitize_dockerfile_field(value: Optional[str], field_name: str) -> str:
    """Return ``value`` if it is safe to render verbatim into a Dockerfile,
    otherwise raise :class:`DockerfileFieldRejected`.

    The validator is intentionally strict. Generated connectors should not
    need control characters or shell metacharacters in their display name,
    description, connector type, category, or env-var names.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        raise DockerfileFieldRejected(
            f"{field_name}: expected str, got {type(value).__name__}"
        )
    if len(value) > _DOCKERFILE_FIELD_MAX_LEN:
        raise DockerfileFieldRejected(
            f"{field_name}: exceeds {_DOCKERFILE_FIELD_MAX_LEN} chars (got {len(value)})"
        )
    bad = _DOCKERFILE_FIELD_BAD_CHARS.search(value)
    if bad is not None:
        raise DockerfileFieldRejected(
            f"{field_name}: contains forbidden character {bad.group(0)!r} "
            f"(newlines, quotes, backslash, backtick, $, and ; are not allowed)"
        )
    return value


# Field names whose values we always sanitize before Jinja2 embeds them.
# Keep this list in sync with the substitutions in templates/Dockerfile.j2.
_DOCKERFILE_SPEC_FIELDS: tuple[str, ...] = (
    "display_name",
    "description",
    "version",
    "connector_type",
)


def sanitize_spec_for_dockerfile(spec: object) -> List[SecurityFinding]:
    """Validate every spec field that flows into the Dockerfile.

    Returns a list of findings (empty on success). Mutates nothing — the
    Dockerfile template still reads the original attributes; we just refuse
    to build if any field is unsafe.
    """
    findings: List[SecurityFinding] = []
    for field in _DOCKERFILE_SPEC_FIELDS:
        value = getattr(spec, field, None)
        try:
            sanitize_dockerfile_field(value, f"spec.{field}")
        except DockerfileFieldRejected as exc:
            findings.append(SecurityFinding(
                rule="dockerfile_field",
                message=str(exc),
                line=0,
            ))

    # spec.category may be an Enum — validate its `.value` attribute.
    category = getattr(spec, "category", None)
    category_value = getattr(category, "value", category)
    try:
        sanitize_dockerfile_field(category_value if isinstance(category_value, str) else None,
                                  "spec.category.value")
    except DockerfileFieldRejected as exc:
        findings.append(SecurityFinding(
            rule="dockerfile_field",
            message=str(exc),
            line=0,
        ))

    # spec.config_fields[*].env_var is iterated by the template and emitted
    # as `ENV {{ field.env_var }}=""`. A field with a newline in its
    # env_var would break out of the ENV directive into a new RUN line.
    config_fields: Iterable[object] = getattr(spec, "config_fields", []) or []
    for idx, cfg in enumerate(config_fields):
        env_var = getattr(cfg, "env_var", None)
        if env_var is None:
            continue
        try:
            sanitize_dockerfile_field(env_var, f"spec.config_fields[{idx}].env_var")
        except DockerfileFieldRejected as exc:
            findings.append(SecurityFinding(
                rule="dockerfile_field",
                message=str(exc),
                line=0,
            ))
    return findings
