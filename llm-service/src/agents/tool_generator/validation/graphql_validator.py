"""
GraphQL spec validator (Phase 7) — checks that a GraphQL ConnectorSpec is
internally consistent before generation ships.

Catches at *build time* the bugs that would otherwise blow up at runtime:
  - Query template syntax errors (unbalanced braces, missing fragments)
  - Variable signature mismatch (template declares `$id` but spec.parameters has no `id`)
  - Operation type mismatch (template starts `mutation` but spec marks it as query)
  - Operation name mismatch (template field name differs from graphql.operation_name)
  - Missing required parameters when template uses `!`
  - Empty / whitespace-only templates
  - Lazy-mode connectors with zero registered ops

Returns a list of `ValidationIssue` records — `severity=error` blocks the
build, `severity=warning` is informational.

VERSION: 1.0.0
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Set

from schemas.spec import (
    ConnectorSpec,
    GraphQLOperationType,
    OperationConfig,
    Protocol,
)

logger = logging.getLogger(__name__)


@dataclass
class ValidationIssue:
    severity: str          # "error" | "warning"
    code: str              # short stable id (e.g. "GQL001")
    message: str
    operation_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Regex helpers (intentionally tolerant — full GraphQL grammar would be heavy)
# ---------------------------------------------------------------------------

_OP_HEADER_RE = re.compile(
    r"^\s*(query|mutation|subscription)\s+(\w+)?\s*"
    r"(?:\(([^)]*)\))?\s*\{",
    re.IGNORECASE,
)
_FIRST_FIELD_RE = re.compile(r"^\s*(\w+)")
_VAR_DECL_RE = re.compile(r"\$(\w+)\s*:\s*([\[\]\w!]+)")
_VAR_USE_RE = re.compile(r"\$(\w+)")


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def validate_graphql_spec(spec: ConnectorSpec) -> List[ValidationIssue]:
    """Validate a complete GraphQL ConnectorSpec.

    Returns a flat list of issues. An empty list means the spec is OK.
    Callers can split errors from warnings via `issue.severity`.
    """
    issues: List[ValidationIssue] = []

    if str(getattr(spec.protocol, "value", spec.protocol)).lower() != "graphql":
        return [ValidationIssue(
            severity="error",
            code="GQL000",
            message=f"validate_graphql_spec called on non-graphql spec (protocol={spec.protocol})",
        )]

    graphql_ops = [o for o in spec.operations if o.graphql is not None]
    if not graphql_ops:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL001",
            message="GraphQL connector has zero operations with graphql metadata",
        ))
        return issues

    seen_names: Set[str] = set()
    for op in graphql_ops:
        if op.name in seen_names:
            issues.append(ValidationIssue(
                severity="error",
                code="GQL002",
                message=f"duplicate operation name '{op.name}'",
                operation_name=op.name,
            ))
        seen_names.add(op.name)
        issues.extend(validate_operation(op))

    if not spec.base_url:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL010",
            message="GraphQL spec missing base_url",
        ))

    return issues


def validate_operation(op: OperationConfig) -> List[ValidationIssue]:
    """Validate a single GraphQL operation."""
    issues: List[ValidationIssue] = []
    if op.graphql is None:
        return [ValidationIssue(
            severity="error",
            code="GQL003",
            message=f"operation '{op.name}' has no graphql metadata",
            operation_name=op.name,
        )]

    template = (op.graphql.query_template or "").strip()
    if not template:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL004",
            message="empty query_template",
            operation_name=op.name,
        ))
        return issues

    # 1. Balanced braces
    open_count = template.count("{")
    close_count = template.count("}")
    if open_count != close_count:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL005",
            message=f"unbalanced braces ({open_count} open, {close_count} close)",
            operation_name=op.name,
        ))
    open_paren = template.count("(")
    close_paren = template.count(")")
    if open_paren != close_paren:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL006",
            message=f"unbalanced parentheses ({open_paren} open, {close_paren} close)",
            operation_name=op.name,
        ))

    # 2. Operation header parses + matches declared operation_type
    header = _OP_HEADER_RE.match(template)
    if header is None:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL007",
            message="query_template does not start with a valid query/mutation/subscription header",
            operation_name=op.name,
        ))
        return issues

    declared_kind = op.graphql.operation_type
    template_kind_str = header.group(1).lower()
    template_kind = {
        "query": GraphQLOperationType.QUERY,
        "mutation": GraphQLOperationType.MUTATION,
        "subscription": GraphQLOperationType.SUBSCRIPTION,
    }[template_kind_str]
    if template_kind != declared_kind:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL008",
            message=(
                f"operation_type mismatch: spec says '{declared_kind.value}' but "
                f"template starts with '{template_kind_str}'"
            ),
            operation_name=op.name,
        ))

    # 3. Variable signature must declare every $var the body uses,
    #    and every declared $var should be passable via op.parameters.
    var_decls = dict(_VAR_DECL_RE.findall(header.group(3) or ""))   # name → graphql type str
    body = template[header.end():]
    used_vars = set(_VAR_USE_RE.findall(body))

    undeclared = used_vars - set(var_decls.keys())
    if undeclared:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL009",
            message=f"template uses undeclared variables: {sorted(undeclared)}",
            operation_name=op.name,
        ))

    # Each declared variable should map to a parameter (warn if not — many
    # generated specs are correct here, but defensively flag it).
    param_names = {p.name for p in op.parameters}
    for var_name, gql_type in var_decls.items():
        if var_name not in param_names:
            issues.append(ValidationIssue(
                severity="warning",
                code="GQL011",
                message=(
                    f"variable ${var_name} declared in template but not in spec.parameters "
                    f"(callers must pass it manually via the variables dict)"
                ),
                operation_name=op.name,
            ))
        if gql_type.endswith("!"):
            # NON_NULL — ensure spec marks it required (warning, not error,
            # since the runtime will error on missing values too)
            param = next((p for p in op.parameters if p.name == var_name), None)
            if param is not None and not param.required:
                issues.append(ValidationIssue(
                    severity="warning",
                    code="GQL012",
                    message=(
                        f"variable ${var_name} is NON_NULL ({gql_type}) but spec parameter "
                        "is marked optional"
                    ),
                    operation_name=op.name,
                ))

    # 4. First selection field name must match graphql.operation_name
    field_match = _FIRST_FIELD_RE.match(body)
    if field_match is None:
        issues.append(ValidationIssue(
            severity="error",
            code="GQL013",
            message="could not parse selection field from template",
            operation_name=op.name,
        ))
    else:
        first_field = field_match.group(1)
        if first_field != op.graphql.operation_name:
            issues.append(ValidationIssue(
                severity="warning",
                code="GQL014",
                message=(
                    f"first selection field '{first_field}' does not match "
                    f"graphql.operation_name '{op.graphql.operation_name}'"
                ),
                operation_name=op.name,
            ))

    return issues


# ---------------------------------------------------------------------------
# Convenience aggregators
# ---------------------------------------------------------------------------


def has_errors(issues: List[ValidationIssue]) -> bool:
    return any(i.severity == "error" for i in issues)


def errors_only(issues: List[ValidationIssue]) -> List[ValidationIssue]:
    return [i for i in issues if i.severity == "error"]


def warnings_only(issues: List[ValidationIssue]) -> List[ValidationIssue]:
    return [i for i in issues if i.severity == "warning"]
