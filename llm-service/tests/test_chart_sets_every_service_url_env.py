"""The Helm chart must set every service-to-service URL its Go code reads.

THE DEFECT CLASS
----------------
Go code across the three Go services reads a URL from the environment and, when
it is empty, falls back to a bare docker-compose service name:

    base := os.Getenv("KAFKA_CONNECT_URL")
    if base == "" {
        base = "http://kafka-connect:8083"     // <-- compose-shaped
    }

Under docker-compose that fallback is correct: the compose network resolves
`kafka-connect`. Under Helm every Service carries the release prefix, so the
same string resolves to nothing and the call fails DNS. The manifest is valid
YAML, `helm lint` and `helm template` both pass, and the pod stays Ready --
these call paths degrade quietly rather than crashing, so nothing turns red.
It is decidable only against the set of Services the chart actually declares.

This has now happened four times (KAFKA_CONNECT_URL, TOOL_GENERATOR_URL, then
LLM_SERVICE_URL/PLANNER_URL across three deployments, then the api-gateway and
orchestrator internal-URL pairs). Hence a guard rather than a fourth patch.

WHY test_chart_service_hostnames_resolve.py DOES NOT COVER THIS
---------------------------------------------------------------
That guard checks the inverse direction and is structurally blind here twice
over. It iterates hostnames the chart BUILDS and asserts each names a declared
Service -- but this defect is an ABSENCE: the chart builds no hostname at all,
and nothing that iterates over what is present can see what is missing. It also
reads only _helpers.tpl, so hostnames written inline in an app template are
invisible to it regardless. Both guards are needed; neither implies the other.

WHY THE PREDICATE IS A CHAIN AND NOT A SINGLE NAME
--------------------------------------------------
Consumers commonly read several names in order before giving up:

    if u == "" { u = os.Getenv("API_GATEWAY_INTERNAL_URL") }
    if u == "" { u = os.Getenv("API_GATEWAY_URL") }
    if u == "" { u = "http://api-gateway:8080" }

The chart only has to set ONE name in such a chain for the bare fallback to be
unreachable. Asserting on individual names instead of chains produces false
positives: api-gateway's BACKEND_ORCHESTRATOR_URL (cmd/server/main.go:560) is
genuinely unset, yet harmless, because ORCHESTRATOR_URL earlier in the same
chain IS set. That case is why this guard groups by assignment target.
"""

from __future__ import annotations

import os
import re

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "templates")
APPS = os.path.join(TEMPLATES, "apps")
HELPERS = os.path.join(TEMPLATES, "_helpers.tpl")

# Go module root -> the chart template that deploys it.
SERVICES = {
    "api-gateway": "api-gateway.yaml",
    "backend-orchestrator": "orchestrator.yaml",
    "backend-temporal-adapter": "temporal-adapter.yaml",
}

# Chains we deliberately do not require the chart to set, keyed by the chain's
# env names. Every entry needs a reason, and test_every_exemption_is_still_needed
# forces an entry out the moment it stops applying.
EXEMPT = {
    ("SCHEMA_REGISTRY_URL",): (
        "The chart declares no schema-registry Service at all, so there is no "
        "address to point this at -- setting it is not the fix. This is a "
        "missing-component gap, not a missing-wiring one: the Avro/schema "
        "paths in api-gateway (avro_producer.go, unified_producer.go, "
        "schema_registry.go) and orchestrator (kafka/manager.go) cannot work "
        "on the chart until it ships one. Remove this entry when it does."
    ),
}

_OPENERS = ("if", "range", "with", "block", "define")
_ACTION = re.compile(r"\{\{-?\s*(.*?)\s*-?\}\}", re.S)
_ENV_NAME = re.compile(r"^\s*-\s*name:\s*([A-Z0-9_]+)\s*$", re.M)
_INCLUDE = re.compile(r'include\s+"([^"]+)"')

# var = <anything> os.Getenv("NAME")     -- one link in a fallback chain
_GETENV = re.compile(r'(\w+)\s*(?::=|=)\s*.*os\.Getenv\("([A-Z0-9_]+)"\)')
# var = "http://host:port"               -- the terminal literal
_BARE = re.compile(r'(\w+)\s*(?::=|=)\s*"(https?)://([A-Za-z0-9_.-]+):(\d+)')
# How far the literal may sit from the last Getenv and still be the same chain.
_CHAIN_GAP = 6


def _define_bodies(text: str) -> dict:
    """Extract every {{ define }} body, tracking nesting.

    A non-greedy match to the first {{ end }} is wrong: these bodies contain
    if/range blocks with their own {{ end }}. That mistake silently yields
    empty bodies, which would make this whole guard vacuous -- every helper
    would appear to contribute zero env names and every chain would look
    unset. test_the_template_parser_resolves_a_known_helper pins it.
    """
    out = {}
    for m in re.finditer(r'\{\{-?\s*define\s+"([^"]+)"\s*-?\}\}', text):
        name, start, depth = m.group(1), m.end(), 1
        for a in _ACTION.finditer(text, start):
            parts = a.group(1).split()
            kw = parts[0] if parts else ""
            if kw in _OPENERS:
                depth += 1
            elif kw == "end":
                depth -= 1
                if depth == 0:
                    out[name] = text[start:a.start()]
                    break
    return out


def _env_names_for(template_path: str) -> set:
    """Every env var name a template sets, following includes recursively."""
    bodies = _define_bodies(open(HELPERS).read())
    seen_helpers = set()

    def walk(text: str) -> set:
        names = set(_ENV_NAME.findall(text))
        for helper in _INCLUDE.findall(text):
            if helper in bodies and helper not in seen_helpers:
                seen_helpers.add(helper)
                names |= walk(bodies[helper])
        return names

    return walk(open(template_path).read())


def _chains() -> list:
    """Every (service, chain-of-env-names, bare-host) in the Go sources."""
    found = []
    for root, template in sorted(SERVICES.items()):
        for dirpath, _dirs, files in os.walk(os.path.join(REPO_ROOT, root)):
            for fn in sorted(files):
                if not fn.endswith(".go") or fn.endswith("_test.go"):
                    continue
                path = os.path.join(dirpath, fn)
                with open(path, encoding="utf-8", errors="replace") as fh:
                    lines = fh.read().splitlines()
                acc = {}
                for i, ln in enumerate(lines):
                    g = _GETENV.search(ln)
                    if g:
                        var, name = g.group(1), g.group(2)
                        names, first, _last = acc.get(var, ([], i + 1, i + 1))
                        acc[var] = (names + [name], first, i + 1)
                        continue
                    b = _BARE.search(ln)
                    if not b:
                        continue
                    var, host, port = b.group(1), b.group(3), b.group(4)
                    if var not in acc:
                        continue
                    names, first, last = acc.pop(var)
                    # A host with a dot is a real DNS name or an IP; localhost
                    # is in-pod. Only a bare single label is compose-shaped.
                    if "." in host or host == "localhost":
                        continue
                    if i + 1 - last <= _CHAIN_GAP:
                        found.append({
                            "template": template,
                            "chain": tuple(names),
                            "host": f"{host}:{port}",
                            "where": f"{os.path.relpath(path, REPO_ROOT)}:{first}",
                        })
    return found


CHAINS = _chains()
UNEXEMPT = [c for c in CHAINS if c["chain"] not in EXEMPT]


def test_the_template_parser_resolves_a_known_helper():
    """Pin the nesting-aware parser against a helper with a known payload.

    rsync-ai.cryptoEnv sets exactly these three. If _define_bodies regresses to
    stopping at the first nested {{ end }}, this fails loudly instead of
    letting the main assertions pass vacuously on empty helper bodies.
    """
    bodies = _define_bodies(open(HELPERS).read())
    assert "rsync-ai.cryptoEnv" in bodies
    got = set(_ENV_NAME.findall(bodies["rsync-ai.cryptoEnv"]))
    assert got == {"JWT_SECRET", "ENCRYPTION_KEY", "INTERNAL_SERVICE_SECRET"}, got


def test_the_scan_is_not_vacuous():
    """Floors, so a broken scanner cannot pass by finding nothing."""
    assert len(CHAINS) >= 30, f"only {len(CHAINS)} chains found -- scanner broken?"
    by_service = {}
    for c in CHAINS:
        by_service.setdefault(c["template"], []).append(c)
    for template in SERVICES.values():
        assert by_service.get(template), f"no chains found for {template}"
    multi = [c for c in CHAINS if len(c["chain"]) > 1]
    assert multi, "no multi-name chains found -- chain grouping is not working"


@pytest.mark.parametrize(
    "chain",
    sorted({c["chain"] for c in UNEXEMPT}),
    ids=lambda ch: "+".join(ch),
)
def test_the_chart_sets_every_url_env_its_code_falls_back_from(chain):
    for c in [x for x in UNEXEMPT if x["chain"] == chain]:
        declared = _env_names_for(os.path.join(APPS, c["template"]))
        if declared & set(chain):
            continue
        pytest.fail(
            f"{c['where']} reads {' -> '.join(chain)} and falls back to "
            f"\"http://{c['host']}\", a bare docker-compose service name. "
            f"{c['template']} sets none of those names, so on Kubernetes this "
            f"call dials a host that does not resolve (Services carry the "
            f"release prefix). Set one of {list(chain)} in {c['template']} to "
            f"the prefixed Service address."
        )


@pytest.mark.parametrize("chain", sorted(EXEMPT), ids=lambda ch: "+".join(ch))
def test_every_exemption_is_still_needed(chain):
    """An exemption that no longer corresponds to a real chain is a hole."""
    assert any(c["chain"] == chain for c in CHAINS), (
        f"EXEMPT lists {chain} but no Go code falls back from it any more. "
        f"Drop the entry -- a stale exemption silently widens the next time "
        f"that name comes back."
    )
