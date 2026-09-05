"""One SASL mechanism -> JAAS login module mapping, written out SEVEN times.

docker-compose.quickstart.yml:309 says the compose kafka-init builder and the
Helm one are "hand-kept in lockstep". Nothing checked that, and the comment
undercounts: seven tracked files encode the same mapping, in four different
syntaxes.

  docker-compose.quickstart.yml                      Docker install path
  deploy/helm/rsync-ai/templates/jobs/kafka-init.yaml Kubernetes install path
  deploy/helm/rsync-ai/templates/_helpers.tpl         chart app-side sasl.jaas.config
  llm-service/src/utils/kafka_security.py             Python tier / Kafka Connect
  shared/mcp-connectors/.../debezium/connector.py     Debezium schema history client
  scripts/kafka-init-new-topics.sh                    add-a-topic operational script
  deploy/helm/rsync-ai/test/kind/broker-up.sh         kind probe (the drift DETECTOR)

They cannot be factored into one file. install.sh downloads exactly ONE file
(the quickstart compose), so the compose side can ship no sidecar to source;
the chart renders rather than executes; the Python tiers ship in images that do
not contain the shell scripts. Duplication is the design. What was missing is
the check that the duplicates agree.

Why drift here is expensive rather than annoying: a Kafka client whose security
config does not match the listener does not error, it BLOCKS. A mechanism or a
module that drifts on one site produces an install that hangs on one platform
and works on the other, with no message naming the difference -- which is the
failure both kafka-init builders exist to convert into a message, and the reason
they write AdminClient timeouts even for PLAINTEXT.

The mapping is pinned as a CANONICAL constant rather than compared pairwise, so
the test states the truth instead of only detecting disagreement: seven sites
that had all drifted the same way would pass a pairwise check. Adding a fifth
mechanism is meant to require editing CANONICAL here -- that is the point, not
friction.

What this file deliberately does NOT assert: that the seven reject AWS_MSK_IAM
in the same PLACE. The chart rejects it at RENDER time (_helpers.tpl's `fail`),
so it can never reach the job; everything else has no render step and must fail
at runtime. Same outcome, necessarily different mechanism.
"""

from __future__ import annotations

import fnmatch
import pathlib
import re
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]

COMPOSE = REPO / "docker-compose.quickstart.yml"
CHART_JOB = REPO / "deploy" / "helm" / "rsync-ai" / "templates" / "jobs" / "kafka-init.yaml"
HELPERS = REPO / "deploy" / "helm" / "rsync-ai" / "templates" / "_helpers.tpl"
KAFKA_SECURITY = REPO / "llm-service" / "src" / "utils" / "kafka_security.py"
DEBEZIUM = (
    REPO / "shared" / "mcp-connectors" / "internal" / "debezium"
    / "versions" / "v1.0.0" / "connector.py"
)
OPS_SCRIPT = REPO / "scripts" / "kafka-init-new-topics.sh"
KIND_PROBE = REPO / "deploy" / "helm" / "rsync-ai" / "test" / "kind" / "broker-up.sh"

PLAIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
SCRAM_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule"
OAUTH_MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"

#: The mapping every site must encode. Kafka's own contract, not a rsync choice.
CANONICAL = {
    "PLAIN": PLAIN_MODULE,
    "SCRAM-SHA-256": SCRAM_MODULE,
    "SCRAM-SHA-512": SCRAM_MODULE,
    "OAUTHBEARER": OAUTH_MODULE,
}

MODULE_RE = r"org\.apache\.kafka\.common\.security\.(?:plain\.Plain|scram\.Scram|oauthbearer\.OAuthBearer)LoginModule"


def _text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def _token(word: str, text: str) -> bool:
    """Whole-token containment, on the string a reader actually sees.

    A plain `"AWS_MSK_IAM" in text` is satisfied by AWS_MSK_IAM_ANYTHING, and by
    an unrelated env-var comment 270 lines from the rejection it is supposed to
    be pinning. Both of those made an earlier draft of this file pass a mutation
    that deleted the thing under test.

    The escape decoding is not cosmetic: the chart's message is a Go template
    string, so its line breaks are the two characters `\\n`. Without decoding,
    the character before AWS_MSK_IAM is the letter `n` and a word-boundary check
    rejects a token that is plainly there -- the parser reporting a defect the
    file does not have.
    """
    decoded = re.sub(r"\\[ntr]", " ", text)
    return re.search(rf"(?<![A-Za-z0-9_]){re.escape(word)}(?![A-Za-z0-9_])", decoded) is not None


# ── three parsers, one per syntax; no windows, no heuristics ─────────────────

def _case_arm_modules(text: str) -> dict[str, str]:
    """`MECH|MECH) VAR=<module>` shell case arms.

    Covers four sites whose only differences are the variable name (MOD /
    _jaas_module / LOGIN_MODULE), the quoting, and whether the assignment sits
    on the arm's own line. `\\s*` is what makes this safe rather than a guess:
    it crosses whitespace only, so an arm with no module assignment can never
    be paired with a later arm's module.
    """
    out: dict[str, str] = {}
    pattern = rf'([A-Z0-9][A-Z0-9|_\-]*)\)\s*(?:MOD|_jaas_module|LOGIN_MODULE)=["\']?({MODULE_RE})'
    for m in re.finditer(pattern, text):
        for mech in m.group(1).split("|"):
            out[mech] = m.group(2)
    return out


def _helm_if_modules(text: str) -> dict[str, str]:
    """The chart's `{{- if eq $m "MECH" -}}<module>` helper.

    Scoped to the saslLoginModule define block and windowed between consecutive
    module literals, so the `fail` message below it -- which lists every
    mechanism name in prose -- cannot be read as a mapping.
    """
    start = text.find('{{- define "rsync-ai.kafka.saslLoginModule" -}}')
    if start < 0:
        return {}
    nxt = text.find("{{- define ", start + 1)
    block = text[start : nxt if nxt > 0 else len(text)]

    out: dict[str, str] = {}
    prev = 0
    for m in re.finditer(MODULE_RE, block):
        for mech in re.findall(r'eq \$m "([A-Z0-9-]+)"', block[prev : m.start()]):
            out[mech] = m.group(0)
        prev = m.end()
    return out


def _py_dict_modules(text: str) -> dict[str, str]:
    """`_JAAS_MODULES = {...}`, with `MECHANISM_*` constants resolved."""
    consts = dict(re.findall(r'^(MECHANISM_[A-Z0-9_]+)\s*=\s*"([^"]+)"', text, re.M))

    i = text.find("_JAAS_MODULES")
    if i < 0:
        return {}
    i = text.find("{", i)
    depth, j = 0, i
    while j < len(text):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                break
        j += 1
    block = text[i : j + 1]

    out: dict[str, str] = {}
    for m in re.finditer(rf'(?:"([A-Z0-9-]+)"|([A-Za-z_][A-Za-z0-9_]*))\s*:\s*\(?\s*"({MODULE_RE})"', block):
        mech = m.group(1) or consts.get(m.group(2) or "")
        if mech:
            out[mech] = m.group(3)
    return out


SITES = {
    "compose(quickstart kafka-init)": (COMPOSE, _case_arm_modules),
    "helm(kafka-init job)": (CHART_JOB, _case_arm_modules),
    "helm(_helpers.tpl saslLoginModule)": (HELPERS, _helm_if_modules),
    "llm-service(kafka_security._JAAS_MODULES)": (KAFKA_SECURITY, _py_dict_modules),
    "debezium(connector._JAAS_MODULES)": (DEBEZIUM, _py_dict_modules),
    "scripts(kafka-init-new-topics.sh)": (OPS_SCRIPT, _case_arm_modules),
    "kind(broker-up.sh probe)": (KIND_PROBE, _case_arm_modules),
}


# ── the mapping itself ───────────────────────────────────────────────────────

@pytest.mark.parametrize("name", sorted(SITES))
def test_every_site_encodes_the_canonical_mechanism_mapping(name):
    path, parse = SITES[name]
    got = parse(_text(path))
    assert got == CANONICAL, (
        f"{name} ({path.relative_to(REPO)}) does not encode the canonical mapping.\n"
        f"  parsed:   {got}\n  expected: {CANONICAL}\n"
        "The login module is a function of the mechanism. A wrong pairing fails the "
        "handshake with a message naming the MODULE, not the mechanism, so it reads "
        "as a broken broker rather than a config mismatch -- and a MISSING pairing "
        "does not fail at all, it hangs. There are seven copies of this mapping "
        "(see the module docstring); a change has to land on all of them."
    )


def test_no_eighth_copy_of_the_mapping_appeared_unguarded():
    """Discovery, not a hardcoded list -- the copies outnumbered the comment.

    A file that names all three login modules is encoding this mapping. Finding
    them by scanning rather than by memory is what turned "the two builders are
    hand-kept in lockstep" into seven sites. If someone adds an eighth, this
    fails and asks them to register it here or factor it out -- otherwise the
    new copy is unguarded and nothing says so.
    """
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.split()

    found = set()
    for rel in tracked:
        # Prose describing the mapping is a doc-rot problem, not an executable
        # one. Test files are excluded because THIS file names all three.
        if rel.endswith(".md") or "/tests/" in rel or pathlib.Path(rel).name.startswith("test_"):
            continue
        p = REPO / rel
        try:
            body = p.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if len(set(re.findall(MODULE_RE, body))) == 3:
            found.add(rel)

    registered = {str(path.relative_to(REPO)) for path, _ in SITES.values()}
    assert found == registered, (
        "the set of files encoding the mechanism->login-module mapping changed.\n"
        f"  unregistered (parsed by nothing, guarded by nothing): {sorted(found - registered)}\n"
        f"  registered but no longer encoding it:                 {sorted(registered - found)}\n"
        "Add the new site to SITES with a parser, or factor it out."
    )


def test_ci_runs_this_guard_when_any_site_it_watches_changes():
    """The guard must not be gated on a filter that excludes its own subjects.

    ci.yml's `llm` filter gates the job that runs this file, and the filter's
    own comments record two rounds of this bug already: chart guards that were
    dead on chart PRs, and a leak-proof job that would not run when the leak
    proof itself was edited. Five of the seven sites sit under paths the filter
    already lists; two do not, and one of those is the file install.sh
    downloads. Checking the coverage here rather than by eye is the only way it
    stays true.
    """
    import yaml

    ci = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    step = next(
        s for s in ci["jobs"]["changes"]["steps"] if "filters" in (s.get("with") or {})
    )
    globs = yaml.safe_load(step["with"]["filters"])["llm"]
    assert globs, "the llm path filter parsed empty -- this check would pass vacuously"

    def covered(rel: str) -> bool:
        # fnmatch, so a basename glob counts as coverage. The hand-rolled
        # matcher here understood only `dir/**` and exact literals, so when
        # ci.yml collapsed its root compose entries into `docker-compose*.yml`
        # this read as UNCOVERED -- a false negative that would have pushed the
        # filter back toward redundant literals. fnmatch is looser than the
        # picomatch dorny/paths-filter uses (its `*` crosses `/`), so it can
        # only over-report coverage, never fail a pattern CI would honour.
        return any(
            fnmatch.fnmatch(rel, g) or (g.endswith("/**") and rel.startswith(g[:-2]))
            for g in globs
        )

    uncovered = sorted(
        str(p.relative_to(REPO)) for p, _ in SITES.values() if not covered(str(p.relative_to(REPO)))
    )
    assert not uncovered, (
        f"these mapping sites are not in ci.yml's `llm` path filter: {uncovered}\n"
        "A PR touching only one of them would skip the job that runs this file, "
        "so the drift this guard exists to catch would merge green. Add the path "
        "to the filter."
    )


# ── the specific claim at docker-compose.quickstart.yml:309 ──────────────────
# The two kafka-init builders are not just a shared mapping: they write the same
# properties file. These two checks cover the rest of that file.

def _property_keys(text: str) -> set[str]:
    return {m.group(1) for m in re.finditer(r'echo\s+"([a-z][a-z0-9.]*\.[a-z][a-z0-9.]*)=', text)}


def _timeouts(text: str) -> dict[str, str]:
    return dict(re.findall(r"((?:request|default\.api)\.timeout\.ms)=(\d+)", text))


def test_the_two_kafka_init_builders_emit_the_same_client_property_keys():
    a, b = _property_keys(_text(COMPOSE)), _property_keys(_text(CHART_JOB))
    assert len(a) >= 8, f"parsed only {len(a)} keys from compose ({sorted(a)}) -- comparison would be vacuous"
    assert a == b, (
        "the two kafka-init builders no longer write the same properties file.\n"
        f"  only in docker-compose.quickstart.yml: {sorted(a - b)}\n"
        f"  only in the Helm kafka-init job:       {sorted(b - a)}\n"
        "A property present on one platform and not the other is a client that "
        "connects on Docker and blocks on Kubernetes (or the reverse), with no "
        "error naming the difference."
    )


def test_the_two_kafka_init_builders_agree_on_the_admin_client_timeouts():
    a, b = _timeouts(_text(COMPOSE)), _timeouts(_text(CHART_JOB))
    assert a, "no timeouts parsed -- the comparison below would be vacuous"
    assert a == b, (
        f"AdminClient timeouts drifted: compose={a} helm={b}.\n"
        "These are the bound on the failure both builders exist to make visible: "
        "a mismatched client BLOCKS rather than erroring, and the timeout is what "
        "turns that hang into a message."
    )


# ── the unsupported mechanism, rejected where each site is able to ───────────

def _case_block(text: str) -> str:
    """The case...esac that carries the login modules (files have others)."""
    for m in re.finditer(r"\bcase\b.*?\besac\b", text, re.S):
        if "LoginModule" in m.group(0):
            return m.group(0)
    return ""


def _default_arm(text: str) -> str:
    m = re.search(r"\*\)(.*?);;", _case_block(text), re.S)
    return m.group(1) if m else ""


@pytest.mark.parametrize(
    "name,path",
    [
        ("compose(quickstart kafka-init)", COMPOSE),
        ("scripts(kafka-init-new-topics.sh)", OPS_SCRIPT),
    ],
)
def test_the_runtime_sites_reject_an_unsupported_mechanism_by_name(name, path):
    """AWS_MSK_IAM is implemented in Go and in nothing that speaks JVM JAAS.

    The JVM CLI image ships no aws-msk-iam-auth jar, so these sites cannot honour
    it. Falling through instead of exiting produces a ClassNotFoundException at
    topic-create time with no explanation -- or worse, an empty jaas config and a
    hang. Both arms must exit AND name the mechanism, because "not supported"
    without the name sends the operator to the wrong half of the stack: the Go
    data plane really does implement it.
    """
    arm = _default_arm(_text(path))
    assert arm, f"{name}: no default (*) arm found in the login-module case statement"
    assert _token("AWS_MSK_IAM", arm), (
        f"{name}: the default arm no longer names AWS_MSK_IAM. Mentioning it "
        "elsewhere in the file does not count -- an operator reads the failure, "
        "not the file."
    )
    assert "exit 1" in arm, f"{name}: the default arm no longer exits; an unsupported mechanism falls through"


def test_the_chart_rejects_the_unsupported_mechanism_at_render_time():
    """The chart's rejection is `fail`, inside the helper -- not a runtime echo.

    Rejecting at render means the job never exists, which is why the Helm
    kafka-init default arm does not name AWS_MSK_IAM: it never has to. Delete
    this `fail` and that asymmetry stops being correct and starts being a gap,
    silently.
    """
    text = _text(HELPERS)
    start = text.find('{{- define "rsync-ai.kafka.saslLoginModule" -}}')
    assert start >= 0, "the saslLoginModule helper is gone -- _helm_if_modules parses nothing"
    nxt = text.find("{{- define ", start + 1)
    block = text[start : nxt if nxt > 0 else len(text)]

    assert re.search(r"\bfail\b", block), (
        "the saslLoginModule helper no longer calls `fail`. Without it an "
        "unsupported mechanism renders an EMPTY login module, and an empty "
        "sasl.jaas.config does not error -- it hangs at connect time."
    )
    assert _token("AWS_MSK_IAM", block), (
        "the helper's fail message no longer names AWS_MSK_IAM. It must, because "
        "the Go data plane DOES implement the mechanism -- without the name the "
        "operator cannot tell a chart gap from a product gap."
    )


# ── controls: every parser above must be able to fail ────────────────────────

@pytest.mark.parametrize("name", sorted(SITES))
def test_control_each_parser_reads_its_own_site(name):
    """A positive denominator per site.

    Every assertion here is an equality against CANONICAL. If a refactor changes
    a file's syntax, its parser returns {} and the only symptom is a passing
    test that has stopped reading anything. This makes the empty parse loud.
    """
    path, parse = SITES[name]
    got = parse(_text(path))
    assert len(got) == len(CANONICAL), (
        f"{name}: parsed {len(got)} mechanism->module entries ({got}); "
        f"expected {len(CANONICAL)}. The file's syntax changed and its parser no "
        "longer reads it -- fix the parser, do not delete the site."
    )


def test_control_the_parsers_and_token_check_discriminate():
    """Mutating each site must change what its parser sees.

    The `_token` control is here because its absence is a bug this file already
    had: a substring check called AWS_MSK_IAM_REMOVED_MARKER a pass, so a
    mutation deleting the chart's render-time rejection went undetected.
    """
    # shell case arms
    shell = _text(CHART_JOB)
    swapped = shell.replace(f"MOD={OAUTH_MODULE}", f"MOD={PLAIN_MODULE}")
    assert swapped != shell, "mutation did not apply -- this control proves nothing"
    assert _case_arm_modules(swapped) != _case_arm_modules(shell)

    # helm if/else helper
    tpl = _text(HELPERS)
    renamed = tpl.replace('eq $m "OAUTHBEARER"', 'eq $m "OAUTHBEARER-X"')
    assert renamed != tpl, "mutation did not apply -- this control proves nothing"
    assert _helm_if_modules(renamed) != _helm_if_modules(tpl)

    # python dict, through the MECHANISM_* indirection
    py = _text(KAFKA_SECURITY)
    reconst = py.replace('MECHANISM_SCRAM_SHA_512 = "SCRAM-SHA-512"', 'MECHANISM_SCRAM_SHA_512 = "SCRAM-SHA-511"')
    assert reconst != py, "mutation did not apply -- this control proves nothing"
    assert _py_dict_modules(reconst) != _py_dict_modules(py), (
        "the python parser is not resolving MECHANISM_* constants -- it would "
        "miss a renamed mechanism entirely"
    )

    # whole-token containment
    assert _token("AWS_MSK_IAM", "supports AWS_MSK_IAM here")
    assert not _token("AWS_MSK_IAM", "supports AWS_MSK_IAM_REMOVED_MARKER here")
