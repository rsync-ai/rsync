"""The e2e gate deletes the Kafka topics its CDC tests create.

Every CDC test registers a Debezium connector whose `topic.prefix` is its own
fresh, timestamped name, so a run creates `<name>`, `<name>.<db>.<table>` (and
`.dlq`) and `schemahistory.<name>`. Deleting a connector does not delete its
topics, and nothing else did: by 2026-09-27 the warm CI Kafka held 916 of them.
The gate now reclaims the tests' own namespace -- `debug-*`, `e2e-*` and their
`schemahistory.` twins -- when it starts and when it exits.

Three things have to hold, and each has its own test here:
  * reclaim_e2e_topics(), cut out of e2e/run_gate.sh and run against a fake
    Kafka, deletes exactly that namespace and nothing a pipeline or Kafka owns;
  * every topic an e2e test creates falls inside the namespace, so a new test
    cannot leak by picking another prefix (test_mysql_cdc_to_real_s3.sh did);
  * the gate calls it, connectors first: a live connector recreates its topics.

Nothing here can reach a real docker: the PATH holds only the fakes and a
handful of text tools.
"""

import os
import pathlib
import re
import shutil
import stat
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
GATE = REPO / "e2e" / "run_gate.sh"
E2E = REPO / "e2e"
# The CI runner's /bin/bash is 3.2, the oldest the gate has to run on.
BASH = "/bin/bash" if os.path.exists("/bin/bash") else shutil.which("bash")
TOOLS = ("grep", "sed", "awk", "cat", "mv", "rm", "tr", "wc", "head", "mkdir")

# The namespace the tests create topics in and the gate deletes.
TEST_TOPIC = re.compile(r"^(schemahistory\.)?(debug|e2e)-")

KAFKA = "rsync-ci-kafka"
CONNECT = "rsync-ci-kafka-connect"

TEST_TOPICS = [
    "debug-cdc-pg-mysql-1759000000",
    "debug-cdc-pg-mysql-1759000000.public.orders",
    "debug-cdc-pg-mysql-1759000000.public.orders.dlq",
    "schemahistory.debug-cdc-pg-mysql-1759000000",
    "e2e-cdc-1759000001",
    "e2e-cdc-1759000001.testdb.users",
    "schemahistory.e2e-cdc-1759000001",
    "e2e-hybrid-cdc-1759000002.dimensions",
]
KEPT_TOPICS = [
    # product pipelines: the pipeline delete path owns these
    "rsync.pipeline.90ec0f81.data",
    "rsync.cdc-1a2b3c4d.public.orders",
    "cdc-1a2b3c4d.public.orders",
    "schemahistory.cdc-1a2b3c4d",
    # Kafka, Connect and the schema registry
    "__consumer_offsets",
    "connect-configs",
    "connect-offsets",
    "connect-status",
    "_schemas",
    # near misses of the namespace
    "rsync.debug-x",
    "cdc-e2e-x",
    "debugger",
    "e2e_x",
    "my-debug-topic",
    # would be deleted too if the gate left the `.` in a name unescaped (Kafka
    # refuses the `_` spelling beside the `.` one, so the near miss is a `-`)
    "schemahistory-debug-cdc-pg-mysql-1759000000",
]

FAKE_DOCKER = r"""#!/bin/bash
S="$FAKE_STATE"
echo "$*" >> "$S/docker.log"
[ "$1" = exec ] || { echo "fake docker: unexpected call: $*" >&2; exit 2; }
ctr="$2"
if [ -f "$S/down" ]; then echo "Error response from daemon: container $ctr is not running" >&2; exit 1; fi
case " $* " in
  *" bash -lc "*)
    [ "$ctr" = "$FAKE_CONNECT" ] || { echo "Error: No such container: $ctr" >&2; exit 1; }
    exec /bin/bash -c "${@: -1}" ;;
  *"/kafka-topics.sh "*)
    [ "$ctr" = "$FAKE_KAFKA" ] || { echo "Error: No such container: $ctr" >&2; exit 1; } ;;
  *) echo "fake docker: unexpected call: $*" >&2; exit 2 ;;
esac
op=""; re=""; ifx=""; prev=""
for a in "$@"; do
  case "$a" in --list) op=list ;; --delete) op=delete ;; --if-exists) ifx=1 ;; esac
  [ "$prev" = "--topic" ] && re="$a"
  prev="$a"
done
case "$op" in
  list) cat "$S/topics" ;;
  delete)
    if [ -f "$S/fail_delete" ]; then echo "Error while executing topic command : Timed out waiting for a node assignment." >&2; exit 1; fi
    echo "$re" >> "$S/delete_patterns"
    # kafka-topics reads --topic as a regex that must match the whole name
    if ! grep -Eqx -- "$re" "$S/topics" && [ -z "$ifx" ]; then
      echo "Error while executing topic command : Topic '$re' does not exist as expected" >&2; exit 1
    fi
    grep -Evx -- "$re" "$S/topics" > "$S/topics.new"
    mv "$S/topics.new" "$S/topics" ;;
  *) echo "fake kafka-topics: unexpected call: $*" >&2; exit 2 ;;
esac
"""

# Connect's REST API, as the reclaim script inside the connect container sees it.
FAKE_CURL = r"""#!/bin/bash
S="$FAKE_STATE"
url="${@: -1}"
case " $* " in
  *" -X DELETE "*)
    name="${url##*/}"
    echo "$name" >> "$S/deleted_connectors"
    grep -vx -- "$name" "$S/connectors" > "$S/connectors.new"
    mv "$S/connectors.new" "$S/connectors" ;;
  *) awk 'BEGIN { printf "[" } { printf "%s\"%s\"", (NR > 1 ? "," : ""), $0 } END { printf "]" }' "$S/connectors" ;;
esac
"""


def _extract(text: str, name: str) -> str:
    m = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert m, f"{name}() not found in {GATE}"
    return m.group(0)


@pytest.fixture
def gate(tmp_path):
    text = GATE.read_text()
    lib = tmp_path / "reclaim_lib.sh"
    lib.write_text("\n".join(
        _extract(text, f) for f in ("delete_topics_exactly", "reclaim_e2e_connectors", "reclaim_e2e_topics", "on_exit")
    ))

    bindir = tmp_path / "bin"
    bindir.mkdir()
    for tool in TOOLS:
        real = shutil.which(tool)
        assert real, f"{tool} missing on this host"
        (bindir / tool).symlink_to(real)
    for fname, body in (("docker", FAKE_DOCKER), ("curl", FAKE_CURL)):
        p = bindir / fname
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IXUSR)

    state = tmp_path / "state"
    state.mkdir()

    class Gate:
        lock = tmp_path / "stack.lock"

        def put(self, name, lines):
            (state / name).write_text("".join(f"{ln}\n" for ln in lines))

        def lines(self, name):
            f = state / name
            return f.read_text().splitlines() if f.exists() else []

        def flag(self, name):
            (state / name).write_text("")

        def run(self, snippet, **env_extra):
            harness = (
                "set -uo pipefail\n"
                "log()  { printf '\\n==> %s\\n' \"$*\"; }\n"
                "warn() { printf 'WARN: %s\\n' \"$*\" >&2; }\n"
                f"source '{lib}'\n"
                f"KAFKA_CONTAINER={KAFKA}; KAFKA_CONNECT_CONTAINER={CONNECT}\n"
                f"STACK_LOCK_DIR='{self.lock}'\n"
                f"{snippet}\n"
            )
            env = {"PATH": str(bindir), "FAKE_STATE": str(state), "HOME": str(tmp_path),
                   "FAKE_KAFKA": KAFKA, "FAKE_CONNECT": CONNECT, **env_extra}
            return subprocess.run([BASH, "-c", harness], env=env, capture_output=True, text=True, timeout=60)

    return Gate()


# --- reclaim_e2e_topics: deletes the test namespace, nothing else ------------


def test_deletes_exactly_the_test_namespace(gate):
    gate.put("topics", TEST_TOPICS + KEPT_TOPICS)
    r = gate.run("reclaim_e2e_topics")
    assert r.returncode == 0, r.stderr
    assert sorted(gate.lines("topics")) == sorted(KEPT_TOPICS), r.stdout + r.stderr
    assert f"deleted {len(TEST_TOPICS)} leftover e2e topic" in r.stdout
    # the fixture lists are what the census below relies on
    assert all(TEST_TOPIC.match(t) for t in TEST_TOPICS)
    assert not any(TEST_TOPIC.match(t) for t in KEPT_TOPICS)


def test_deletes_in_batches_and_counts_every_topic(gate):
    many = [f"debug-cdc-mysql-pg-{1759000000 + i}.e2e_db.users" for i in range(120)]
    gate.put("topics", many + KEPT_TOPICS)
    r = gate.run("reclaim_e2e_topics")
    assert r.returncode == 0, r.stderr
    assert sorted(gate.lines("topics")) == sorted(KEPT_TOPICS)
    patterns = gate.lines("delete_patterns")
    assert len(patterns) == 3, patterns
    assert all(p.count("|") < 50 for p in patterns), "a batch holds more than 50 names"
    assert "deleted 120 leftover e2e topic" in r.stdout


def test_nothing_to_reclaim_deletes_nothing(gate):
    gate.put("topics", KEPT_TOPICS)
    r = gate.run("reclaim_e2e_topics")
    assert r.returncode == 0, r.stderr
    assert gate.lines("delete_patterns") == []
    assert "no leftover e2e topics" in r.stdout
    assert sorted(gate.lines("topics")) == sorted(KEPT_TOPICS)


def test_a_failed_delete_warns_and_never_fails_the_gate(gate):
    # three batches' worth, so "stops at the first failure" is observable
    gate.put("topics", [f"debug-cdc-mysql-pg-{1759000000 + i}" for i in range(120)] + KEPT_TOPICS)
    gate.flag("fail_delete")
    r = gate.run("reclaim_e2e_topics; echo rc=$?")
    assert "rc=0" in r.stdout
    assert "WARN: topic reclaim" in r.stderr
    assert sum("--delete" in c for c in gate.lines("docker.log")) == 1, "kept deleting after a failure"


def test_an_unreachable_kafka_warns_and_never_fails_the_gate(gate):
    gate.put("topics", TEST_TOPICS)
    gate.flag("down")
    r = gate.run("reclaim_e2e_topics; echo rc=$?")
    assert "rc=0" in r.stdout
    assert "WARN: topic reclaim" in r.stderr
    assert "no leftover" not in r.stdout, "an unreachable Kafka reported as clean"
    assert not any("--delete" in c for c in gate.lines("docker.log"))


def test_opt_out_touches_nothing(gate):
    gate.put("topics", TEST_TOPICS)
    r = gate.run("reclaim_e2e_topics", E2E_SKIP_TOPIC_RECLAIM="1")
    assert r.returncode == 0, r.stderr
    assert gate.lines("docker.log") == []
    assert sorted(gate.lines("topics")) == sorted(TEST_TOPICS)


# --- reclaim_e2e_connectors: covers both test prefixes ------------------------


def test_connector_reclaim_covers_both_test_prefixes(gate):
    test_connectors = ["debug-cdc-pg-mysql-1759000000", "e2e-cdc-1759000001", "e2e-hybrid-cdc-1759000002"]
    kept = ["cdc-1a2b3c4d", "rsync-prod-x", "my-debug-conn", "e2e_x"]
    gate.put("connectors", test_connectors + kept)
    r = gate.run("reclaim_e2e_connectors")
    assert r.returncode == 0, r.stderr
    assert sorted(gate.lines("connectors")) == sorted(kept), r.stdout + r.stderr
    assert sorted(gate.lines("deleted_connectors")) == sorted(test_connectors)


# --- on_exit: reclaims after the run, connectors first, only its own stack ----

STUBS = (
    'calls() { echo "$1" >> "$FAKE_STATE/calls"; }\n'
    "dc_e2e() { calls dc_e2e; }\n"
    "reclaim_e2e_connection_rows() { calls connection_rows; }\n"
    "reclaim_e2e_pipelines() { calls pipelines; }\n"
    "reclaim_e2e_connectors() { calls connectors; }\n"
    "reclaim_e2e_topics() { calls topics; }\n"
    "release_stack_lock() { calls release; }\n"
)


def test_on_exit_reclaims_connectors_then_topics(gate):
    r = gate.run(STUBS + 'mkdir -p "$STACK_LOCK_DIR"; echo $$ > "$STACK_LOCK_DIR/pid"\n(exit 3); on_exit')
    assert r.returncode == 3, "on_exit must keep the gate's exit code"
    calls = gate.lines("calls")
    assert {"dc_e2e", "connection_rows", "pipelines", "connectors", "topics"} <= set(calls), calls
    assert calls.index("connectors") < calls.index("topics"), "a live connector recreates the topics just deleted"
    assert calls.index("topics") < calls.index("release")


def test_on_exit_leaves_another_owners_stack_alone(gate):
    # The lock was never ours: the acquire timed out while another run held it.
    r = gate.run(STUBS + 'mkdir -p "$STACK_LOCK_DIR"; echo 1 > "$STACK_LOCK_DIR/pid"\n(exit 1); on_exit')
    assert r.returncode == 1
    assert (gate.lock / "pid").read_text().strip() == "1", r.stderr  # the lock really is someone else's
    calls = gate.lines("calls")
    # ALL of it belongs to the owner: `down -v` on its e2e DB fixtures and the
    # connection-row delete would pull the stack out from under its running tests
    # just as surely as a topic delete. Only the topics were guarded at first.
    for reaped in ("dc_e2e", "connection_rows", "pipelines", "connectors", "topics"):
        assert reaped not in calls, f"on_exit ran {reaped} on another owner's stack: {calls}"
    assert "release" in calls, "on_exit did not run to the end"


def test_on_exit_keeps_everything_under_e2e_keep(gate):
    r = gate.run(STUBS + 'mkdir -p "$STACK_LOCK_DIR"; echo $$ > "$STACK_LOCK_DIR/pid"\non_exit', E2E_KEEP="1")
    assert r.returncode == 0, r.stderr
    assert not {"pipelines", "topics"} & set(gate.lines("calls"))


# --- the gate's own wiring ----------------------------------------------------


def test_gate_reclaims_topics_at_start_after_connectors():
    text = GATE.read_text()
    trap = text.index("\ntrap on_exit EXIT\n")
    assert text.index("\nreclaim_e2e_topics() {") < trap, (
        "reclaim_e2e_topics must be defined above the EXIT trap, or an early exit cannot resolve it"
    )
    top = [m.group(1) for m in re.finditer(r"^(reclaim_e2e_\w+)\s*$", text[trap:], re.M)]
    assert "reclaim_e2e_topics" in top, "the gate never reclaims topics before its tests"
    assert top.index("reclaim_e2e_connectors") < top.index("reclaim_e2e_topics")
    assert re.search(r'^KAFKA_CONTAINER="\$\{STACK_PREFIX\}-kafka"', text, re.M), (
        "both stacks name their broker <prefix>-kafka"
    )


# --- census: every topic an e2e test creates is in the reclaimed namespace ----

# Where a test names a topic it creates: the connector's prefix and schema
# history topic, and topics it creates by hand with kafka-topics.
TOPIC_SITE = re.compile(
    r'"(?:topic\.prefix|schema\.history\.internal\.kafka\.topic)"\s*:\s*(?:f?(["\'])(.*?)\1|(\w+))'
    r"|kafka-topics(?:\.sh)?\s[^\n]*--create[^\n]*--topic\s+[\"']?([^\"'\s]+)"
)
REF_HEAD = re.compile(r"\$\{(\w+)\}|\{(\w+)\}")


def _nearest_assignment(text: str, var: str, pos: int):
    assign = re.compile(rf"(?:^|[\s;(])(?:local\s+|export\s+)?{var}\s*=\s*f?([\"'])(.*?)\1", re.M)
    before = [m for m in assign.finditer(text) if m.start() < pos]
    return before[-1] if before else None


def _resolve(text: str, value: str, pos: int, bare: bool, where: str, depth: int = 0) -> str:
    """`value` with its leading variable reference expanded, through the nearest
    assignment above `pos`, until it starts with literal text."""
    assert depth < 8, f"{where}: reference chain too deep"
    head = ""
    if value.startswith("schemahistory."):
        head, value = "schemahistory.", value[len("schemahistory."):]
    if bare:  # an unquoted identifier: the whole value is one variable
        var, rest = value, ""
    else:
        m = REF_HEAD.match(value)
        if not m:
            return head + value
        var, rest = m.group(1) or m.group(2), value[m.end():]
    a = _nearest_assignment(text, var, pos)
    assert a, f"{where}: no assignment of `{var}` above its use"
    return head + _resolve(text, a.group(2), a.start(), False, where, depth + 1) + rest


def _topic_sites():
    for path in sorted(E2E.glob("test_*")):
        if path.suffix not in (".py", ".sh"):
            continue
        text = path.read_text()
        for m in TOPIC_SITE.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            where = f"{path.relative_to(REPO)}:{line}"
            if m.group(3):
                yield where, _resolve(text, m.group(3), m.start(), True, where)
            else:
                yield where, _resolve(text, m.group(2) or m.group(4), m.start(), False, where)


def test_every_topic_an_e2e_test_creates_is_reclaimed():
    sites = list(_topic_sites())
    # 19 tests register a connector; each names a prefix and most a history topic.
    assert len(sites) >= 30, f"census found only {len(sites)} topic sites -- the pattern has drifted"
    outside = [f"{where} -> {topic}" for where, topic in sites if not TEST_TOPIC.match(topic)]
    assert not outside, (
        "these e2e topics fall outside the namespace the gate reclaims "
        "(`debug-*`/`e2e-*`, optionally `schemahistory.`-prefixed), so every run leaks them:\n  "
        + "\n  ".join(outside)
    )


def test_every_connector_registering_test_is_in_the_census():
    registering = {p.name for p in E2E.glob("test_*") if '"connector.class"' in p.read_text()}
    covered = {where.split(":")[0].rsplit("/", 1)[-1] for where, _ in _topic_sites()}
    assert registering, "no e2e test registers a connector -- the census is looking in the wrong place"
    assert registering <= covered, f"registers a connector but names no topic the census can see: {sorted(registering - covered)}"
