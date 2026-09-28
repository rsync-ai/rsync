"""The e2e gate deletes the pipelines its tests create, and their Kafka names.

Each gated batch test creates a pipeline per run and never deletes it, so the
row stays, and with it the `pipeline.<id8>.data` topic and `sink-<id8>-batch`
group the run left: 249 rows in the warm CI stack by 2026-09-27, 193 of them
golden-types-*. The API delete is no way out there -- it calls the orchestrator's
kafka-teardown with an INTERNAL_SERVICE_SECRET the gate stack leaves empty and
gets a 403 -- so the gate reclaims them itself, when it starts and when it exits.

What has to hold, each with its own test here:
  * reclaim_e2e_pipelines(), cut out of e2e/run_gate.sh and run against a fake
    Postgres, Kafka and sink service, deletes the rows whose name is a gated
    test's name plus its run suffix -- nothing a person named -- and, of Kafka,
    only the names derived from those rows (cdc_kafka_teardown.go's
    ownsTopic/ownsGroup shapes);
  * their sink workers stop first. A batch run's worker outlives the run, and a
    live worker recreates the topic it reads the moment it is deleted and holds
    its group -- which the fake Kafka does too, as the CI stack's did on
    2026-09-27. A worker that will not stop keeps its pipeline for the next gate;
  * an id8 a kept pipeline shares is not swept (that file's id8IsUnique rule);
  * Kafka goes before the rows, so an unreachable Kafka leaves the rows for the
    next gate instead of orphaning their topics for good;
  * no failure fails the gate;
  * every pipeline a gated test creates carries a name the pattern matches.

Nothing here can reach a real docker: the PATH holds only the fakes and a
handful of text tools, and the gate's own sink program is pointed at a fake
sink service on 127.0.0.1.
"""

import http.server
import json
import os
import pathlib
import re
import shutil
import stat
import subprocess
import sys
import threading

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
GATE = REPO / "e2e" / "run_gate.sh"
E2E = REPO / "e2e"
# The CI runner's /bin/bash is 3.2, the oldest the gate has to run on.
BASH = "/bin/bash" if os.path.exists("/bin/bash") else shutil.which("bash")
TOOLS = ("grep", "sed", "awk", "cat", "cut", "sort", "comm", "paste", "tr", "mv", "rm", "wc", "head", "mkdir")

PG, KAFKA, CONNECT = "rsync-ci-postgres", "rsync-ci-kafka", "rsync-ci-kafka-connect"
SINK = "rsync-ci-kafka-mcp-sink-v1-0-0-mcp"


def _uuid(id8, n):
    return f"{id8}-0000-4000-8000-{n:012d}"


# Rows the gate's tests created: one per name shape the gated buckets use.
TEST_ROWS = {
    _uuid("aaaa0001", 1): "golden-types-1759000000",
    _uuid("aaaa0002", 2): "e2e-batch-minio-1759000001",
    _uuid("aaaa0003", 3): "e2e-batch-reload-resume-1759000002",
    _uuid("aaaa0004", 4): "pgleak-1759000003",
    _uuid("aaaa0005", 5): "pgns-1759000004",
    _uuid("aaaa0006", 6): "e2e-ns-batch-1759000005",
    _uuid("aaaa0007", 7): "gh-oauth-pipe-1a2b3c4d",
    _uuid("aaaa0008", 8): "widgets-graphql-to-postgres-9f8e7d6c",
    _uuid("aaaa0009", 9): "e2e-ss-cdc-1759000006",
}
KEPT_ROWS = {
    _uuid("bbbb0001", 11): "orders to warehouse",
    _uuid("bbbb0002", 12): "golden-types",  # no run suffix
    _uuid("bbbb0003", 13): "golden-types-final",  # a word, not a run suffix
    _uuid("bbbb0004", 14): "golden-types-1759000000-copy",  # suffix is not the end
    _uuid("bbbb0005", 15): "my-golden-types-1759000000",  # prefix is not the start
    _uuid("bbbb0006", 16): "e2e-test-1",  # a person's "e2e" pipeline: suffix too short
    _uuid("bbbb0007", 17): "mongo-multi-to-gcs-1759000000",  # ungated test: run by hand
    _uuid("bbbb0008", 18): "pgleak-deadbee",  # 7 hex digits
    _uuid("abcdef01", 19): "prod-cdc",  # its id8 is a near miss of a test row's below
}
NEAR_ROW = (_uuid("abcdef01".replace("01", "0f"), 20), "golden-types-1759000009")  # abcdef0f


def _kafka_of(uid):
    """Every topic and group cdc_kafka_teardown.go says a pipeline owns."""
    i8 = uid[:8]
    topics = [
        f"rsync.pipeline.{i8}.data", f"rsync.pipeline.{i8}.data.dlq", f"pipeline.{i8}.data",
        f"rsync.cdc-{i8}", f"rsync.cdc-{i8}.inventory.orders", f"rsync.cdc-{i8}.inventory.orders.dlq",
        f"cdc-{i8}.inventory.orders", f"schemahistory.cdc-{i8}", f"rsync.schemahistory.cdc-{i8}",
        f"rsync.signals.{i8}", f"rsync.heartbeat.rsync.cdc-{i8}", f"rsync.heartbeat.cdc-{i8}",
    ]
    groups = [
        f"rsync.sink-{i8}-batch", f"rsync.sink-{i8}-stream", f"sink-{i8}", f"rsync.sink-{i8}",
        f"rsync.cdc-{i8}-signal", f"rsync.cdc-schema-changes-{uid}", f"cdc-table-stats-{uid}",
    ]
    return topics, groups


OTHER_TOPICS = [
    "__consumer_offsets", "connect-configs", "connect-offsets", "connect-status", "_schemas",
    "e2e-cdc-1759000001.testdb.users",  # reclaim_e2e_topics' job, not this one
    "rsync.pipeline.aaaa00011.data",  # an id8 followed by more hex is another pipeline
    "rsync.cdc-aaaa00011.inventory.orders",
    "rsync.signals.aaaa0001x",
]
OTHER_GROUPS = ["connect-cluster", "rsync.sink-aaaa00011-batch", "rsync.cdc-aaaa0001-signalx"]

FAKE_DOCKER = r"""#!__PY__
import os, re, sys
S = os.environ["FAKE_STATE"]
args = sys.argv[1:]
with open(f"{S}/docker.log", "a") as f:  # a script argument logs as <script>: one line per call
    f.write(" ".join("<script>" if "\n" in a else a for a in args) + "\n")

def lines(name):
    p = f"{S}/{name}"
    return open(p).read().splitlines() if os.path.exists(p) else []

def put(name, rows):
    open(f"{S}/{name}", "w").write("".join(r + "\n" for r in rows))

def flag(name):
    return os.path.exists(f"{S}/{name}")

def live():
    # worker_id (its consumer group) | pipeline_id | the topic it reads
    return [ln.split("|") for ln in lines("workers")]

if args[:1] != ["exec"]:
    sys.exit(f"fake docker: unexpected call: {args}")
ctr = args[1] if args[1] != "-i" else args[2]
if flag(f"down_{ctr}"):
    sys.exit(f"Error response from daemon: container {ctr} is not running")
if "python3" in args:
    # The gate's own program, pointed at the fake sink service; stdin passes through.
    if ctr != os.environ["FAKE_SINK"] or args[1] != "-i":
        sys.exit(f"fake docker: unexpected call: {args}")
    prog = args[args.index("-c") + 1].replace("http://localhost:8000/", os.environ["FAKE_SINK_URL"])
    os.execv(sys.executable, [sys.executable, "-c", prog])
if "bash" in args and "-lc" in args:
    if ctr != os.environ["FAKE_CONNECT"]:
        sys.exit(f"Error: No such container: {ctr}")
    os.execv("/bin/bash", ["/bin/bash", "-c", args[-1]])
if "psql" in args:
    if ctr != os.environ["FAKE_PG"]:
        sys.exit(f"Error: No such container: {ctr}")
    sql = args[-1]
    with open(f"{S}/sql.log", "a") as f:
        f.write(sql + "\n")
    rows = lines("pipelines")
    if sql == "SELECT id, name FROM pipelines":
        print("\n".join(rows))
    elif sql.startswith("WITH del AS (DELETE FROM pipelines WHERE id = ANY('{"):
        if flag("fail_pg_delete"):
            sys.exit('ERROR:  update or delete on table "pipelines" violates a constraint')
        ids = set(re.search(r"ANY\('\{([^}]*)\}'::uuid\[\]\)", sql).group(1).split(","))
        keep = [r for r in rows if r.split("|", 1)[0] not in ids]
        put("pipelines", keep)
        print(len(rows) - len(keep))
    else:
        sys.exit(f"fake psql: unexpected SQL: {sql}")
    sys.exit(0)
if ctr != os.environ["FAKE_KAFKA"]:
    sys.exit(f"Error: No such container: {ctr}")
tool = args[2].rsplit("/", 1)[-1]
if tool == "kafka-topics.sh":
    if "--list" in args:
        print("\n".join(lines("topics")))
    elif "--delete" in args:
        if flag("fail_topic_delete"):
            sys.exit("Error while executing topic command : Timed out waiting for a node assignment.")
        pat = args[args.index("--topic") + 1]
        with open(f"{S}/topic_deletes", "a") as f:
            f.write(pat + "\n")
        # A live worker's consumer recreates the topic it reads as soon as it is gone.
        back = [w[2] for w in live() if re.fullmatch(pat, w[2])]
        put("topics", [t for t in lines("topics") if not re.fullmatch(pat, t)] + back)
    else:
        sys.exit(f"fake kafka-topics: unexpected call: {args}")
elif tool == "kafka-consumer-groups.sh":
    if "--list" in args:
        print("\n".join(lines("groups")))
    elif "--delete" in args:
        asked = [args[i + 1] for i, a in enumerate(args) if a == "--group"]
        with open(f"{S}/group_deletes", "a") as f:
            f.write(" ".join(asked) + "\n")
        busy = set(lines("busy_groups")) | {w[0] for w in live()}
        put("groups", [g for g in lines("groups") if g not in asked or g in busy])
        if busy & set(asked):
            sys.exit("Error: Deletion of some consumer groups failed: GroupNotEmptyException")
    else:
        sys.exit(f"fake kafka-consumer-groups: unexpected call: {args}")
else:
    sys.exit(f"fake docker: unexpected tool: {args}")
""".replace("__PY__", sys.executable)

# Connect's REST API, as the reclaim script inside the connect container sees it.
FAKE_CURL = r"""#!/bin/bash
S="$FAKE_STATE"
url="${@: -1}"
case " $* " in
  *" -X DELETE "*)
    name="${url##*/}"
    echo "$name" >> "$S/deleted_connectors"
    echo "exec x bash -lc DELETE connector $name" >> "$S/docker.log"
    grep -vx -- "$name" "$S/connectors" > "$S/connectors.new"
    mv "$S/connectors.new" "$S/connectors" ;;
  *) awk 'BEGIN { printf "[" } { printf "%s\"%s\"", (NR > 1 ? "," : ""), $0 } END { printf "]" }' "$S/connectors" ;;
esac
"""


class _FakeSink(http.server.BaseHTTPRequestHandler):
    """kafka-mcp-sink's list_sinks/stop_sink (connector.py), over the state dir."""

    def do_POST(self):
        s = self.server.state
        cfg = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["config"]
        tool = self.path.strip("/")
        rd = lambda n: (s / n).read_text().splitlines() if (s / n).exists() else []  # noqa: E731
        with open(s / "docker.log", "a") as f:
            f.write(f"sink {tool} {cfg.get('consumer_group', '')}\n")
        workers = [w.split("|") for w in rd("workers")]
        if tool == "list_sinks":
            if (s / "fail_list_sinks").exists():
                return self._send(503, {"detail": "Service Unavailable"})
            return self._send(200, {"success": True, "status": "ok", "workers": [
                {"worker_id": w, "consumer_group": w, "pipeline_id": p, "intentional_stop": False, "crashed": False}
                for w, p, _ in workers]})
        wid = cfg["consumer_group"]
        if wid in rd("stuck_workers"):
            return self._send(200, {"success": False, "error": f"Error stopping worker {wid}: timed out"})
        gone = wid in rd("vanished_workers") or wid not in {w for w, _, _ in workers}
        (s / "workers").write_text("".join(f"{'|'.join(w)}\n" for w in workers if w[0] != wid))
        if gone:
            return self._send(200, {"success": False, "error": f"Worker not found: {wid}"})
        return self._send(200, {"success": True, "status": "stopped", "worker_id": wid, "stopped": [wid]})

    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        pass


def _extract(text, name):
    m = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", text, re.S | re.M)
    assert m, f"{name}() not found in {GATE}"
    return m.group(0)


def _pattern_line(text):
    m = re.search(r"^E2E_PIPELINE_NAME_RE='([^']+)'$", text, re.M)
    assert m, f"E2E_PIPELINE_NAME_RE='...' not found in {GATE}"
    return m


def _name_re():
    return re.compile(_pattern_line(GATE.read_text()).group(1))


@pytest.fixture
def gate(tmp_path):
    text = GATE.read_text()
    lib = tmp_path / "reclaim_lib.sh"
    lib.write_text(
        _pattern_line(text).group(0) + "\n"
        + "\n".join(_extract(text, f) for f in ("delete_topics_exactly", "stop_sink_workers_of", "reclaim_e2e_pipelines"))
    )
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
    sink = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _FakeSink)
    sink.state = state
    threading.Thread(target=sink.serve_forever, daemon=True).start()

    class Gate:
        def put(self, name, lines):
            (state / name).write_text("".join(f"{ln}\n" for ln in lines))

        def rows(self, rows):
            self.put("pipelines", [f"{k}|{v}" for k, v in rows.items()])

        def lines(self, name):
            f = state / name
            return f.read_text().splitlines() if f.exists() else []

        def flag(self, name):
            (state / name).write_text("")

        def run(self, snippet="reclaim_e2e_pipelines; echo rc=$?", **env_extra):
            harness = (
                "set -uo pipefail\n"
                "log()  { printf '\\n==> %s\\n' \"$*\"; }\n"
                "warn() { printf 'WARN: %s\\n' \"$*\" >&2; }\n"
                f"source '{lib}'\n"
                f"ORCH_PG_CONTAINER={PG}; KAFKA_CONTAINER={KAFKA}; KAFKA_CONNECT_CONTAINER={CONNECT}\n"
                f"KAFKA_SINK_CONTAINER={SINK}\n"
                f"{snippet}\n"
            )
            env = {"PATH": str(bindir), "FAKE_STATE": str(state), "HOME": str(tmp_path),
                   "FAKE_PG": PG, "FAKE_KAFKA": KAFKA, "FAKE_CONNECT": CONNECT, "FAKE_SINK": SINK,
                   "FAKE_SINK_URL": f"http://127.0.0.1:{sink.server_address[1]}/",
                   # urllib reads the macOS system proxy settings unless told not to
                   "no_proxy": "*", **env_extra}
            return subprocess.run([BASH, "-c", harness], env=env, capture_output=True, text=True, timeout=60)

    g = Gate()
    for name in ("pipelines", "topics", "groups", "connectors", "workers"):
        g.put(name, [])
    yield g
    sink.shutdown()
    sink.server_close()


def _worker_of(uid):
    return f"rsync.sink-{uid[:8]}-batch|{uid}|rsync.pipeline.{uid[:8]}.data"


def _world(gate, rows, live=None):
    """The rows, their Kafka names and connectors, and -- as a batch run leaves
    them -- a sink worker still running for each (or for `live` only)."""
    topics, groups = list(OTHER_TOPICS), list(OTHER_GROUPS)
    for uid in rows:
        t, g = _kafka_of(uid)
        topics += t
        groups += g
    gate.rows(rows)
    gate.put("topics", topics)
    gate.put("groups", groups)
    gate.put("connectors", [f"cdc-{uid[:8]}" for uid in rows] + ["e2e-cdc-1759000001"])
    gate.put("workers", [_worker_of(uid) for uid in (rows if live is None else live)])


# --- the pattern: the gate's names, never a person's ---------------------------


def test_the_pattern_names_the_test_rows_and_nothing_else():
    name_re = _name_re()
    assert all(name_re.match(n) for n in TEST_ROWS.values())
    assert not any(name_re.match(n) for n in KEPT_ROWS.values())


# --- reclaim_e2e_pipelines: rows, then only their own Kafka names --------------


def test_deletes_the_test_rows_and_only_their_kafka_names(gate):
    _world(gate, {**TEST_ROWS, **KEPT_ROWS})
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    assert sorted(gate.lines("pipelines")) == sorted(f"{k}|{v}" for k, v in KEPT_ROWS.items()), r.stdout + r.stderr

    kept_topics, kept_groups = list(OTHER_TOPICS), list(OTHER_GROUPS)
    for uid in KEPT_ROWS:
        t, g = _kafka_of(uid)
        kept_topics += t
        kept_groups += g
    assert sorted(gate.lines("topics")) == sorted(kept_topics), r.stdout + r.stderr
    assert sorted(gate.lines("groups")) == sorted(kept_groups), r.stdout + r.stderr
    assert sorted(gate.lines("connectors")) == sorted(
        [f"cdc-{uid[:8]}" for uid in KEPT_ROWS] + ["e2e-cdc-1759000001"]
    )
    assert sorted(gate.lines("workers")) == sorted(_worker_of(uid) for uid in KEPT_ROWS), "stopped a kept pipeline's worker"
    assert not any(f"stop_sink rsync.sink-{uid[:8]}-" in c for uid in KEPT_ROWS for c in gate.lines("docker.log"))
    n = len(TEST_ROWS)
    assert f"stopped {n} sink worker(s)" in r.stdout
    assert f"deleted {n} leftover e2e pipeline row(s)" in r.stdout
    assert f"deleted {n * len(_kafka_of(_uuid('x', 0))[0])} topic(s)" in r.stdout
    assert f"deleted {n * len(_kafka_of(_uuid('x', 0))[1])} consumer group(s)" in r.stdout


def test_an_id8_one_char_off_is_another_pipeline(gate):
    # abcdef0f is swept; abcdef01 (kept) must survive although both start abcdef0.
    _world(gate, {NEAR_ROW[0]: NEAR_ROW[1], **KEPT_ROWS})
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    left = set(gate.lines("topics"))
    assert not any("abcdef0f" in t for t in left), r.stdout
    assert set(_kafka_of(_uuid("abcdef01", 19))[0]) <= left


def test_an_id8_a_kept_pipeline_shares_is_not_swept(gate):
    shared = _uuid("bbbb0001", 99)  # same id8 as the kept "orders to warehouse"
    _world(gate, {shared: "golden-types-1759000010", **KEPT_ROWS}, live=KEPT_ROWS)
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    assert not any(ln.startswith(shared) for ln in gate.lines("pipelines")), "the test row itself still goes"
    topics, groups = _kafka_of(_uuid("bbbb0001", 11))
    assert set(topics) <= set(gate.lines("topics")), "swept a kept pipeline's topics"
    kept_groups = [g for g in groups if "cdc-schema-changes-" not in g and "cdc-table-stats-" not in g]
    assert set(kept_groups) <= set(gate.lines("groups")), "swept a kept pipeline's groups"
    assert "cdc-bbbb0001" in gate.lines("connectors")
    assert _worker_of(_uuid("bbbb0001", 11)) in gate.lines("workers"), "stopped the kept pipeline's worker"
    assert "shared with a kept pipeline" in r.stderr


def test_connectors_go_before_their_topics(gate):
    _world(gate, TEST_ROWS)
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    log = gate.lines("docker.log")
    first_topic_delete = next(i for i, c in enumerate(log) if "kafka-topics.sh" in c and "--delete" in c)
    last_connector_delete = max(i for i, c in enumerate(log) if "DELETE connector" in c)
    assert last_connector_delete < first_topic_delete, "a live connector recreates the topics just deleted"


def test_sink_workers_stop_before_their_topics_and_groups(gate):
    _world(gate, TEST_ROWS)
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    log = gate.lines("docker.log")
    stops = [i for i, c in enumerate(log) if c.startswith("sink stop_sink ")]
    assert len(stops) == len(TEST_ROWS), log
    first_delete = next(i for i, c in enumerate(log) if "--delete" in c or "DELETE connector" in c)
    assert max(stops) < first_delete, "a live worker recreates its topic and holds its group"


def test_a_worker_that_will_not_stop_keeps_its_pipeline(gate):
    _world(gate, TEST_ROWS)
    stuck = _uuid("aaaa0001", 1)
    gate.put("stuck_workers", [_worker_of(stuck).split("|")[0]])
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    assert "1 sink worker(s) would not stop" in r.stderr
    assert gate.lines("pipelines") == [f"{stuck}|{TEST_ROWS[stuck]}"], "its row went, so nothing names its topics again"
    topics, groups = _kafka_of(stuck)
    assert sorted(gate.lines("topics")) == sorted(OTHER_TOPICS + topics), r.stdout
    assert sorted(gate.lines("groups")) == sorted(OTHER_GROUPS + groups), r.stdout
    assert "cdc-aaaa0001" in gate.lines("connectors")
    assert f"deleted {len(TEST_ROWS) - 1} leftover e2e pipeline row(s)" in r.stdout


def test_when_no_worker_stops_nothing_is_deleted(gate):
    _world(gate, TEST_ROWS)
    gate.put("stuck_workers", [_worker_of(uid).split("|")[0] for uid in TEST_ROWS])
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    assert len(gate.lines("pipelines")) == len(TEST_ROWS)
    assert not any("--delete" in c or "DELETE connector" in c for c in gate.lines("docker.log"))
    assert not any("DELETE" in s for s in gate.lines("sql.log"))


def test_a_worker_already_gone_counts_as_stopped(gate):
    # "Worker not found" is how stop_sink says it -- cdc_kafka_teardown.go reads it the same way.
    _world(gate, TEST_ROWS)
    gate.put("vanished_workers", [_worker_of(_uuid("aaaa0001", 1)).split("|")[0]])
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    assert "would not stop" not in r.stderr
    assert gate.lines("pipelines") == []


def test_topics_go_in_exact_name_batches(gate):
    many = {_uuid(f"cc{i:06x}", i): f"golden-types-{1759000100 + i}" for i in range(60)}
    _world(gate, many)
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    patterns = gate.lines("topic_deletes")
    assert len(patterns) > 1 and all(p.count("|") < 50 for p in patterns), "a batch holds more than 50 names"
    assert all("\\." in p and not re.search(r"(?<!\\)\.", p.replace(".+", "")) for p in patterns)
    assert sorted(gate.lines("topics")) == sorted(OTHER_TOPICS)


# --- ordering and failure: nothing fails the gate, nothing is orphaned early ---


@pytest.mark.parametrize("down", [f"down_{KAFKA}", f"down_{PG}", f"down_{SINK}", "fail_list_sinks"])
def test_an_unreachable_service_skips_and_keeps_the_rows(gate, down):
    _world(gate, TEST_ROWS)
    gate.flag(down)
    r = gate.run()
    assert "rc=0" in r.stdout
    assert "WARN: pipeline reclaim" in r.stderr and "next gate retries" in r.stderr
    assert "no leftover" not in r.stdout, "an unreachable service reported as clean"
    # Kafka first: with Kafka down the rows stay, or nothing could name their topics again.
    assert len(gate.lines("pipelines")) == len(TEST_ROWS)
    assert not any("DELETE" in s for s in gate.lines("sql.log"))
    assert not any("--delete" in c or "stop_sink" in c for c in gate.lines("docker.log"))


def test_a_failed_row_delete_warns_and_never_fails_the_gate(gate):
    _world(gate, TEST_ROWS)
    gate.flag("fail_pg_delete")
    r = gate.run()
    assert "rc=0" in r.stdout
    assert "WARN: pipeline reclaim" in r.stderr and "next gate retries" in r.stderr
    assert len(gate.lines("pipelines")) == len(TEST_ROWS)


def test_a_failed_topic_delete_warns_and_never_fails_the_gate(gate):
    _world(gate, TEST_ROWS)
    gate.flag("fail_topic_delete")
    r = gate.run()
    assert "rc=0" in r.stdout
    assert "WARN: pipeline reclaim: a topic delete failed" in r.stderr
    assert sum("kafka-topics.sh" in c and "--delete" in c for c in gate.lines("docker.log")) == 1


def test_a_group_with_live_members_is_reported_not_counted(gate):
    _world(gate, TEST_ROWS)
    busy = f"rsync.sink-{_uuid('aaaa0001', 1)[:8]}-batch"
    gate.put("busy_groups", [busy])
    r = gate.run()
    assert "rc=0" in r.stdout
    n = len(TEST_ROWS) * len(_kafka_of(_uuid("x", 0))[1])
    assert f"deleted {n - 1} consumer group(s)" in r.stdout, r.stdout
    assert "1 consumer group(s) still have members" in r.stderr
    assert busy in gate.lines("groups")
    assert len(gate.lines("group_deletes")) == 1, "one delete call for every group, not one each"


def test_nothing_to_reclaim_touches_nothing(gate):
    _world(gate, KEPT_ROWS)
    r = gate.run()
    assert "rc=0" in r.stdout, r.stderr
    assert "no leftover e2e pipelines" in r.stdout
    assert not any("kafka-" in c for c in gate.lines("docker.log"))
    assert len(gate.lines("pipelines")) == len(KEPT_ROWS)


def test_opt_out_touches_nothing(gate):
    _world(gate, TEST_ROWS)
    r = gate.run(E2E_SKIP_PIPELINE_RECLAIM="1")
    assert "rc=0" in r.stdout, r.stderr
    assert gate.lines("docker.log") == []


# --- the gate's own wiring ----------------------------------------------------


def test_the_sink_container_and_port_are_the_ones_compose_runs():
    text = GATE.read_text()
    m = re.search(r'^KAFKA_SINK_CONTAINER="\$\{STACK_PREFIX\}(-[^"]+)"$', text, re.M)
    assert m, "KAFKA_SINK_CONTAINER is no longer <prefix>-<name>"
    main = (REPO / "docker-compose.yml").read_text()
    assert f"container_name: rsync-ai{m.group(1)}\n" in main
    assert f"container_name: ${{STACK_PREFIX:-rsync-ai}}{m.group(1)}\n" in (REPO / "docker-compose.ci-isolate.yml").read_text()
    service = re.search(rf"container_name: rsync-ai{re.escape(m.group(1))}\n(.*?)\n  \S", main, re.S).group(1)
    port = re.search(r'^\s+MCP_PORT: "(\d+)"$', service, re.M)
    assert port and f"http://localhost:{port.group(1)}/" in _extract(text, "stop_sink_workers_of"), (
        "stop_sink_workers_of calls the sink on a port its compose service does not listen on"
    )


def test_gate_reclaims_pipelines_at_start_and_exit():
    text = GATE.read_text()
    trap = text.index("\ntrap on_exit EXIT\n")
    for defn in ("\nreclaim_e2e_pipelines() {", "\ndelete_topics_exactly() {", "\nstop_sink_workers_of() {",
                 "\nE2E_PIPELINE_NAME_RE='"):
        assert text.index(defn) < trap, f"{defn.strip()} must precede the EXIT trap, or an early exit cannot resolve it"
    top = [m.group(1) for m in re.finditer(r"^(reclaim_e2e_\w+)\s*$", text[trap:], re.M)]
    assert "reclaim_e2e_pipelines" in top, "the gate never reclaims pipelines before its tests"
    assert re.search(r"^\s+reclaim_e2e_pipelines\s*$", _extract(text, "on_exit"), re.M), (
        "on_exit never reclaims the pipelines this run created"
    )


# --- census: every pipeline a gated test creates carries a reclaimable name ----

BUCKET = re.compile(r"^(?:BATCH|CDC|CHAOS|OAUTH|GRAPHQL|SMOKE)_TESTS=\(\n(.*?)^\)", re.S | re.M)
CREATES = re.compile(r"/pipelines(?:\?|\")|INTO pipelines\b")
NAME_SITE = re.compile(
    r"/pipelines\?[^\n]*--arg name \"([^\"]+)\""  # sh: jq payload on the api_post line
    r"|INTO pipelines \([^)]*\) VALUES \('[^']*','([^']+)'"  # sh: seeded with SQL
    r"|\"name\":\s*f?\"([^\"]+)\",\s*\"request\""  # py: the create payload
)


def _gated():
    text = GATE.read_text()
    names = {t for m in BUCKET.finditer(text) for t in re.findall(r"^\s*(test_\S+\.(?:sh|py))\b", m.group(1), re.M)}
    assert len(names) >= 20, f"found only {len(names)} gated tests -- the bucket pattern has drifted"
    return sorted(names)


def _resolve(text, value, pos):
    m = re.match(r"\$\{(\w+)\}$", value)
    if not m:
        return value
    before = [a for a in re.finditer(rf"^\s*{m.group(1)}=\"([^\"]+)\"", text, re.M) if a.start() < pos]
    assert before, f"no assignment of {m.group(1)} above its use"
    return before[-1].group(1)


def _name_sites():
    for name in _gated():
        text = (E2E / name).read_text()
        for m in NAME_SITE.finditer(text):
            raw = _resolve(text, next(g for g in m.groups() if g), m.start())
            # every interpolation is the run's timestamp or a uuid's hex slice
            yield f"{name}:{text.count(chr(10), 0, m.start()) + 1}", raw, re.sub(r"\$\{\w+\}|\{[^{}]+\}", "1759000000", raw)


def test_every_pipeline_a_gated_test_creates_is_reclaimable():
    sites = list(_name_sites())
    assert len(sites) >= 10, f"census found only {len(sites)} pipeline-name sites -- the pattern has drifted"
    name_re = _name_re()
    outside = [f"{where} -> {raw}" for where, raw, sample in sites if not name_re.match(sample)]
    assert not outside, (
        "these gated tests create pipelines the gate cannot reclaim, so every run leaks one; "
        "name them <prefix>-<run suffix> and add the prefix to E2E_PIPELINE_NAME_RE:\n  " + "\n  ".join(outside)
    )


def test_every_pipeline_creating_gated_test_is_in_the_census():
    creating = {n for n in _gated() if CREATES.search((E2E / n).read_text())}
    covered = {where.split(":")[0] for where, _, _ in _name_sites()}
    assert len(creating) >= 10, f"only {len(creating)} gated tests create a pipeline -- the detector has drifted"
    assert creating <= covered, f"create a pipeline under a name the census cannot see: {sorted(creating - covered)}"
