"""A data directory is never the root of the volume it is stored on.

`mkfs.ext4` puts a `lost+found` directory at the root of every filesystem it
creates, so a block-storage PVC arrives at its first pod NON-EMPTY. Three of this
chart's StatefulSets run software that reads its data directory before it writes
to it, and two of the three refuse to start when they find something they did not
put there:

  postgres         initdb refuses a non-empty $PGDATA
  demo-warehouse   same image, same refusal
  kafka            LogManager rejects any entry that is not a topic-partition:

                     Error starting LogManager
                     org.apache.kafka.common.KafkaException: Found directory
                     /var/lib/kafka/data/lost+found, 'lost+found' is not in the
                     form of topic-partition or topic-partition.uniqueId-delete

The two postgres StatefulSets have always pointed $PGDATA one level below the
mount. Kafka did not, and `KAFKA_LOG_DIRS` was the mount root -- so the broker
crash-looped on the first install onto any ext4 PVC, which on GKE is the default
`standard-rwo` class. Nothing downstream survives a dead broker: Kafka Connect,
the CDC plane and the orchestrator's Kafka wait all fail behind it, so the whole
install reads as "the chart is broken" rather than "one mount is wrong".

Three things kept this alive:

  * **Compose cannot reach it.** A bind-mounted host directory has no
    `lost+found`, so every docker-compose install -- dev, CI, the quickstart,
    the one-command self-host -- passes. This is a Kubernetes-only defect, and
    only on a *real* block device: kind's hostPath-backed volumes are clean too.
  * **`helm template` and `helm lint` cannot see it.** The manifest is valid;
    the filesystem it lands on is what makes it wrong.
  * **It is invisible on the second install.** Once a broker has written
    partition directories the operator's attention has long moved on, and the
    failure only ever reproduces on a FRESH volume -- the same shape as the
    RF=1 Connect trap in test_chart_kafka_replication.py.

So the rule has to be checked where it is decided, at render time, for every
component that keeps state on a claim. Static layer first (no helm needed), then
a render layer that asserts on real `helm template` output.

The census is deliberately exhaustive and fails closed: a NEW StatefulSet with a
volumeClaimTemplate must be classified strict or tolerant before this file will
pass. "Nobody thought about it" is exactly how Kafka got here, and an
auto-discovering test that silently ignores unknown components would have had
nothing to say about it.
"""

import pathlib
import re
import shutil
import subprocess

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"
INFRA = CHART / "templates" / "infra"

# The documented install command's flags, plus the two opt-in components that own
# a claim, so the render reaches every subject this file judges. Fakes throughout.
RENDER_FLAGS = [
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "frontend.publicUrl=https://app.example.com",
    "--set", "frontend.apiUrl=https://api.example.com",
    "--set", "demo.enabled=true",
    "--set", "secrets.demoWarehousePassword=FAKEPLACEHOLDER",
    "--set", "connectors.fleet[0].id=postgresql",
    "--set", "connectors.fleet[0].version=v1.0.0",
    "--set", "connectors.fleet[0].image.repository=mcp-postgresql",
]

# Components that READ their data directory before writing it, and refuse a
# directory they did not create. Value = the env var naming that directory.
STRICT = {
    "kafka.yaml": "KAFKA_LOG_DIRS",
    "postgresql.yaml": "PGDATA",
    "demo-warehouse.yaml": "PGDATA",
}

# Components that may keep a data directory at the volume root, each with the
# reason it survives finding `lost+found` there. All three were observed Running
# on GKE `standard-rwo` (ext4 pd-balanced) in the same install where Kafka died.
TOLERANT = {
    "minio.yaml": "a stray top-level directory is not a valid bucket name, and "
                  "MinIO ignores it rather than failing startup",
    "redis.yaml": "redis addresses dump.rdb/appendonly by name and never "
                  "enumerates its dir",
    "ollama.yaml": "the model cache is content-addressed under subdirectories "
                   "the server creates itself",
}

_HELM_COMMENT = re.compile(r"\{\{-?/\*.*?\*/-?\}\}", re.DOTALL)


def _body(path: pathlib.Path) -> str:
    """File text with `{{/* ... */}}` stripped.

    The fix quotes the Kafka error inside a comment so the next reader sees what
    the broken version looked like; a scanner that cannot tell code from
    commentary would read that quote as configuration.
    """
    return _HELM_COMMENT.sub("", path.read_text())


def claim_backed_templates() -> dict:
    """Every infra template that declares a volumeClaimTemplate -> its text."""
    out = {}
    for p in sorted(INFRA.glob("*.yaml")):
        text = _body(p)
        if "volumeClaimTemplates:" in text:
            out[p.name] = text
    return out


# --------------------------------------------------------------------------
# vacuity floor -- every assertion below is worthless if the scan reads nothing,
# and reading nothing passes silently.
# --------------------------------------------------------------------------


def test_the_scan_finds_the_stateful_components():
    found = set(claim_backed_templates())
    assert "kafka.yaml" in found, (
        f"kafka.yaml declares no volumeClaimTemplate; scanned {INFRA} and found "
        f"{sorted(found)}. Either the chart moved or this test is reading the "
        "wrong directory -- it cannot judge a mount it cannot see."
    )
    assert len(found) >= 5, (
        f"only {len(found)} claim-backed templates found ({sorted(found)}); "
        "refusing to report a pass on a denominator this small."
    )


# --------------------------------------------------------------------------
# census -- an unclassified component fails, because "nobody thought about it"
# is how this defect shipped.
# --------------------------------------------------------------------------


def test_every_stateful_component_is_classified():
    unclassified = sorted(set(claim_backed_templates()) - set(STRICT) - set(TOLERANT))
    assert not unclassified, (
        f"{unclassified} keep state on a PVC but are not classified in this file. "
        "A block-storage PVC arrives holding lost+found. Decide which it is: add "
        "it to STRICT (and put its data dir below the mount root) if it validates "
        "its data directory at startup, or to TOLERANT with the reason it does "
        "not care."
    )


def test_the_classification_names_real_templates():
    """A rule about a deleted file protects nothing."""
    present = set(claim_backed_templates())
    missing = sorted((set(STRICT) | set(TOLERANT)) - present)
    assert not missing, (
        f"{missing} are classified here but declare no volumeClaimTemplate any "
        "more. Drop them, or this file is guarding fiction."
    )


# --------------------------------------------------------------------------
# static layer -- runs without helm.
# --------------------------------------------------------------------------


_MOUNT_PATH = re.compile(r"^\s*mountPath:\s*(\S+)\s*$", re.MULTILINE)
_SUB_PATH = re.compile(r"^\s*subPath:\s*(\S+)\s*$", re.MULTILINE)


def _data_dir(text: str, env_var: str) -> str:
    m = re.search(
        rf"-\s*name:\s*{env_var}\s*\n\s*value:\s*[\"']?([^\"'\s]+)[\"']?", text
    )
    assert m, f"{env_var} not found -- cannot judge a data dir this file does not set"
    return m.group(1)


@pytest.mark.parametrize("name,env_var", sorted(STRICT.items()))
def test_the_data_dir_is_below_the_mount_root(name, env_var):
    text = claim_backed_templates()[name]

    mounts = _MOUNT_PATH.findall(text)
    assert len(mounts) == 1, (
        f"{name} has {len(mounts)} mountPaths {mounts}; this check assumes the "
        "one claim-backed mount and must be taught the difference before it can "
        "judge which one holds the data dir."
    )
    mount = mounts[0].rstrip("/")
    data_dir = _data_dir(text, env_var)
    sub_paths = _SUB_PATH.findall(text)

    # Either route puts the data one level down: subPath mounts a subdirectory of
    # the volume (so lost+found stays at the volume root, outside the mount), or
    # the env var points below the mount (so lost+found stays outside the data
    # dir). What must never hold is BOTH the data dir == the mount AND no subPath.
    deeper = data_dir.rstrip("/") != mount and data_dir.startswith(mount + "/")
    assert deeper or sub_paths, (
        f"{name}: {env_var}={data_dir} is the root of the PVC mounted at {mount}, "
        "with no subPath. Every ext4 PVC arrives with lost+found at its root, so "
        "this pod crash-loops on the first install onto real block storage (GKE "
        "standard-rwo, EBS gp3, Azure managed-csi) while every docker-compose "
        "install stays green. Mount a subdirectory (subPath:) or point "
        f"{env_var} one level down."
    )


# --------------------------------------------------------------------------
# render layer -- asserts on real `helm template` output. Skips without helm,
# and a skip is not a pass; CI installs helm (see
# test_ci_guarantees_the_helm_the_chart_guards_need.py). The gate is a skipif
# decorator on each case, not an `if` inside the helper below, and it is
# written out in full on each case rather than aliased to a shared mark: that
# guard's census reads decorator *source text*, so both a skip buried in a
# helper and a `@NEEDS_HELM` alias enrol this file with zero cases and leave
# its workflow-coverage check unable to see that this suite needs helm.
# --------------------------------------------------------------------------

def _render():
    proc = subprocess.run(
        ["helm", "template", "r", str(CHART), *RENDER_FLAGS],
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(REPO),
    )
    assert proc.returncode == 0, f"helm template failed:\n{proc.stderr[-3000:]}"
    docs = [d for d in yaml.safe_load_all(proc.stdout) if d]
    assert len(docs) >= 20, (
        f"only {len(docs)} documents rendered -- the render is not exercising "
        "the chart."
    )
    return docs


def _claim_mounts(docs):
    """(statefulset, container, mountPath, subPath, env) per claim-backed mount."""
    out = []
    for d in docs:
        if d.get("kind") != "StatefulSet":
            continue
        claims = {v["metadata"]["name"] for v in d["spec"].get("volumeClaimTemplates", [])}
        for c in d["spec"]["template"]["spec"]["containers"]:
            env = {e["name"]: e.get("value") for e in c.get("env", [])}
            for m in c.get("volumeMounts", []):
                if m["name"] in claims:
                    out.append(
                        (d["metadata"]["name"], c["name"], m["mountPath"],
                         m.get("subPath"), env)
                    )
    return out


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_render_produces_claim_backed_mounts():
    mounts = _claim_mounts(_render())
    assert len(mounts) >= 5, (
        f"only {len(mounts)} claim-backed mounts in the render {mounts}; the "
        "flags are not enabling the stateful components this file judges."
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_the_rendered_kafka_log_dir_is_not_the_volume_root():
    """The specific regression, named, on real output.

    Kept separate from the parametrized static check because this is the one that
    actually shipped, and a failure here should say Kafka, not "some component".
    """
    mounts = [m for m in _claim_mounts(_render()) if m[1] == "kafka"]
    assert len(mounts) == 1, f"expected one claim-backed kafka mount, got {mounts}"
    _sts, _c, mount_path, sub_path, env = mounts[0]

    log_dirs = env.get("KAFKA_LOG_DIRS")
    assert log_dirs, "the rendered kafka container sets no KAFKA_LOG_DIRS"

    root = log_dirs.rstrip("/") == mount_path.rstrip("/")
    assert not (root and not sub_path), (
        f"KAFKA_LOG_DIRS={log_dirs} is the root of the volume mounted at "
        f"{mount_path} and the mount has no subPath. The broker will not start:\n"
        "  org.apache.kafka.common.KafkaException: Found directory "
        f"{log_dirs}/lost+found, 'lost+found' is not in the form of "
        "topic-partition or topic-partition.uniqueId-delete\n"
        "This is invisible to compose and to kind, and takes Kafka Connect, the "
        "CDC plane and the orchestrator down with it on a real cluster."
    )
