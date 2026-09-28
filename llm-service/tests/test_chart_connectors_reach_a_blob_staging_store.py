"""Blob passthrough on Helm must stage through a store connectors may reach.

The blob lane hands the SOURCE connector a staging store to put bytes in and the
DESTINATION connector the same store to fetch them from (`buildStagingConfig`,
backend-orchestrator/internal/agents/executor/blob_lane.go). With no
BLOB_STAGING_ENDPOINT_URL the orchestrator falls back to MINIO_ENDPOINT_URL: the
release's own MinIO, which holds every pipeline's staged claim-check rows.

With `networkPolicy.enabled=true`, `allow-intra-release` shuts every pod
labelled `rsync.ai/connector-id` out of the platform datastores -- that MinIO
included, on purpose (KI-CHART-NO-CONNECTOR-NETWORK-BOUNDARY). So both halves of
a blob-passthrough pipeline were denied the store they were told to use
(KI-CHART-NETWORKPOLICY-BLOCKS-CONNECTOR-STAGING). Compose fixed the same class
with a second, blob-only MinIO on the connector network; this pins the chart's
equivalent:

  1. the orchestrator names a staging store that is a rendered Service and is
     NOT the platform MinIO;
  2. a NetworkPolicy admits connector pods to that store, on its S3 port only;
  3. no policy admits connector pods to the platform MinIO (the fix must not
     be "open the platform MinIO");
  4. the store has its own login, and install-k8s.sh generates it;
  5. off `objectStorage.mode=minio` nothing extra renders -- the external
     store is outside the cluster and egress is not restricted.

This proves the rendered manifests, not a live install on a policy-enforcing CNI.
"""

import pathlib
import re
import shutil
import subprocess
from urllib.parse import urlparse

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "deploy" / "helm" / "rsync-ai"
INSTALL_K8S = REPO / "install-k8s.sh"

RENDER_FLAGS = [
    "--set", "secrets.jwtSecret=FAKEPLACEHOLDER",
    "--set", "secrets.encryptionKey=FAKEPLACEHOLDER",
    "--set", "secrets.postgresPassword=FAKEPLACEHOLDER",
    "--set", "secrets.minioAccessKey=FAKEPLACEHOLDER",
    "--set", "secrets.minioSecretKey=FAKEPLACEHOLDER",
    "--set", "frontend.publicUrl=https://app.example.com",
    "--set", "frontend.apiUrl=https://api.example.com",
]
CONNECTOR_LABEL = "rsync.ai/connector-id"

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")


def _render(*extra):
    proc = subprocess.run(
        ["helm", "template", "r", str(CHART), *RENDER_FLAGS, *extra],
        capture_output=True, text=True, timeout=180, cwd=str(REPO),
    )
    assert proc.returncode == 0, f"helm template failed:\n{proc.stderr[-3000:]}"
    docs = [d for d in yaml.safe_load_all(proc.stdout) if d]
    assert len(docs) >= 20, f"only {len(docs)} documents rendered"
    return docs


def _orchestrator_env(docs):
    for d in docs:
        if d.get("kind") == "Deployment":
            for c in d["spec"]["template"]["spec"]["containers"]:
                if c["name"] == "orchestrator":
                    return {e["name"]: e for e in c.get("env", [])}
    pytest.fail("no orchestrator container rendered")


def _service(docs, host):
    for d in docs:
        if d.get("kind") == "Service" and d["metadata"]["name"] == host:
            return d
    return None


def _policies(docs):
    return [d for d in docs if d.get("kind") == "NetworkPolicy"]


def _selects(policy, labels):
    """Does this policy's podSelector select a pod carrying `labels`?"""
    sel = policy["spec"].get("podSelector") or {}
    for k, v in (sel.get("matchLabels") or {}).items():
        if labels.get(k) != v:
            return False
    for expr in sel.get("matchExpressions") or []:
        key, op = expr["key"], expr["operator"]
        if op == "Exists" and key not in labels:
            return False
        if op == "DoesNotExist" and key in labels:
            return False
        if op == "In" and labels.get(key) not in expr["values"]:
            return False
        if op == "NotIn" and labels.get(key) in expr["values"]:
            return False
    return True


def _admits_connectors(policy):
    """The ports this policy opens to a source pod carrying the connector label."""
    ports = []
    for rule in policy["spec"].get("ingress") or []:
        for peer in rule.get("from") or []:
            ps = peer.get("podSelector") or {}
            if any(e["key"] == CONNECTOR_LABEL and e["operator"] == "Exists"
                   for e in ps.get("matchExpressions") or []):
                ports += [p["port"] for p in rule.get("ports") or [{"port": "ALL"}]]
    return ports


def _pod_labels(docs, component):
    for d in docs:
        if d.get("kind") in ("StatefulSet", "Deployment"):
            labels = d["spec"]["template"]["metadata"]["labels"]
            if labels.get("app.kubernetes.io/component") == component:
                return labels
    pytest.fail(f"no {component} workload rendered")


def test_the_orchestrator_stages_blobs_in_a_store_that_is_not_the_platform_minio():
    docs = _render("--set", "networkPolicy.enabled=true")
    env = _orchestrator_env(docs)

    assert "BLOB_STAGING_ENDPOINT_URL" in env, (
        "the orchestrator gets no BLOB_STAGING_ENDPOINT_URL, so buildStagingConfig "
        "hands connectors the platform MinIO -- which allow-intra-release denies them"
    )
    staging = urlparse(env["BLOB_STAGING_ENDPOINT_URL"]["value"])
    platform = urlparse(env["MINIO_ENDPOINT_URL"]["value"])
    assert staging.hostname != platform.hostname, "the staging store IS the platform MinIO"
    # base_connector.py _get_staging_client picks path-style addressing only
    # for an endpoint with "minio", "localhost" or ":9000" in it.
    assert staging.port == 9000, env["BLOB_STAGING_ENDPOINT_URL"]["value"]
    assert _service(docs, staging.hostname), f"{staging.hostname} is not a rendered Service"

    for key in ("BLOB_STAGING_ACCESS_KEY_ID", "BLOB_STAGING_SECRET_ACCESS_KEY"):
        ref = env[key]["valueFrom"]["secretKeyRef"]
        assert ref["key"].startswith("BLOB_STAGING_"), (
            f"{key} reads {ref['key']}: connectors would be handed the platform MinIO's login"
        )


def test_connector_pods_are_admitted_to_the_staging_store_and_only_on_its_s3_port():
    docs = _render("--set", "networkPolicy.enabled=true")
    store = _pod_labels(docs, "blob-staging")
    admitting = [p for p in _policies(docs) if _selects(p, store) and _admits_connectors(p)]
    assert admitting, "no NetworkPolicy admits connector pods to the blob-staging store"
    for p in admitting:
        assert _admits_connectors(p) == [9000], (p["metadata"]["name"], _admits_connectors(p))


def test_no_policy_admits_connector_pods_to_the_platform_minio():
    docs = _render("--set", "networkPolicy.enabled=true")
    platform = _pod_labels(docs, "minio")
    # Non-zero control: the platform MinIO IS the target of some policy, so an
    # empty result below is not an empty policy list.
    assert any(_selects(p, platform) for p in _policies(docs))
    opened = [p["metadata"]["name"] for p in _policies(docs)
              if _selects(p, platform) and _admits_connectors(p)]
    assert not opened, f"{opened} admit connector pods to the platform MinIO"


def test_nothing_extra_renders_off_the_bundled_minio():
    docs = _render(
        "--set", "objectStorage.mode=s3",
        "--set", "objectStorage.external.endpointUrl=https://s3.eu-west-1.amazonaws.com",
        "--set", "networkPolicy.enabled=true",
    )
    assert "BLOB_STAGING_ENDPOINT_URL" not in _orchestrator_env(docs)
    kinds = [(d["kind"], d["metadata"]["name"]) for d in docs]
    assert not [k for k in kinds if "blob-staging" in k[1]], kinds


def test_the_staging_store_has_its_own_login():
    docs = _render("--set", "secrets.blobStagingAccessKey=STAGINGKEY",
                   "--set", "secrets.blobStagingSecretKey=STAGINGSECRET")
    secret = next(d for d in docs if d.get("kind") == "Secret" and "stringData" in d)
    assert secret["stringData"]["BLOB_STAGING_ACCESS_KEY"] == "STAGINGKEY"
    assert secret["stringData"]["BLOB_STAGING_SECRET_KEY"] == "STAGINGSECRET"
    assert secret["stringData"]["MINIO_ACCESS_KEY"] == "FAKEPLACEHOLDER"


def test_install_k8s_generates_the_staging_login():
    text = INSTALL_K8S.read_text(encoding="utf-8")
    keys = re.search(r'^SECRET_KEYS="([^"]+)"', text, re.M)
    assert keys, "install-k8s.sh no longer defines SECRET_KEYS"
    for k in ("BLOB_STAGING_ACCESS_KEY", "BLOB_STAGING_SECRET_KEY"):
        assert k in keys.group(1).split(), f"install-k8s.sh does not generate {k}"
        assert re.search(r"blobStaging(Access|Secret)Key: .*%s" % k, text), (
            f"install-k8s.sh generates {k} but never writes it into the chart values"
        )
