"""_get_staging_client must support workload identity and honor the endpoint.

Two defects lived in the same 39 lines, and both broke the same story: install
rsync on a managed Kubernetes cluster against managed object storage.

1. Absent credentials were treated as a misconfiguration. On EKS/GKE/AKS the
   chart emits no MINIO_ACCESS_KEY_ID / MINIO_SECRET_ACCESS_KEY at all --
   _helpers.tpl:659 gates that pair on objectStorage.external.accessKeyId, and
   values.yaml:375-378 documents leaving both empty as *the* way to use IRSA /
   Workload Identity / AAD Pod Identity. The orchestrator honors that by leaving
   both empty in the staging config it sends (blob_lane.go buildStagingConfig
   defaults them only against the bundled MinIO endpoint), and there is a Go test
   asserting it does. Then this function answered None, and _stage_blob raised
   "blob passthrough requires a claim-check staging client"
   (object_storage_source.py:509-514). The supported cloud path failed before
   staging a single object -- and it failed with a message naming missing
   credentials, which is the one thing the operator had correctly not set.

2. The non-MinIO branch dropped endpoint_url. Invisible against real AWS, where
   boto3 rebuilds the same regional endpoint from region_name. Wrong everywhere
   else: objectStorage.mode=gcs|azure renders
   MINIO_ENDPOINT_URL=https://storage.googleapis.com (or the Azure S3 gateway),
   neither of which matches the is_minio substring test, so the client silently
   addressed AWS S3 instead of the provider the operator configured.

boto3 is not installed in this environment, which is why the fake below is not a
convenience: recording the kwargs is the only way to assert what the client was
actually built with, and "it returned something" is not the claim under test.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import subprocess
import sys
import types

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.abspath(os.path.join(_HERE, "..", ".."))
_BC_PATH = os.path.join(_HERE, "..", "base_connector.py")
_spec = importlib.util.spec_from_file_location("base_connector_staging", _BC_PATH)
_bc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bc)

MINIO_ENDPOINT = "http://rsync-ai-minio:9000"
S3_ENDPOINT = "https://s3.eu-west-1.amazonaws.com"
GCS_ENDPOINT = "https://storage.googleapis.com"


class _FakeBotoConfig:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _Recorder:
    """Stands in for boto3, capturing the client kwargs verbatim."""

    def __init__(self):
        self.calls = []

    def client(self, service, **kwargs):
        self.calls.append((service, kwargs))
        return object()


class _StubConnector:
    """Minimal `self` for the unbound method: it only needs .log."""

    def __init__(self):
        self.logs = []

    def log(self, message, level="info"):
        self.logs.append((level, message))


@pytest.fixture
def boto(monkeypatch):
    rec = _Recorder()
    botocore = types.ModuleType("botocore")
    botocore_config = types.ModuleType("botocore.config")
    botocore_config.Config = _FakeBotoConfig
    botocore.config = botocore_config
    monkeypatch.setitem(sys.modules, "boto3", rec)
    monkeypatch.setitem(sys.modules, "botocore", botocore)
    monkeypatch.setitem(sys.modules, "botocore.config", botocore_config)
    return rec


def build(boto, **config):
    """Call the real _get_staging_client and return (result, kwargs, stub)."""
    stub = _StubConnector()
    result = _bc.BaseMCPConnector._get_staging_client(stub, config)
    kwargs = boto.calls[0][1] if boto.calls else None
    return result, kwargs, stub


# --- 1. workload identity: both credentials absent ---------------------------

@pytest.mark.parametrize("endpoint", [S3_ENDPOINT, GCS_ENDPOINT])
def test_absent_credentials_build_a_client_on_the_default_chain(boto, endpoint):
    """The EKS/GKE/AKS path. Returning None here is what broke it."""
    result, kwargs, stub = build(boto, endpoint=endpoint, region="eu-west-1")

    assert result is not None, (
        "both credentials absent means workload identity, not a misconfiguration -- "
        "the chart emits no credential env at all on that path (_helpers.tpl:659). "
        "Returning None makes _stage_blob raise and every blob run on a managed "
        "cluster dies before staging its first object."
    )
    assert len(boto.calls) == 1, f"expected exactly one boto3.client call, got {boto.calls!r}"
    assert "aws_access_key_id" not in kwargs and "aws_secret_access_key" not in kwargs, (
        "empty-string credentials must not be passed to boto3 at all: a present-but-empty "
        f"key silences the default credential chain instead of deferring to it. got {kwargs!r}"
    )


def test_the_default_chain_path_says_so_in_the_log(boto):
    """Silent is wrong here -- the operator needs to see which path was taken."""
    _, _, stub = build(boto, endpoint=S3_ENDPOINT, region="eu-west-1")
    assert any(
        level != "error" and "default credential chain" in msg for level, msg in stub.logs
    ), f"no non-error log naming the default credential chain: {stub.logs!r}"


# --- 2. half-configured credentials are still an error -----------------------

@pytest.mark.parametrize(
    "config",
    [
        {"access_key": "fakeuser", "secret_key": ""},
        {"access_key": "", "secret_key": "fakesecret"},
    ],
    ids=["only-access-key", "only-secret-key"],
)
def test_exactly_one_credential_is_rejected(boto, config):
    """A half-written secret must fail loudly, not fall through to the chain."""
    result, _, stub = build(boto, endpoint=S3_ENDPOINT, **config)

    assert result is None, (
        "one credential set and the other not is a half-written secret. Falling "
        "through to the default chain hides it until it resurfaces as a confusing "
        "permission denial at put_object time."
    )
    assert not boto.calls, f"no client should have been built, got {boto.calls!r}"
    assert any(level == "error" for level, _ in stub.logs), (
        f"the rejection must be logged at error level: {stub.logs!r}"
    )


# --- 3. static credentials still work (the compose / bundled-MinIO path) ------

def test_both_credentials_present_are_passed_through(boto):
    result, kwargs, _ = build(
        boto, endpoint=MINIO_ENDPOINT, access_key="fakeuser", secret_key="fakesecret"
    )
    assert result is not None
    assert kwargs["aws_access_key_id"] == "fakeuser"
    assert kwargs["aws_secret_access_key"] == "fakesecret"


def test_minio_keeps_path_addressing(boto):
    """MinIO needs path-style; virtual-host style against it 404s per bucket."""
    _, kwargs, _ = build(
        boto, endpoint=MINIO_ENDPOINT, access_key="fakeuser", secret_key="fakesecret"
    )
    assert kwargs["endpoint_url"] == MINIO_ENDPOINT
    assert kwargs["config"].kwargs.get("s3") == {"addressing_style": "path"}


# --- 4. the configured endpoint is honored regardless of provider ------------

@pytest.mark.parametrize(
    "endpoint", [GCS_ENDPOINT, S3_ENDPOINT, "https://fake.blob.core.windows.net"]
)
def test_a_non_minio_endpoint_is_still_passed_to_boto3(boto, endpoint):
    """Dropping it sent gcs/azure installs to AWS S3 with no error."""
    _, kwargs, _ = build(boto, endpoint=endpoint, access_key="fakeuser", secret_key="fakesecret")
    assert kwargs.get("endpoint_url") == endpoint, (
        f"objectStorage.mode=gcs|azure renders MINIO_ENDPOINT_URL={endpoint!r}, which does "
        "not match the is_minio substring test. Dropping endpoint_url there does not fail -- "
        "boto3 quietly addresses AWS S3 instead of the configured provider. is_minio decides "
        f"addressing style, not whether to honor the endpoint. got {kwargs!r}"
    )


def test_a_non_minio_endpoint_does_not_get_path_addressing(boto):
    """The is_minio branch must still mean something -- this is the control."""
    _, kwargs, _ = build(
        boto, endpoint=S3_ENDPOINT, access_key="fakeuser", secret_key="fakesecret"
    )
    assert kwargs["config"].kwargs.get("s3") is None


# --- 5. every vendored copy carries the same fix -----------------------------

def _tracked_base_connectors():
    out = subprocess.run(
        ["git", "ls-files", "-z", "*base_connector.py"],
        cwd=_REPO, capture_output=True, text=True, check=True,
    ).stdout
    return [p for p in out.split("\0") if p]


def test_every_vendored_base_connector_has_the_same_fix():
    """Connectors run from versions/<v>/, not from the canonical root.

    A fix that lands only on shared/mcp-connectors/base_connector.py ships in
    nothing: server_manager.go resolves the versioned snapshot dir, and each of
    those carries its own frozen copy. This is the drift class that let the
    pipedrive and aws-s3 fixes sit on a stranded file.
    """
    files = _tracked_base_connectors()
    assert len(files) >= 20, (
        f"expected the full vendored set, found {len(files)} -- a shrunken denominator "
        "would let this test pass while checking almost nothing"
    )

    missing_wi, forked_build, stale_gate = [], [], []
    for rel in files:
        src = open(os.path.join(_REPO, rel)).read()
        body = _staging_client_source(src, rel)
        if "bool(access_key) != bool(secret_key)" not in body:
            missing_wi.append(rel)
        # Deliberately structural rather than a match on a local variable name,
        # so a rename inside the function does not read as drift. The old code
        # built the client at TWO return sites -- the second of which dropped
        # endpoint_url -- and the fix collapses them into one kwargs dict.
        if body.count("boto3.client(") != 1:
            forked_build.append(f"{rel} ({body.count('boto3.client(')} construction sites)")
        if "if not access_key or not secret_key:" in body:
            stale_gate.append(rel)

    assert not missing_wi, f"workload-identity fix missing from: {missing_wi}"
    assert not forked_build, (
        "the two-branch client construction is back, which is how endpoint_url got "
        f"dropped on the non-MinIO path: {forked_build}"
    )
    assert not stale_gate, f"the rejecting gate is still present in: {stale_gate}"


def _staging_client_source(src, rel):
    """The _get_staging_client body, located by AST rather than by string search."""
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.FunctionDef) and node.name == "_get_staging_client":
            return ast.get_source_segment(src, node) or ""
    raise AssertionError(
        f"{rel} has no _get_staging_client -- this guard is pinned to that function "
        "and must not silently pass on an empty string"
    )
