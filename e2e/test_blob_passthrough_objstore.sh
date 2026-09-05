#!/usr/bin/env bash
set -uo pipefail

# Universal blob / native-format passthrough — live connector-level E2E (PR #326).
#
# Proves the byte-moving core of the blob lane against REAL object stores, using
# the deployed aws-s3 + azure-blob MCP connector images (the artifacts that run in
# the orchestrator's blob lane). It drives each connector's own export/import_data
# tools in-container (the same code path the orchestrator's executeBlobDataTransfer
# + kafka-mcp-sink writeBlobToDestination invoke), so a green run proves:
#
#   SOURCE  aws-s3.export(copy_raw_files=true)  -> opaque bytes staged byte-identical
#           to the claim-check store (s3://stage/<conn>/<sha256>/<leaf>), one blob
#           envelope {object_key,data_ref,sha256,content_type,size} per object.
#   DEST    <conn>.import_data(blob=true,data_ref,...) -> fetch staged bytes, verify
#           sha256 (fail loud on mismatch), write BYTE-IDENTICAL with content_type
#           carried verbatim.
#
# Coverage:
#   A. aws-s3 -> aws-s3 round-trip: 3 objects (PDF, PNG, ZERO-byte) byte-identical
#      (sha256 parity) + content_type parity.  (zero-byte = the "kept in blob mode"
#      edge — a structured export would drop it.)
#   B. over-cap fails LOUD (max_blob_bytes below an object's size => refused, never a
#      silent/partial copy).
#   C. cross-provider aws-s3 -> azure-blob: the azure dest connector fetches the same
#      staged bytes from the S3/MinIO claim-check store (the staging client is always
#      boto3, regardless of dest provider) and writes them byte-identical to Azurite.
#
# Self-contained: spins up a throwaway MinIO (source+dest+claim-check) and Azurite
# (cross-provider dest) on the MCP network, runs, and tears them down. NOT the
# internal infra MinIO (INV-1 denies that by host) — these are throwaway emulators
# with non-`minio` DNS names, which INV-1 allows.
#
# Usage:  bash e2e/test_blob_passthrough_objstore.sh
# Exit:   0 = all blob-passthrough checks passed · non-zero = a check failed.

MCP_NET="${MCP_NET:-rsync-ai-mcp}"
AWS_MCP="${AWS_MCP:-rsync-ai-aws-s3-v1-0-0-mcp}"
AZ_MCP="${AZ_MCP:-rsync-ai-azure-blob-v1-0-0-mcp}"

# Throwaway MinIO (source + dest + claim-check staging). Name has NO 'minio'
# substring so INV-1 treats it as an external store.
BS="${BS:-blobstore}"
BS_ENDPOINT="http://${BS}:9000"
BS_KEY="minioadmin"; BS_SECRET="minioadmin"; BS_REGION="us-east-1"
SRC_BUCKET="blob-src"; DST_BUCKET="blob-dst"; STAGE_BUCKET="blob-stage"; STAGE_PREFIX="stage"

# Throwaway Azurite (cross-provider dest). Standard well-known devstoreaccount1 key.
AZ="${AZ:-blobazurite}"
AZ_ACCT="devstoreaccount1"
AZ_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
AZ_ENDPOINT="http://${AZ}:10000/${AZ_ACCT}"
AZ_CONN="DefaultEndpointsProtocol=http;AccountName=${AZ_ACCT};AccountKey=${AZ_KEY};BlobEndpoint=${AZ_ENDPOINT};"
AZ_CONTAINER="blob-az"

# Throwaway fake-gcs-server (second cross-provider dest).
GE="${GE:-blobgcs}"
GE_ENDPOINT="http://${GE}:4443"
GE_BUCKET="blob-gcs-dst"

log() { printf '%s\n' "$*"; }
die() { printf '❌ %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
need docker

GCS_MCP="${GCS_MCP:-rsync-ai-gcs-v1-0-0-mcp}"

cleanup() {
  log ""
  log "🧹 Cleanup (best-effort)…"
  docker rm -f "${BS}" "${AZ}" "${GE}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker ps --format '{{.Names}}' | grep -qx "${AWS_MCP}" || die "aws-s3 MCP container '${AWS_MCP}' not running"
docker ps --format '{{.Names}}' | grep -qx "${AZ_MCP}"  || die "azure-blob MCP container '${AZ_MCP}' not running"
docker ps --format '{{.Names}}' | grep -qx "${GCS_MCP}" || die "gcs MCP container '${GCS_MCP}' not running"

log "=================================================="
log "🧪 blob/native-format passthrough — live connector E2E"
log "=================================================="

# ---- emulators ---------------------------------------------------------------
log "▶ starting throwaway MinIO '${BS}' (source+dest+claim-check)…"
docker rm -f "${BS}" >/dev/null 2>&1 || true
docker run -d --name "${BS}" --network "${MCP_NET}" \
  -e MINIO_ROOT_USER="${BS_KEY}" -e MINIO_ROOT_PASSWORD="${BS_SECRET}" \
  minio/minio:latest server /data >/dev/null || die "failed to start MinIO ${BS}"

log "▶ starting throwaway Azurite '${AZ}' (cross-provider dest)…"
docker rm -f "${AZ}" >/dev/null 2>&1 || true
docker run -d --name "${AZ}" --network "${MCP_NET}" \
  -e "AZURITE_ACCOUNTS=${AZ_ACCT}:${AZ_KEY}" \
  mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --skipApiVersionCheck --disableProductStyleUrl >/dev/null \
  || die "failed to start Azurite ${AZ}"

log "▶ starting throwaway fake-gcs-server '${GE}' (cross-provider dest)…"
docker rm -f "${GE}" >/dev/null 2>&1 || true
docker run -d --name "${GE}" --network "${MCP_NET}" \
  fsouza/fake-gcs-server:latest -scheme http -port 4443 -external-url "${GE_ENDPOINT}" >/dev/null \
  || die "failed to start fake-gcs-server ${GE}"

# readiness: from inside the aws-s3 container (it has boto3 + reaches the MCP net)
log "▶ waiting for MinIO…"
ok=""
for _ in $(seq 1 30); do
  if docker exec -e EP="${BS_ENDPOINT}" -e AK="${BS_KEY}" -e SK="${BS_SECRET}" "${AWS_MCP}" python3 -c '
import os,boto3
from botocore.config import Config
c=boto3.client("s3",endpoint_url=os.environ["EP"],aws_access_key_id=os.environ["AK"],
  aws_secret_access_key=os.environ["SK"],region_name="us-east-1",
  config=Config(signature_version="s3v4",s3={"addressing_style":"path"}))
c.list_buckets();print("ok")' >/dev/null 2>&1; then ok=1; break; fi
  sleep 1
done
[ -n "${ok}" ] || die "MinIO ${BS} did not become ready"
log "✅ MinIO ready"

# ============================================================================
#  A + B : aws-s3 source export -> aws-s3 dest import (byte-identical) + over-cap
# ============================================================================
log ""
log "=== A/B: aws-s3 export(copy_raw_files) -> aws-s3 import_data(blob) ==="
OUT="$(docker exec \
  -e EP="${BS_ENDPOINT}" -e AK="${BS_KEY}" -e SK="${BS_SECRET}" -e RG="${BS_REGION}" \
  -e SRC="${SRC_BUCKET}" -e DST="${DST_BUCKET}" -e STG="${STAGE_BUCKET}" -e PFX="${STAGE_PREFIX}" \
  -i "${AWS_MCP}" python3 - <<'PY'
import os, json, hashlib, boto3
from botocore.config import Config
import connector

EP, AK, SK, RG = os.environ["EP"], os.environ["AK"], os.environ["SK"], os.environ["RG"]
SRC, DST, STG, PFX = os.environ["SRC"], os.environ["DST"], os.environ["STG"], os.environ["PFX"]

def s3():
    return boto3.client("s3", endpoint_url=EP, aws_access_key_id=AK, aws_secret_access_key=SK,
                        region_name=RG, config=Config(signature_version="s3v4", s3={"addressing_style":"path"}))

c = s3()
for b in (SRC, DST, STG):
    try: c.create_bucket(Bucket=b)
    except Exception: pass

# Synthetic fixtures: a PDF, a PNG, and a ZERO-byte object (the blob-mode edge case).
fixtures = {
    "docs/report.pdf": b"%PDF-1.4\n" + os.urandom(4096) + b"\n%%EOF\n",
    "img/logo.png":    b"\x89PNG\r\n\x1a\n" + os.urandom(2048),
    "data/empty.bin":  b"",
}
src_sha = {}
for k, v in fixtures.items():
    c.put_object(Bucket=SRC, Key=k, Body=v)
    src_sha[k] = hashlib.sha256(v).hexdigest()

srv = connector.AwsS3MCPServer()
src_cfg = {"bucket": SRC, "endpoint_url": EP, "access_key_id": AK, "secret_access_key": SK, "region": RG}
staging = {"endpoint": EP, "access_key": AK, "secret_key": SK, "region": RG, "bucket": STG, "prefix": PFX}

result = {"src_sha": src_sha, "blobs": [], "A": {}, "B": {}}

# ---- A: export (blob mode) ----
exp = srv.export({"config": src_cfg, "copy_raw_files": True, "staging_config": staging, "limit": 100})
blobs = exp.get("blobs") or []
result["A"]["export_success"] = bool(exp.get("success"))
result["A"]["n_blobs"] = len(blobs)
# envelope sha256 must equal source sha256 for every object (incl. the zero-byte one)
env_ok = (len(blobs) == len(fixtures))
for bl in blobs:
    ok = (bl.get("object_key") in src_sha) and (bl.get("sha256") == src_sha[bl["object_key"]])
    env_ok = env_ok and ok
    result["blobs"].append({"object_key": bl.get("object_key"), "data_ref": bl.get("data_ref"),
                            "sha256": bl.get("sha256"), "content_type": bl.get("content_type"),
                            "size": bl.get("size")})
result["A"]["envelope_sha_match"] = env_ok

# ---- A: import_data(blob) into dest bucket, then read back + compare bytes ----
dst_cfg = {"bucket": DST, "endpoint_url": EP, "access_key_id": AK, "secret_access_key": SK, "region": RG}
roundtrip_ok = (len(blobs) == len(fixtures))
ct_ok = True
for bl in blobs:
    imp = srv.import_data({"config": dst_cfg, "blob": True, "raw": True,
                           "data_ref": bl["data_ref"], "staging_config": staging,
                           "key": bl["object_key"], "content_type": bl.get("content_type"),
                           "sha256": bl.get("sha256"), "size": bl.get("size")})
    if not imp.get("success"):
        roundtrip_ok = False; result["A"].setdefault("import_errors", []).append({bl["object_key"]: imp.get("error")}); continue
    obj = c.get_object(Bucket=DST, Key=bl["object_key"])
    body = obj["Body"].read()
    dst_sha = hashlib.sha256(body).hexdigest()
    if dst_sha != src_sha[bl["object_key"]]:
        roundtrip_ok = False
        result["A"].setdefault("byte_mismatch", []).append({bl["object_key"]: [src_sha[bl["object_key"]], dst_sha]})
    # content_type carried verbatim (source-derived == stored on dest)
    if (obj.get("ContentType") or "") != (bl.get("content_type") or ""):
        ct_ok = False
result["A"]["byte_identical"] = roundtrip_ok
result["A"]["content_type_parity"] = ct_ok

# ---- B: over-cap MUST fail loud (no silent/partial copy) ----
try:
    capped = srv.export({"config": src_cfg, "copy_raw_files": True, "staging_config": staging,
                         "max_blob_bytes": 5, "limit": 100})
    # fail-loud = NOT a clean success that emitted the over-cap object
    over = [b for b in (capped.get("blobs") or []) if (b.get("size") or 0) > 5]
    result["B"]["refused"] = (not capped.get("success")) or (len(over) == 0 and not capped.get("success"))
    result["B"]["raised"] = False
    result["B"]["detail"] = {"success": capped.get("success"), "error": str(capped.get("error"))[:200],
                             "over_cap_emitted": len(over)}
except Exception as e:
    result["B"]["refused"] = True
    result["B"]["raised"] = True
    result["B"]["detail"] = str(e)[:200]

print("BLOB_E2E_RESULT=" + json.dumps(result))
PY
)" || true

echo "${OUT}" | grep -v '^BLOB_E2E_RESULT=' || true
RES="$(printf '%s\n' "${OUT}" | sed -n 's/^BLOB_E2E_RESULT=//p' | tail -1)"
[ -n "${RES}" ] || die "aws-s3 blob run produced no result (container error above)"

# evaluate A + B with python (jq may not be present)
python3 - "$RES" <<'PY' || die "Test A/B FAILED (see detail above)"
import json,sys
r=json.loads(sys.argv[1])
A,B=r["A"],r["B"]
print("----- A: aws-s3 -> aws-s3 -----")
print(f"  export_success      = {A.get('export_success')}")
print(f"  n_blobs             = {A.get('n_blobs')} (expect 3)")
print(f"  envelope_sha_match  = {A.get('envelope_sha_match')}")
print(f"  byte_identical      = {A.get('byte_identical')}")
print(f"  content_type_parity = {A.get('content_type_parity')}")
for k in ("import_errors","byte_mismatch"):
    if A.get(k): print(f"  {k}: {A[k]}")
print("  per-object sha256 (source == dest, byte-identical):")
sha=r["src_sha"]
for bl in r["blobs"]:
    print(f"    {bl['object_key']:<18} {bl['sha256']}  ct={bl['content_type']} size={bl['size']}")
print("----- B: over-cap fail-loud -----")
print(f"  refused={B.get('refused')} raised={B.get('raised')} detail={B.get('detail')}")
ok = (A.get('export_success') and A.get('n_blobs')==3 and A.get('envelope_sha_match')
      and A.get('byte_identical') and A.get('content_type_parity') and B.get('refused'))
sys.exit(0 if ok else 1)
PY
log "✅ A + B PASS (aws-s3 byte-identical round-trip incl. zero-byte; over-cap refused)"

# ============================================================================
#  C : cross-provider aws-s3(staged) -> azure-blob dest (Azurite), byte-identical
# ============================================================================
log ""
log "=== C: cross-provider — azure-blob import_data(blob) from S3 claim-check ==="
BLOBS_JSON="$(python3 -c 'import json,sys; r=json.loads(sys.argv[1]); print(json.dumps({"blobs":r["blobs"],"src_sha":r["src_sha"]}))' "$RES")"

COUT="$(docker exec \
  -e EP="${BS_ENDPOINT}" -e AK="${BS_KEY}" -e SK="${BS_SECRET}" -e RG="${BS_REGION}" \
  -e AZ_CONN="${AZ_CONN}" -e AZ_CONTAINER="${AZ_CONTAINER}" -e BLOBS_JSON="${BLOBS_JSON}" \
  -i "${AZ_MCP}" python3 - <<'PY'
import os, json, hashlib
import connector
from azure.storage.blob import BlobServiceClient

data = json.loads(os.environ["BLOBS_JSON"])
blobs, src_sha = data["blobs"], data["src_sha"]
AZ_CONN, CONT = os.environ["AZ_CONN"], os.environ["AZ_CONTAINER"]
staging = {"endpoint": os.environ["EP"], "access_key": os.environ["AK"],
           "secret_key": os.environ["SK"], "region": os.environ["RG"]}

svc = BlobServiceClient.from_connection_string(AZ_CONN)
try: svc.create_container(CONT)
except Exception: pass

srv = connector.AzureBlobMCPServer()
az_cfg = {"connection_string": AZ_CONN, "container": CONT}
result = {"C": {"n": len(blobs), "byte_identical": True, "content_type_parity": True, "errors": []}}
for bl in blobs:
    imp = srv.import_data({"config": az_cfg, "blob": True, "raw": True,
                           "data_ref": bl["data_ref"], "staging_config": staging,
                           "container": CONT, "key": bl["object_key"],
                           "content_type": bl.get("content_type"), "sha256": bl.get("sha256")})
    if not imp.get("success"):
        result["C"]["byte_identical"] = False
        result["C"]["errors"].append({bl["object_key"]: imp.get("error")}); continue
    bc = svc.get_container_client(CONT).get_blob_client(bl["object_key"])
    body = bc.download_blob().readall()
    if hashlib.sha256(body).hexdigest() != src_sha[bl["object_key"]]:
        result["C"]["byte_identical"] = False
        result["C"]["errors"].append({bl["object_key"]: "sha256 mismatch"})
    props = bc.get_blob_properties()
    if (props.content_settings.content_type or "") != (bl.get("content_type") or ""):
        result["C"]["content_type_parity"] = False

print("BLOB_C_RESULT=" + json.dumps(result))
PY
)" || true

echo "${COUT}" | grep -v '^BLOB_C_RESULT=' || true
CRES="$(printf '%s\n' "${COUT}" | sed -n 's/^BLOB_C_RESULT=//p' | tail -1)"
[ -n "${CRES}" ] || die "azure-blob cross-provider run produced no result (container error above)"
python3 - "$CRES" <<'PY' || die "Test C FAILED (see detail above)"
import json,sys
r=json.loads(sys.argv[1])["C"]
print("----- C: aws-s3(staged) -> azure-blob (Azurite) -----")
print(f"  n={r['n']} byte_identical={r['byte_identical']} content_type_parity={r['content_type_parity']}")
if r["errors"]: print(f"  errors: {r['errors']}")
sys.exit(0 if (r["n"]==3 and r["byte_identical"] and r["content_type_parity"]) else 1)
PY
log "✅ C PASS (cross-provider byte-identical: aws-s3 claim-check -> azure-blob dest)"

# ============================================================================
#  D : cross-provider aws-s3(staged) -> gcs dest (fake-gcs-server), byte-identical
# ============================================================================
log ""
log "=== D: cross-provider — gcs import_data(blob) from S3 claim-check ==="
DOUT="$(docker exec \
  -e EP="${BS_ENDPOINT}" -e AK="${BS_KEY}" -e SK="${BS_SECRET}" -e RG="${BS_REGION}" \
  -e GE_ENDPOINT="${GE_ENDPOINT}" -e GE_BUCKET="${GE_BUCKET}" -e BLOBS_JSON="${BLOBS_JSON}" \
  -i "${GCS_MCP}" python3 - <<'PY'
import os, json, hashlib
import connector
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials

data = json.loads(os.environ["BLOBS_JSON"])
blobs, src_sha = data["blobs"], data["src_sha"]
GE_ENDPOINT, BUCKET = os.environ["GE_ENDPOINT"], os.environ["GE_BUCKET"]
staging = {"endpoint": os.environ["EP"], "access_key": os.environ["AK"],
           "secret_key": os.environ["SK"], "region": os.environ["RG"]}

gc = storage.Client(project="emulator", credentials=AnonymousCredentials(),
                    client_options={"api_endpoint": GE_ENDPOINT})
try: gc.create_bucket(BUCKET)
except Exception: pass

srv = connector.GcsMCPServer()
gcs_cfg = {"bucket": BUCKET, "endpoint_url": GE_ENDPOINT, "project_id": "emulator"}
result = {"D": {"n": len(blobs), "byte_identical": True, "content_type_parity": True, "errors": []}}
for bl in blobs:
    imp = srv.import_data({"config": gcs_cfg, "blob": True, "raw": True,
                           "data_ref": bl["data_ref"], "staging_config": staging,
                           "bucket": BUCKET, "key": bl["object_key"],
                           "content_type": bl.get("content_type"), "sha256": bl.get("sha256")})
    if not imp.get("success"):
        result["D"]["byte_identical"] = False
        result["D"]["errors"].append({bl["object_key"]: imp.get("error")}); continue
    blob = gc.bucket(BUCKET).blob(bl["object_key"])
    body = blob.download_as_bytes()
    if hashlib.sha256(body).hexdigest() != src_sha[bl["object_key"]]:
        result["D"]["byte_identical"] = False
        result["D"]["errors"].append({bl["object_key"]: "sha256 mismatch"})
    blob.reload()
    if (blob.content_type or "") != (bl.get("content_type") or ""):
        result["D"]["content_type_parity"] = False

print("BLOB_D_RESULT=" + json.dumps(result))
PY
)" || true

echo "${DOUT}" | grep -v '^BLOB_D_RESULT=' || true
DRES="$(printf '%s\n' "${DOUT}" | sed -n 's/^BLOB_D_RESULT=//p' | tail -1)"
[ -n "${DRES}" ] || die "gcs cross-provider run produced no result (container error above)"
python3 - "$DRES" <<'PY' || die "Test D FAILED (see detail above)"
import json,sys
r=json.loads(sys.argv[1])["D"]
print("----- D: aws-s3(staged) -> gcs (fake-gcs-server) -----")
print(f"  n={r['n']} byte_identical={r['byte_identical']} content_type_parity={r['content_type_parity']}")
if r["errors"]: print(f"  errors: {r['errors']}")
sys.exit(0 if (r["n"]==3 and r["byte_identical"] and r["content_type_parity"]) else 1)
PY
log "✅ D PASS (cross-provider byte-identical: aws-s3 claim-check -> gcs dest)"

log ""
log "✅✅ ALL blob-passthrough checks PASSED — byte-identical raw-object copy proven for all 3 object stores:"
log "    A: aws-s3 ↔ aws-s3 (PDF + PNG + zero-byte, content_type carried verbatim)"
log "    B: over-cap fails loud (no silent/partial copy)"
log "    C: cross-provider aws-s3 → azure-blob (Azurite)"
log "    D: cross-provider aws-s3 → gcs (fake-gcs-server)"
