#!/usr/bin/env bash
set -euo pipefail

# ─── rsync.ai — One-command Kubernetes installer ─────────────────────────────
#
# Usage (any cluster your current kubectl context points at):
#   curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install-k8s.sh | bash
#
# It needs nothing else from you. Every secret and setting you do not supply is
# generated or defaulted, and the result is a working install:
#
#   1. writes  ~/rsync-ai-k8s/.env  -- generated secrets (chmod 600) plus every
#      optional setting, commented out. Edit it and re-run to change anything.
#   2. renders ~/rsync-ai-k8s/values.generated.yaml from that .env
#   3. runs    helm upgrade --install  (helm is downloaded and checksum-verified
#      if you do not have it) and waits for the release to be ready
#   4. prints how to open the UI; on a laptop it also opens the port-forwards
#
# Re-running is the upgrade path and is safe: secrets already in the .env -- or,
# failing that, already in the cluster -- are reused, never regenerated. That
# matters for ENCRYPTION_KEY (rotating it makes every saved connection
# undecryptable) and POSTGRES_PASSWORD (the database keeps the one it was
# initialised with).
#
# Every setting goes in the .env, or on the `bash` side of the pipe:
#   curl -sSL .../install-k8s.sh | RSYNC_NAMESPACE=data bash
# Written before `curl` it is exported to `curl`, which never reads it.
#
# Flags (after `bash -s --` when piped):
#   --render-only     write the files and render the chart; touch no cluster
#   --port-forward    force the port-forwards at the end (default: only on a tty)
#   --no-port-forward never open them
#   -h, --help
#
# Copyright (c) 2025 Infini Data Solution (Rahul Kumar Vishnoi)
# Licensed under the Elastic License 2.0 — https://rsync.ai/license
# ─────────────────────────────────────────────────────────────────────────────

# The chart. Pinned to the release this script ships with, for the same reason
# install.sh pins RSYNC_REF: the chart's images default to its own appVersion, so
# one number names both halves. RSYNC_CHART may also be a local chart directory
# (a checkout, or `helm pull --untar`), in which case no version is passed.
RSYNC_CHART="${RSYNC_CHART:-oci://ghcr.io/rsync-ai/charts/rsync-ai}"
RSYNC_CHART_VERSION="${RSYNC_CHART_VERSION:-0.1.4}"

# Used only when there is no helm on PATH. OCI charts are GA from 3.8.
HELM_FALLBACK_VERSION="v3.16.4"
HELM_MIN_MINOR=8

INSTALL_DIR="${RSYNC_INSTALL_DIR:-${HOME:-$PWD}/rsync-ai-k8s}"
ENV_FILE=".env"
VALUES_FILE="values.generated.yaml"

# What an install asks the scheduler for, in REQUESTS (not usage). Measured by
# rendering the chart with an empty fleet and no demo, then adding one connector
# / the demo at a time; test_install_k8s_defaults_work.py re-measures on every
# run, so a chart change that moves these fails CI instead of the next user.
# BASE includes the post-install hook pods (kafka-init, minio-mb): they run after
# every Deployment is up, and on a full node they are what stays Pending.
BASE_MEM_MIB=8416
BASE_CPU_M=3410
CONNECTOR_MEM_MIB=96
CONNECTOR_CPU_M=50
DEMO_MEM_MIB=128
DEMO_CPU_M=50
# The in-cluster model: its server and its pull hook.
OLLAMA_MEM_MIB=6272
OLLAMA_CPU_M=1050
# What --wait cannot survive is a pod that can never be scheduled, so the fleet
# and demo the user did NOT choose are trimmed to this when the cluster cannot
# hold the defaults. Enough for the documented PG -> Mongo CDC path.
LEAN_CONNECTORS="postgresql,mongodb"

# The rung below lean. Trimming connectors cannot rescue a 4-vCPU node: BASE
# alone asks 3410m, and a GKE e2-standard-4 has ~3920m allocatable with several
# hundred more taken by kube-system DaemonSets, so the lean set's 3510m still
# does not fit and its pods sit Pending. What does fit is one replica of each
# front-end Deployment (both default to 2) and a smaller CPU request for the
# orchestrator. The chart sets no CPU limit anywhere, so a smaller CPU request
# costs no achievable CPU -- only the guaranteed floor under contention. Memory
# limits are untouched: the orchestrator's 2Gi is what tableConcurrency reads
# itself against. Savings, re-measured every run by
# test_install_k8s_defaults_work.py: api-gateway 2->1 (200m/384Mi), frontend
# 2->1 (100m/256Mi), orchestrator cpu 500m->250m.
TIGHT_ORCHESTRATOR_CPU="250m"
TIGHT_MEM_MIB=640
TIGHT_CPU_M=550
# Set by fit_to_cluster when even the lean set does not fit; read by write_values.
TIGHT=false
# How long to wait for the cluster's OWN pods before measuring what is free.
# Nothing is installed during this wait; see wait_for_system_pods.
SETTLE_TIMEOUT_S="${RSYNC_SETTLE_TIMEOUT_S:-90}"

# Source/destination connectors a pod can be started for, as id:version. The
# version is part of the Service name the orchestrator resolves
# (rsync-ai.connectorServiceName), so it must equal the connector's
# latest.json current_version -- not a number that merely looks right. Kubernetes
# has no just-in-time deploy (that needs a Docker socket), so a connector a
# pipeline names but the fleet does not list is simply absent. Each image is
# mcp-<id>. sample-data is absent on purpose: the chart runs it itself
# (connectors.sampleData). A guard (test_install_k8s_defaults_work.py) compares
# this list with the tree, so a new connector cannot ship without being
# installable here.
KNOWN_CONNECTORS="aws-s3:v1.0.0 azure-blob:v1.0.0 bigquery:v1.0.0 clickhouse:v1.0.0 databricks:v1.0.0 gcs:v1.0.0 github-rest:v1.0.0 google-sheets:v1.0.0 mongodb:v1.0.0 mysql:v1.0.0 notion-rest:v1.0.0 oracle:v1.0.0 petstore:v1.0.3 postgresql:v1.0.0 redshift:v1.0.0 shopify-admin-graphql:v1.0.0 snowflake:v1.0.0 sqlserver:v1.0.0 stripe:v1.0.0 widgets-graphql:v1.0.0"
# Enough for the two documented paths (PG -> Mongo CDC, and object storage) plus
# MySQL, which is the third most asked-for source. ~0.5 GiB of requests.
DEFAULT_CONNECTORS="postgresql,mysql,mongodb,aws-s3,gcs"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
info()    { echo -e "${GREEN}✓${NC} $*"; }
warn()    { echo -e "${YELLOW}⚠${NC}  $*"; }
error()   { echo -e "${RED}✗${NC} $*" >&2; }
section() { echo -e "\n${BOLD}${BLUE}▶ $*${NC}"; }
die()     { error "$*"; exit 1; }

RENDER_ONLY="${RSYNC_RENDER_ONLY:-false}"
PORT_FORWARD="${RSYNC_PORT_FORWARD:-auto}"

# ─── .env access ─────────────────────────────────────────────────────────────
# The .env is read and edited, never sourced: it is a document an operator
# edits by hand, and sourcing it would execute whatever they typed into it.

env_value() {
  local key="$1" file="${INSTALL_DIR}/${ENV_FILE}" line=""
  [[ -f "$file" ]] || return 0
  # `export KEY=value` is accepted too, and read as KEY.
  line=$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$file" | tail -1 || true)
  line="${line#*=}"
  # One matched pair of surrounding quotes, the way compose's env-file parser does.
  line="${line%\"}"; line="${line#\"}"
  line="${line%\'}"; line="${line#\'}"
  printf '%s' "$line"
}

# Replaces the key in place rather than appending a second line that would win by
# being later. awk -v, not sed: a value may hold `&` or `/`, both live in a sed
# replacement and inert in an awk variable.
set_env_value() {
  local key="$1" value="$2" file="${INSTALL_DIR}/${ENV_FILE}" tmp
  tmp="$(mktemp "${file}.XXXXXX")"
  # Before a byte is written: this file carries secrets and the temp copy is about
  # to BECOME it. Same directory, so the mv is atomic.
  chmod 600 "$tmp"
  awk -v k="$key" -v v="$value" '
    $0 ~ "^[[:space:]]*(export[[:space:]]+)?" k "=" {
      if (!seen) { p = ""; if (match($0, /^[[:space:]]*export[[:space:]]+/)) p = "export "; print p k "=" v; seen = 1 }
      next
    }
    { print }
    END { if (!seen) print k "=" v }
  ' "$file" > "$tmp"
  mv "$tmp" "$file"
}

# A setting: the environment wins (so `... | RSYNC_X=y bash` works), then the
# .env, then the default. Empty counts as unset at every level.
cfg() {
  local key="$1" default="${2:-}" v=""
  v="${!key:-}"
  [[ -n "$v" ]] || v="$(env_value "$key")"
  [[ -n "$v" ]] || v="$default"
  printf '%s' "$v"
}

lower() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

# 32 alphanumerics: ENCRYPTION_KEY is truncated to its first 32 bytes by the
# services that read it, and the chart refuses any character that a URL or a
# libpq keyword string would mangle (rsync-ai.assertDeliverableSecret).
# No `head` on the OUTPUT side of a pipe: it closes the pipe early, the producer
# takes SIGPIPE, and pipefail fails the assignment.
generate_secret() {
  local raw=""
  if command -v openssl >/dev/null 2>&1; then
    raw=$(openssl rand -base64 96 | LC_ALL=C tr -dc 'a-zA-Z0-9')
  else
    raw=$(head -c 4096 /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9')
  fi
  if (( ${#raw} < 32 )); then
    die "Could not generate a 32-character secret (got ${#raw}). Install openssl and re-run."
  fi
  printf '%s' "${raw:0:32}"
}

# YAML double-quoted scalar. Backslash first, or the escapes added for the quote
# would themselves be doubled.
yq() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "$s"
}

# ─── Prerequisites ───────────────────────────────────────────────────────────

helm_is_usable() {
  local bin="$1" v
  v="$("$bin" version --template '{{.Version}}' 2>/dev/null)" || return 1
  [[ "$v" =~ ^v([0-9]+)\.([0-9]+)\. ]] || return 1
  # 3.8+ (OCI charts went GA there) or any later major. Helm 4 is what a current
  # `brew install helm` gives you; refusing it would download a second helm
  # next to a perfectly good one.
  (( BASH_REMATCH[1] > 3 || (BASH_REMATCH[1] == 3 && BASH_REMATCH[2] >= HELM_MIN_MINOR) ))
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{print $1}'
  else die "Need sha256sum or shasum to verify the helm download."; fi
}

# Finds helm, or fetches it. Never touches /usr/local: a curl-pipe installer that
# needs sudo is one nobody should pipe. The download is verified against the
# checksum file published next to it before a byte of it is executed.
ensure_helm() {
  if command -v helm >/dev/null 2>&1 && helm_is_usable "$(command -v helm)"; then
    HELM="$(command -v helm)"
    return 0
  fi
  local os arch bin_dir tarball base tmp want got
  bin_dir="${INSTALL_DIR}/bin"
  if [[ -x "${bin_dir}/helm" ]] && helm_is_usable "${bin_dir}/helm"; then
    HELM="${bin_dir}/helm"
    return 0
  fi
  case "$(uname -s)" in
    Linux)  os=linux ;;
    Darwin) os=darwin ;;
    *) die "No usable helm (>= 3.${HELM_MIN_MINOR}) found, and no download exists for $(uname -s). Install helm: https://helm.sh/docs/intro/install/" ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64)  arch=amd64 ;;
    arm64|aarch64) arch=arm64 ;;
    *) die "No usable helm (>= 3.${HELM_MIN_MINOR}) found, and no download exists for $(uname -m). Install helm: https://helm.sh/docs/intro/install/" ;;
  esac
  command -v curl >/dev/null 2>&1 || die "Need curl to download helm (or install helm yourself: https://helm.sh/docs/intro/install/)."
  warn "helm >= 3.${HELM_MIN_MINOR} not found; downloading ${HELM_FALLBACK_VERSION} into ${bin_dir}"
  tarball="helm-${HELM_FALLBACK_VERSION}-${os}-${arch}.tar.gz"
  base="https://get.helm.sh/${tarball}"
  tmp="$(mktemp -d)"
  curl -fsSL "$base" -o "${tmp}/${tarball}" || die "Could not download ${base}"
  curl -fsSL "${base}.sha256sum" -o "${tmp}/sum" || die "Could not download ${base}.sha256sum"
  want="$(awk '{print $1}' "${tmp}/sum")"
  got="$(sha256_of "${tmp}/${tarball}")"
  [[ -n "$want" && "$want" == "$got" ]] || die "Checksum mismatch for ${tarball} (want ${want:-<empty>}, got ${got}). Refusing to run it."
  tar -xzf "${tmp}/${tarball}" -C "$tmp"
  mkdir -p "$bin_dir"
  mv "${tmp}/${os}-${arch}/helm" "${bin_dir}/helm"
  chmod 755 "${bin_dir}/helm"
  rm -rf "$tmp"
  HELM="${bin_dir}/helm"
  info "helm ${HELM_FALLBACK_VERSION} installed (checksum verified)"
}

ensure_kubectl() {
  command -v kubectl >/dev/null 2>&1 || die "kubectl not found. Install it (https://kubernetes.io/docs/tasks/tools/) and point it at your cluster, then re-run."
  KUBECTL=(kubectl)
  local ctx
  ctx="$(cfg RSYNC_KUBE_CONTEXT)"
  if [[ -n "$ctx" ]]; then KUBECTL=(kubectl --context "$ctx"); fi
}

kc() { "${KUBECTL[@]}" "$@"; }

helm_ctx_args() {
  local ctx
  ctx="$(cfg RSYNC_KUBE_CONTEXT)"
  if [[ -n "$ctx" ]]; then printf '%s\n' "--kube-context" "$ctx"; fi
}

# ─── Settings ────────────────────────────────────────────────────────────────

# id -> version, from KNOWN_CONNECTORS. bash 3.2 has no associative arrays.
connector_version() {
  local e
  for e in $KNOWN_CONNECTORS; do
    if [[ "${e%%:*}" == "$1" ]]; then printf '%s' "${e#*:}"; return 0; fi
  done
  return 1
}

known_ids() {
  local e out=""
  for e in $KNOWN_CONNECTORS; do out="${out}${out:+ }${e%%:*}"; done
  printf '%s' "$out"
}

# The connector fleet: validated against the known list, because an id with no
# published image is an ImagePullBackOff that only surfaces after the whole
# --wait timeout. The demo needs a postgresql connector; add it rather than
# fail an install whose only crime was listing fewer connectors.
build_fleet() {
  FLEET=""
  local c seen=" "
  for c in $(printf '%s' "$CONNECTORS" | tr ',' ' '); do
    c="$(printf '%s' "$c" | tr -d '[:space:]')"
    [[ -n "$c" ]] || continue
    connector_version "$c" >/dev/null || die "RSYNC_CONNECTORS lists '${c}', which is not a published connector image. Known: $(known_ids). (Anything else: RSYNC_EXTRA_VALUES.)"
    [[ "$seen" == *" $c "* ]] && continue
    seen="${seen}${c} "
    FLEET="${FLEET}${FLEET:+ }${c}"
  done
  if [[ "$DEMO" == "true" && " $FLEET " != *" postgresql "* ]]; then
    FLEET="postgresql${FLEET:+ }${FLEET}"
    warn "RSYNC_DEMO=true needs the postgresql connector; added it to the fleet."
  fi
}

load_settings() {
  NAMESPACE="$(cfg RSYNC_NAMESPACE rsync)"
  RELEASE="$(cfg RSYNC_RELEASE rsync)"
  STORAGE_CLASS="$(cfg RSYNC_STORAGE_CLASS)"
  IMAGE_REGISTRY="$(cfg RSYNC_IMAGE_REGISTRY)"
  IMAGE_TAG="$(cfg RSYNC_IMAGE_TAG)"
  PULL_SECRET="$(cfg RSYNC_IMAGE_PULL_SECRET)"
  APP_HOST="$(cfg RSYNC_APP_HOST)"
  API_HOST="$(cfg RSYNC_API_HOST)"
  INGRESS_CLASS="$(cfg RSYNC_INGRESS_CLASS)"
  TLS_SECRET="$(cfg RSYNC_TLS_SECRET)"
  CONNECTORS="$(cfg RSYNC_CONNECTORS "$DEFAULT_CONNECTORS")"
  DEMO="$(lower "$(cfg RSYNC_DEMO true)")"
  LLM_PROVIDER="$(lower "$(cfg RSYNC_LLM_PROVIDER openai)")"
  EXTRA_VALUES="$(cfg RSYNC_EXTRA_VALUES)"
  WAIT_TIMEOUT="$(cfg RSYNC_WAIT_TIMEOUT 15m)"
  OPENAI_KEY="$(cfg OPENAI_API_KEY)"

  # Kubernetes names, before they reach a command: a typo here is otherwise an
  # apiserver error three steps later that names neither this key nor the file.
  [[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "RSYNC_NAMESPACE='${NAMESPACE}' is not a valid namespace name (lowercase letters, digits, '-')."
  [[ "$RELEASE"   =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#RELEASE} -le 40 ]] || die "RSYNC_RELEASE='${RELEASE}' is not a valid release name (lowercase letters, digits, '-'; at most 40 characters)."
  [[ "$WAIT_TIMEOUT" =~ ^[0-9]+[smh]$ ]] || die "RSYNC_WAIT_TIMEOUT='${WAIT_TIMEOUT}' must look like 900s, 15m or 1h."
  case "$DEMO" in true|false) ;; *) die "RSYNC_DEMO must be true or false (got '${DEMO}')." ;; esac
  case "$LLM_PROVIDER" in openai|ollama) ;; *) die "RSYNC_LLM_PROVIDER must be openai or ollama (got '${LLM_PROVIDER}'). Any other provider: put it in RSYNC_EXTRA_VALUES." ;; esac
  local h
  for h in "$APP_HOST" "$API_HOST"; do
    [[ -z "$h" || "$h" =~ ^[A-Za-z0-9]([-A-Za-z0-9.]*[A-Za-z0-9])?$ ]] || die "'${h}' is not a valid hostname (RSYNC_APP_HOST / RSYNC_API_HOST)."
  done
  if [[ -n "$APP_HOST" || -n "$API_HOST" ]]; then
    [[ -n "$APP_HOST" && -n "$API_HOST" ]] || die "Set BOTH RSYNC_APP_HOST and RSYNC_API_HOST, or neither. The UI calls the API from the browser, so the API needs a hostname of its own."
  fi
  if [[ -n "$EXTRA_VALUES" && ! -f "$EXTRA_VALUES" ]]; then
    die "RSYNC_EXTRA_VALUES='${EXTRA_VALUES}' is not a file."
  fi

  # Whether the user chose the fleet / demo, or is on the defaults: only the
  # defaults may be trimmed to fit a small cluster (see fit_to_cluster).
  CONNECTORS_EXPLICIT=false; [[ -z "$(cfg RSYNC_CONNECTORS)" ]] || CONNECTORS_EXPLICIT=true
  DEMO_EXPLICIT=false;       [[ -z "$(cfg RSYNC_DEMO)" ]]       || DEMO_EXPLICIT=true
  build_fleet

  # Browser-visible URLs. The UI calls the API FROM THE BROWSER, so these must be
  # addresses the browser can reach -- not Service names. With no ingress the
  # answer is the port-forwards this script opens.
  local scheme=http
  [[ -n "$TLS_SECRET" ]] && scheme=https
  if [[ -n "$APP_HOST" ]]; then
    APP_URL="$(cfg RSYNC_APP_URL "${scheme}://${APP_HOST}")"
    API_URL="$(cfg RSYNC_API_URL "${scheme}://${API_HOST}")"
  else
    APP_URL="$(cfg RSYNC_APP_URL http://localhost:3000)"
    API_URL="$(cfg RSYNC_API_URL http://localhost:8080)"
  fi
}

# ─── Secrets ─────────────────────────────────────────────────────────────────

# .env key -> Secret key. Same names compose's .env uses, except that the chart's
# Secret calls the warehouse password DEMO_WAREHOUSE_PASSWORD.
SECRET_KEYS="JWT_SECRET ENCRYPTION_KEY INTERNAL_SERVICE_SECRET POSTGRES_PASSWORD REDIS_PASSWORD MINIO_ACCESS_KEY MINIO_SECRET_KEY RSYNC_DEMO_WAREHOUSE_PASSWORD"

cluster_secret_key() {
  case "$1" in
    RSYNC_DEMO_WAREHOUSE_PASSWORD) echo DEMO_WAREHOUSE_PASSWORD ;;
    *) echo "$1" ;;
  esac
}

# The Secret an earlier install of this release left behind. It is kept on
# uninstall on purpose (helm.sh/resource-policy: keep), and it is the ground
# truth for the two secrets that cannot be re-rolled: the database was
# initialised with POSTGRES_PASSWORD, and every saved connection was encrypted
# with ENCRYPTION_KEY. A lost .env must not cost either.
import_from_cluster() {
  local secret="${RELEASE}-secrets" key ckey val n=0
  kc -n "$NAMESPACE" get secret "$secret" >/dev/null 2>&1 || return 0
  for key in $SECRET_KEYS; do
    [[ -z "$(env_value "$key")" ]] || continue
    ckey="$(cluster_secret_key "$key")"
    val="$(kc -n "$NAMESPACE" get secret "$secret" -o "go-template={{index .data \"${ckey}\"}}" 2>/dev/null | base64 -d 2>/dev/null || true)"
    if [[ -n "$val" && "$val" != "<no value>" ]]; then
      set_env_value "$key" "$val"
      n=$((n + 1))
    fi
  done
  if (( n > 0 )); then
    info "Reused ${n} secret(s) from the existing ${NAMESPACE}/${secret} -- nothing was regenerated"
  fi
}

write_env_skeleton() {
  local file="${INSTALL_DIR}/${ENV_FILE}"
  ( umask 077; : > "$file" )
  cat >> "$file" <<'EOF'
# rsync.ai Kubernetes install settings.
#
# Everything here is optional. Save this file and re-run install-k8s.sh: the
# secrets below are generated once and reused, and every commented line is a
# setting you can turn on. BACK UP THIS FILE -- ENCRYPTION_KEY encrypts every
# connection credential you save, and there is no way to recover them without it.

# ── Secrets (generated; edit only to bring your own) ─────────────────────────

# ── Access ───────────────────────────────────────────────────────────────────
# Default: no ingress. The UI is at http://localhost:3000 through a kubectl
# port-forward that this script opens for you.
# To publish it instead, give it two hostnames (the UI calls the API from the
# browser, so the API needs its own):
#RSYNC_APP_HOST=app.example.com
#RSYNC_API_HOST=api.example.com
#RSYNC_INGRESS_CLASS=nginx
#RSYNC_TLS_SECRET=rsync-tls      # an existing kubernetes.io/tls Secret; makes the URLs https

# ── LLM (chat is how you create pipelines in the UI) ────────────────────────
#OPENAI_API_KEY=sk-...
# Or run a model inside the cluster instead (needs ~6 GiB and 1 CPU more, and
# downloads ~4.7 GB on first start):
#RSYNC_LLM_PROVIDER=ollama

# ── Cluster ──────────────────────────────────────────────────────────────────
#RSYNC_NAMESPACE=rsync
#RSYNC_RELEASE=rsync
#RSYNC_KUBE_CONTEXT=              # default: your current kubectl context
#RSYNC_STORAGE_CLASS=             # default: the cluster's default StorageClass

# ── What to install ──────────────────────────────────────────────────────────
# Source/destination connectors to run as pods (Kubernetes cannot deploy them
# on demand). Pick from: aws-s3 azure-blob bigquery clickhouse databricks gcs
# github-rest google-sheets mongodb mysql notion-rest oracle petstore postgresql
# redshift shopify-admin-graphql snowflake sqlserver stripe widgets-graphql
#RSYNC_CONNECTORS=postgresql,mysql,mongodb,aws-s3,gcs
#RSYNC_DEMO=true                  # the sample-data try-it path

# ── Images ───────────────────────────────────────────────────────────────────
#RSYNC_IMAGE_REGISTRY=ghcr.io/rsync-ai     # a mirror, or your own registry
#RSYNC_IMAGE_TAG=                          # default: the chart's own version
#RSYNC_IMAGE_PULL_SECRET=                  # an existing docker-registry Secret

# ── Anything else ────────────────────────────────────────────────────────────
# A Helm values file layered on top of everything above -- external Postgres or
# Kafka, Workload Identity, resource limits. See deploy/helm/rsync-ai/values*.yaml.
#RSYNC_EXTRA_VALUES=/path/to/my-values.yaml
EOF
}

ensure_secrets() {
  local file="${INSTALL_DIR}/${ENV_FILE}" key generated=0
  if [[ ! -f "$file" ]]; then
    write_env_skeleton
    info "Created ${file}"
    NEW_ENV=true
  else
    chmod 600 "$file"
    NEW_ENV=false
  fi
  if [[ "$RENDER_ONLY" != "true" ]]; then import_from_cluster; fi
  for key in $SECRET_KEYS; do
    if [[ -z "$(env_value "$key")" ]]; then
      set_env_value "$key" "$(generate_secret)"
      generated=$((generated + 1))
    fi
  done
  if (( generated > 0 )); then
    info "Generated ${generated} secret(s) into ${file} (chmod 600)"
  fi
  # Persist only what they typed: OPENAI_API_KEY belongs to the user, so it is
  # read from wherever it is and never copied into the file.
}

# ─── Cluster capacity ────────────────────────────────────────────────────────

# Kubernetes quantities -> MiB / millicores. Shared by the node and the pod sums.
QTY_AWK='
function mib(q,  u, n) {
  if (q == "") return 0
  n = q + 0; u = ""
  if (match(q, /[A-Za-z]+$/)) u = substr(q, RSTART)
  if (u == "Ki") return n / 1024
  if (u == "Mi") return n
  if (u == "Gi") return n * 1024
  if (u == "Ti") return n * 1048576
  if (u == "k" || u == "K") return n * 1000 / 1048576
  if (u == "M") return n * 1000000 / 1048576
  if (u == "G") return n * 1000000000 / 1048576
  if (u == "T") return n * 1000000000000 / 1048576
  return n / 1048576
}
function milli(q) {
  if (q == "") return 0
  if (q ~ /m$/) { sub(/m$/, "", q); return q + 0 }
  return q * 1000
}'

gib() { awk -v m="$1" 'BEGIN { printf "%.1f", m / 1024 }'; }

# What the install would ask the scheduler for: the chart's fixed set, plus one
# slice per connector pod, the demo warehouse, and (opt-in) the in-cluster model.
estimate_need() {
  local n=0 f
  for f in $FLEET; do n=$((n + 1)); done
  NEED_MEM_MIB=$((BASE_MEM_MIB + n * CONNECTOR_MEM_MIB))
  NEED_CPU_M=$((BASE_CPU_M + n * CONNECTOR_CPU_M))
  if [[ "$DEMO" == "true" ]]; then
    NEED_MEM_MIB=$((NEED_MEM_MIB + DEMO_MEM_MIB)); NEED_CPU_M=$((NEED_CPU_M + DEMO_CPU_M))
  fi
  if [[ "$LLM_PROVIDER" == "ollama" ]]; then
    NEED_MEM_MIB=$((NEED_MEM_MIB + OLLAMA_MEM_MIB)); NEED_CPU_M=$((NEED_CPU_M + OLLAMA_CPU_M))
  fi
}

# What the scheduler can still hand out: the nodes' allocatable minus what is
# already requested by every other pod (kube-system alone is ~300 MiB, and it is
# exactly the gap that let an "8.5 GiB" install onto a node it did not fit).
# This release's own pods are excluded: an upgrade replaces them. Sets
# FREE_MEM_MIB / FREE_CPU_M; returns 1 when the nodes cannot be read.
measure_free() {
  local nodes pods alloc used
  nodes="$(kc get nodes -o 'jsonpath={range .items[*]}{.status.allocatable.memory}{" "}{.status.allocatable.cpu}{"\n"}{end}' 2>/dev/null || true)"
  [[ -n "$nodes" ]] || return 1
  alloc="$(printf '%s\n' "$nodes" | awk "${QTY_AWK}"' NF >= 2 { m += mib($1); c += milli($2) } END { printf "%d %d", m, c }')"
  # One line per pod that still holds a claim on the cluster: phase, namespace,
  # release label, node, then "memory,cpu;" for each container. Tab-separated: a
  # request may be empty. A pod with no node yet is counted, not skipped -- it is
  # owed room, and skipping it is what made a starting cluster read as emptier
  # than it is (see wait_for_system_pods). The error this direction can make is
  # trimming an install that would have fit; the other direction is an install
  # that never becomes ready.
  pods="$(kc get pods -A -o 'jsonpath={range .items[*]}{.status.phase}{"\t"}{.metadata.namespace}{"\t"}{.metadata.labels.app\.kubernetes\.io/instance}{"\t"}{.spec.nodeName}{"\t"}{range .spec.containers[*]}{.resources.requests.memory}{","}{.resources.requests.cpu}{";"}{end}{"\n"}{end}' 2>/dev/null || true)"
  used="0 0"
  if [[ -n "$pods" ]]; then
    used="$(printf '%s\n' "$pods" | awk -F'\t' -v ns="$NAMESPACE" -v rel="$RELEASE" "${QTY_AWK}"'
      $1 == "Succeeded" || $1 == "Failed" { next }
      $2 == ns && $3 == rel { next }
      { n = split($5, cs, ";"); for (i = 1; i <= n; i++) { if (cs[i] == "") continue; split(cs[i], r, ","); m += mib(r[1]); c += milli(r[2]) } }
      END { printf "%d %d", m, c }')"
  fi
  FREE_MEM_MIB=$(( ${alloc%% *} - ${used%% *} ))
  FREE_CPU_M=$(( ${alloc##* } - ${used##* } ))
}

need_fits() { (( FREE_MEM_MIB >= NEED_MEM_MIB && FREE_CPU_M >= NEED_CPU_M )); }

# A cluster reports a node's allocatable the moment the node registers, but the
# system workloads that occupy it -- the CNI, kube-proxy, DNS, and on a managed
# cluster the logging and metrics DaemonSets -- are created and scheduled over
# the following half-minute. Measured inside that window the node looks emptier
# than it is. On a kind node pinned to a GKE e2-standard-4's headroom, free CPU
# read 3700m five seconds after the cluster came up and 3600m at fifteen before
# settling at its true 3400m at twenty-five -- and 3600m is enough to make the
# lean rung's 3510m look like it fits, which is precisely the install that then
# sits Pending for fifteen minutes and fails. Waiting for the reading to stop
# moving would not catch it (3600m held for ten seconds); what has to be waited
# for is the workloads. Returns how many system pods the cluster still owes
# itself; a query that cannot be answered returns 0, because this is a wait and
# not a gate.
system_pods_owed() {
  local ds dep
  ds="$(kc get daemonsets -A -o 'jsonpath={range .items[*]}{.status.desiredNumberScheduled}{" "}{.status.numberReady}{"\n"}{end}' 2>/dev/null || true)"
  dep="$(kc get deployments -n kube-system -o 'jsonpath={range .items[*]}{.spec.replicas}{" "}{.status.readyReplicas}{"\n"}{end}' 2>/dev/null || true)"
  printf '%s\n%s\n' "$ds" "$dep" | awk 'NF { d = $1 + 0; r = $2 + 0; if (d > r) owed += d - r } END { print owed + 0 }'
}

wait_for_system_pods() {
  local waited=0 owed announced=false
  while (( waited < SETTLE_TIMEOUT_S )); do
    owed="$(system_pods_owed)"
    if [[ "$owed" == "0" ]]; then
      if [[ "$announced" == "true" ]]; then info "The cluster's system pods are up (waited ${waited}s)."; fi
      return 0
    fi
    if [[ "$announced" == "false" ]]; then
      info "Waiting for the cluster's own system pods to start before measuring free capacity..."
      announced=true
    fi
    sleep 2
    waited=$((waited + 2))
  done
  warn "The cluster's system pods are still starting after ${SETTLE_TIMEOUT_S}s; measuring capacity anyway."
}

# A pod that cannot be scheduled never becomes ready, and `helm --wait` only says
# so ten minutes later. So compare what the install asks for with what the
# cluster has left BEFORE installing, and when the defaults do not fit, install a
# lean set instead of a broken full one -- but only the parts the user did not
# choose. Nothing is written to the .env: on a bigger cluster the next run is
# the full default again.
fit_to_cluster() {
  estimate_need
  wait_for_system_pods
  if ! measure_free; then
    warn "Could not read the nodes' capacity; skipping the capacity check."
    return 0
  fi
  local have="~$(gib "$FREE_MEM_MIB") GiB / $((FREE_CPU_M / 1000)) CPU free"
  local want="~$(gib "$NEED_MEM_MIB") GiB / $(( (NEED_CPU_M + 999) / 1000 )) CPU"
  if need_fits; then
    info "Capacity: ${have} (this install requests ${want})"
    return 0
  fi

  local was_connectors="$CONNECTORS" was_demo="$DEMO" was_fleet="$FLEET" was_need_mem="$NEED_MEM_MIB" was_need_cpu="$NEED_CPU_M"
  local leaned=false
  [[ "$CONNECTORS_EXPLICIT" == "true" ]] || CONNECTORS="$LEAN_CONNECTORS"
  [[ "$DEMO_EXPLICIT" == "true" ]] || DEMO=false
  if [[ "$CONNECTORS" != "$was_connectors" || "$DEMO" != "$was_demo" ]]; then
    build_fleet
    estimate_need
    leaned=true
    if need_fits; then
      warn "This cluster has ${have}; the default install requests ${want}, so pods would stay Pending."
      echo "  Installing a lean set instead: connectors ${CONNECTORS}, demo ${DEMO} (requests ~$(gib "$NEED_MEM_MIB") GiB)."
      echo "  For the full default, add nodes and re-run this script, or choose your own set in ${INSTALL_DIR}/${ENV_FILE}"
      echo "  (RSYNC_CONNECTORS / RSYNC_DEMO)."
      return 0
    fi
  fi

  # Still short -- which is every 4-vCPU node, because the fleet is not what
  # fills it. Collapse the two front-end Deployments to one replica each and
  # lower the orchestrator's CPU request before giving up: a cluster that
  # reaches this line would otherwise get Pending pods and a failed helm wait,
  # so there is no install here to regress. Not written to the .env -- the next
  # run on a bigger cluster is the full default again.
  TIGHT=true
  NEED_MEM_MIB=$((NEED_MEM_MIB - TIGHT_MEM_MIB)); NEED_CPU_M=$((NEED_CPU_M - TIGHT_CPU_M))
  if need_fits; then
    warn "This cluster has ${have}; the default install requests ${want}, so pods would stay Pending."
    if [[ "$leaned" == "true" ]]; then
      echo "  Installing a lean, single-replica set instead: connectors ${CONNECTORS}, demo ${DEMO},"
    else
      echo "  Installing a single-replica set instead:"
    fi
    echo "  one api-gateway and one frontend pod, and a smaller CPU request for the orchestrator"
    echo "  (requests ~$(gib "$NEED_MEM_MIB") GiB / $(( (NEED_CPU_M + 999) / 1000 )) CPU). The chart sets no CPU limit, so this"
    echo "  costs no speed -- it gives up the spare replica that would carry traffic during a node failure."
    echo "  For the full default, add nodes (or a bigger machine type) and re-run this script."
    return 0
  fi

  TIGHT=false
  CONNECTORS="$was_connectors"; DEMO="$was_demo"; FLEET="$was_fleet"
  NEED_MEM_MIB="$was_need_mem"; NEED_CPU_M="$was_need_cpu"
  warn "This cluster has ${have}; this install requests ${want}."
  echo "  Pods that do not fit will sit Pending and helm will give up after its wait timeout."
  echo "  Add nodes (a cluster autoscaler will do this for you), or trim what is installed in ${INSTALL_DIR}/${ENV_FILE}:"
  echo "    RSYNC_CONNECTORS=${LEAN_CONNECTORS}   RSYNC_DEMO=false"
}

# ─── Cluster preflight ───────────────────────────────────────────────────────

preflight_cluster() {
  section "Checking the cluster"
  local ctx
  ctx="$(kc config current-context 2>/dev/null || true)"
  [[ -n "$ctx" ]] || die "kubectl has no current context. Point it at your cluster (e.g. 'gcloud container clusters get-credentials …', 'aws eks update-kubeconfig …', 'az aks get-credentials …') and re-run."
  kc get --raw /readyz >/dev/null 2>&1 || kc version >/dev/null 2>&1 || die "Cannot reach the cluster behind context '${ctx}'. Check 'kubectl get nodes'."
  info "Cluster reachable (context: ${ctx})"

  # An unbound PVC is the most common way a "successful" install hangs: nothing
  # errors, the database pod just sits Pending until --wait gives up.
  local sc_out default_sc=""
  sc_out="$(kc get storageclass -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}{"\t"}{.metadata.annotations.storageclass\.beta\.kubernetes\.io/is-default-class}{"\n"}{end}' 2>/dev/null || true)"
  if [[ -n "$STORAGE_CLASS" ]]; then
    if printf '%s\n' "$sc_out" | cut -f1 | grep -qx "$STORAGE_CLASS"; then
      info "StorageClass ${STORAGE_CLASS}"
    else
      die "RSYNC_STORAGE_CLASS='${STORAGE_CLASS}' does not exist. Available: $(printf '%s\n' "$sc_out" | cut -f1 | tr '\n' ' ')"
    fi
  else
    default_sc="$(printf '%s\n' "$sc_out" | awk -F'\t' '$2=="true" || $3=="true" {print $1; exit}')"
    if [[ -n "$default_sc" ]]; then
      info "Default StorageClass: ${default_sc}"
    else
      die "The cluster has no default StorageClass, so the database's volume would stay Pending forever.
  Available: $(printf '%s\n' "$sc_out" | cut -f1 | tr '\n' ' ')
  Pick one and re-run:  RSYNC_STORAGE_CLASS=<name> (in ${INSTALL_DIR}/${ENV_FILE})"
    fi
  fi

  fit_to_cluster

  # Old volumes with no Secret to match them: the database was initialised with a
  # password this run cannot know, so a fresh one would lock the app out of it.
  if ! kc -n "$NAMESPACE" get secret "${RELEASE}-secrets" >/dev/null 2>&1; then
    local pvcs
    pvcs="$(kc -n "$NAMESPACE" get pvc -l "app.kubernetes.io/instance=${RELEASE}" -o name 2>/dev/null | wc -l | tr -d ' ')"
    if [[ "${pvcs:-0}" -gt 0 ]]; then
      warn "${NAMESPACE} holds ${pvcs} volume(s) from an earlier '${RELEASE}' install but no ${RELEASE}-secrets."
      echo "  The database keeps the password it was first created with; new generated secrets will not open it."
      echo "  Either restore the old .env, or start clean:  kubectl -n ${NAMESPACE} delete pvc -l app.kubernetes.io/instance=${RELEASE}"
    fi
  fi
}

# ─── values.generated.yaml ───────────────────────────────────────────────────

write_values() {
  local file="${INSTALL_DIR}/${VALUES_FILE}" f
  ( umask 077; : > "$file" )
  {
    echo "# Generated by install-k8s.sh from ${ENV_FILE} on every run -- edit the .env (or"
    echo "# RSYNC_EXTRA_VALUES), not this file. Contains secrets; chmod 600."
    echo "secrets:"
    echo "  jwtSecret: $(yq "$(env_value JWT_SECRET)")"
    echo "  encryptionKey: $(yq "$(env_value ENCRYPTION_KEY)")"
    echo "  internalServiceSecret: $(yq "$(env_value INTERNAL_SERVICE_SECRET)")"
    echo "  postgresPassword: $(yq "$(env_value POSTGRES_PASSWORD)")"
    echo "  redisPassword: $(yq "$(env_value REDIS_PASSWORD)")"
    echo "  minioAccessKey: $(yq "$(env_value MINIO_ACCESS_KEY)")"
    echo "  minioSecretKey: $(yq "$(env_value MINIO_SECRET_KEY)")"
    echo "  demoWarehousePassword: $(yq "$(env_value RSYNC_DEMO_WAREHOUSE_PASSWORD)")"
    [[ -z "$OPENAI_KEY" ]] || echo "  openaiApiKey: $(yq "$OPENAI_KEY")"
    echo "frontend:"
    echo "  apiUrl: $(yq "$API_URL")"
    echo "  publicUrl: $(yq "$APP_URL")"
    # Set only when fit_to_cluster found that not even the lean set fits. These
    # are the chart's defaults minus the headroom a small node cannot seat; the
    # memory limits (and so the orchestrator's tableConcurrency) are untouched.
    # RSYNC_EXTRA_VALUES is layered after this file, so a user's own number wins.
    if [[ "$TIGHT" == "true" ]]; then
      echo "  replicaCount: 1"
      echo "apiGateway:"
      echo "  replicaCount: 1"
      echo "orchestrator:"
      echo "  resources:"
      echo "    requests:"
      echo "      cpu: $(yq "$TIGHT_ORCHESTRATOR_CPU")"
    fi

    if [[ -n "$STORAGE_CLASS" || -n "$IMAGE_REGISTRY" || -n "$IMAGE_TAG" || -n "$PULL_SECRET" ]]; then
      echo "global:"
      [[ -z "$STORAGE_CLASS" ]] || echo "  storageClass: $(yq "$STORAGE_CLASS")"
      if [[ -n "$IMAGE_REGISTRY" || -n "$IMAGE_TAG" || -n "$PULL_SECRET" ]]; then
        echo "  image:"
        [[ -z "$IMAGE_REGISTRY" ]] || echo "    registry: $(yq "$IMAGE_REGISTRY")"
        [[ -z "$IMAGE_TAG" ]] || echo "    tag: $(yq "$IMAGE_TAG")"
        [[ -z "$PULL_SECRET" ]] || echo "    pullSecrets: [{name: $(yq "$PULL_SECRET")}]"
      fi
    fi

    if [[ -n "$APP_HOST" ]]; then
      echo "ingress:"
      echo "  enabled: true"
      [[ -z "$INGRESS_CLASS" ]] || echo "  className: $(yq "$INGRESS_CLASS")"
      echo "  hosts:"
      echo "    app: $(yq "$APP_HOST")"
      echo "    api: $(yq "$API_HOST")"
      if [[ -n "$TLS_SECRET" ]]; then
        echo "  tls:"
        echo "    - secretName: $(yq "$TLS_SECRET")"
        echo "      hosts: [$(yq "$APP_HOST"), $(yq "$API_HOST")]"
      fi
    fi

    echo "connectors:"
    if [[ -n "$FLEET" ]]; then
      echo "  fleet:"
      for f in $FLEET; do
        echo "    - id: ${f}"
        echo "      version: $(connector_version "$f")"
        echo "      image: { repository: mcp-${f}, tag: \"\" }"
      done
    else
      echo "  fleet: []"
    fi

    echo "demo:"
    echo "  enabled: ${DEMO}"

    if [[ "$LLM_PROVIDER" == "ollama" ]]; then
      echo "ollama:"
      echo "  enabled: true"
      echo "generation:"
      echo "  llm:"
      echo "    provider: ollama"
    fi
  } >> "$file"
  VALUES_PATH="$file"
}

# ─── Helm ────────────────────────────────────────────────────────────────────

chart_args() {
  if [[ "$RSYNC_CHART" == oci://* || "$RSYNC_CHART" == http*://* ]]; then
    printf '%s\n' "$RSYNC_CHART" "--version" "$RSYNC_CHART_VERSION"
  else
    printf '%s\n' "$RSYNC_CHART"
  fi
}

value_args() {
  printf '%s\n' "-f" "$VALUES_PATH"
  if [[ -n "$EXTRA_VALUES" ]]; then printf '%s\n' "-f" "$EXTRA_VALUES"; fi
}

render_check() {
  section "Rendering the chart"
  local args=() line
  while IFS= read -r line; do args+=("$line"); done < <(chart_args)
  while IFS= read -r line; do args+=("$line"); done < <(value_args)
  if "$HELM" template "$RELEASE" "${args[@]}" --namespace "$NAMESPACE" >/dev/null; then
    info "The chart renders with these settings"
  else
    die "The chart refused these settings (message above). Fix the .env / RSYNC_EXTRA_VALUES and re-run."
  fi
}

release_status() {
  "$HELM" status "$RELEASE" -n "$NAMESPACE" ${HELM_CTX[@]+"${HELM_CTX[@]}"} 2>/dev/null | sed -n 's/^STATUS: //p' | head -1
}

release_revision() {
  "$HELM" status "$RELEASE" -n "$NAMESPACE" ${HELM_CTX[@]+"${HELM_CTX[@]}"} 2>/dev/null | sed -n 's/^REVISION: //p' | head -1
}

run_helm() {
  section "Installing rsync.ai (${RELEASE} in ${NAMESPACE})"
  local status revision
  status="$(release_status || true)"
  revision="$(release_revision || true)"
  case "$status" in
    pending-install|failed)
      # helm refuses `upgrade` on a release whose FIRST revision never finished
      # ("has no deployed releases", or "another operation in progress" after a
      # Ctrl-C). Nothing of value lives in it, and the Secret and the volumes
      # survive an uninstall, so clearing it is safe and saves the user a step.
      if [[ "$revision" == "1" ]]; then
        warn "The previous first install did not finish (${status}); clearing it before retrying (secrets and volumes are kept)"
        "$HELM" uninstall "$RELEASE" -n "$NAMESPACE" --wait --timeout 5m ${HELM_CTX[@]+"${HELM_CTX[@]}"} >/dev/null
      fi ;;
    pending-upgrade|pending-rollback)
      die "Release ${RELEASE} is stuck in '${status}' (an earlier run was interrupted). Clear it, then re-run:
  helm rollback ${RELEASE} -n ${NAMESPACE} ${HELM_CTX[@]+"${HELM_CTX[*]}"}" ;;
  esac

  local args=() line
  while IFS= read -r line; do args+=("$line"); done < <(chart_args)
  while IFS= read -r line; do args+=("$line"); done < <(value_args)
  echo "  This pulls ~14 images and can take 5-10 minutes on a fresh cluster (timeout ${WAIT_TIMEOUT})."
  if "$HELM" upgrade --install "$RELEASE" "${args[@]}" \
      --namespace "$NAMESPACE" --create-namespace \
      --wait --timeout "$WAIT_TIMEOUT" ${HELM_CTX[@]+"${HELM_CTX[@]}"}; then
    info "Release ${RELEASE} deployed"
  else
    error "helm did not finish cleanly. Pods that are not ready:"
    # READY is "n/m". awk regexes have no backreference, so compare the halves.
    kc -n "$NAMESPACE" get pods --no-headers 2>/dev/null \
      | awk '{ split($2, r, "/") } $3 == "Completed" || $3 == "Succeeded" { next } r[1] != r[2] || $3 != "Running"' \
      | sed 's/^/    /' >&2 || true
    echo "  Look closer:  kubectl -n ${NAMESPACE} describe pod <name>   /   kubectl -n ${NAMESPACE} logs <name>" >&2
    # The scheduler says WHY a pod is Pending; helm only says "timed out".
    local sched
    sched="$(kc -n "$NAMESPACE" get events --field-selector reason=FailedScheduling 2>/dev/null || true)"
    if printf '%s\n' "$sched" | grep -q "Insufficient"; then
      echo "  Cause: the cluster is out of room -- the scheduler reports 'Insufficient memory/cpu' for:" >&2
      printf '%s\n' "$sched" | grep "Insufficient" | sed -n 's/.*pod\/\([^ ]*\) .*/\1/p' | sort -u | sed 's/^/    /' >&2
      echo "  Add nodes, or install less: set RSYNC_CONNECTORS=${LEAN_CONNECTORS} and RSYNC_DEMO=false in ${INSTALL_DIR}/${ENV_FILE}." >&2
    fi
    if kc -n "$NAMESPACE" get events 2>/dev/null | grep -q "no match for platform"; then
      echo "  Cause: an image has no build for this cluster's CPU architecture (\"no match for platform\")." >&2
      echo "  Tags cut before multi-arch publishing are amd64-only. On arm64 nodes (Apple Silicon, Graviton," >&2
      echo "  Axion, Ampere) set RSYNC_IMAGE_TAG to a newer release in ${INSTALL_DIR}/${ENV_FILE} and re-run." >&2
    fi
    # A registry that answers slowly is not a broken install: the kubelet retries a
    # failed pull on its own, and a re-run resumes on the layers it already has.
    if kc -n "$NAMESPACE" get events 2>/dev/null | grep "Failed to pull image" | grep -Eq "timeout|deadline exceeded"; then
      echo "  Cause: the registry was too slow to pull from (\"timeout awaiting response headers\" or similar)." >&2
      echo "  This is the network between the nodes and ghcr.io, not the install. Pulled layers are cached," >&2
      echo "  so just re-run this script; if it keeps happening, mirror the images and set RSYNC_IMAGE_REGISTRY" >&2
      echo "  in ${INSTALL_DIR}/${ENV_FILE}." >&2
    fi
    echo "  Then re-run this script; it resumes where it stopped." >&2
    exit 1
  fi
}

svc_port() {
  # $1 = service suffix, $2 = fallback
  local p
  p="$(kc -n "$NAMESPACE" get svc "${RELEASE}-$1" -o 'jsonpath={.spec.ports[0].port}' 2>/dev/null || true)"
  printf '%s' "${p:-$2}"
}

url_port() {
  # Port of a localhost URL, else nothing.
  local u="$1"
  if [[ "$u" =~ ^http://(localhost|127\.0\.0\.1):([0-9]+)/?$ ]]; then printf '%s' "${BASH_REMATCH[2]}"; fi
}

PF_PIDS=()
stop_port_forwards() {
  # `${arr[@]}` on an empty array is an error under `set -u` in bash < 4.4.
  if [[ ${#PF_PIDS[@]} -gt 0 ]]; then kill "${PF_PIDS[@]}" 2>/dev/null || true; fi
}

finish() {
  section "Done"
  local app_port api_port
  app_port="$(url_port "$APP_URL")"
  api_port="$(url_port "$API_URL")"
  echo -e "  ${BOLD}Open:${NC} ${APP_URL}"
  if [[ -n "$app_port" ]]; then
    echo "  (a laptop-only address: it works while these two port-forwards run)"
    echo "    kubectl -n ${NAMESPACE} port-forward svc/${RELEASE}-frontend ${app_port}:$(svc_port frontend 3000)"
    echo "    kubectl -n ${NAMESPACE} port-forward svc/${RELEASE}-api-gateway ${api_port:-8080}:$(svc_port api-gateway 8080)"
    echo "  To publish it to other people, set RSYNC_APP_HOST / RSYNC_API_HOST in ${INSTALL_DIR}/${ENV_FILE} and re-run."
  fi
  echo
  echo "  Your settings and secrets: ${INSTALL_DIR}/${ENV_FILE}   (back it up)"
  if [[ -z "$OPENAI_KEY" && "$LLM_PROVIDER" == "openai" ]]; then
    warn "No LLM is configured. The UI builds pipelines from chat; without a model it only understands"
    echo "  short requests like \"mongodb to gcs\". For anything free-form add OPENAI_API_KEY"
    echo "  (or RSYNC_LLM_PROVIDER=ollama) to the .env and re-run."
  fi
  echo
  echo "  Pods Running is not a pipeline that moves rows -- run one real pipeline and check the destination."
  echo "  Uninstall:  helm uninstall ${RELEASE} -n ${NAMESPACE}   (keeps your secrets and data volumes)"

  local want_pf=false
  case "$PORT_FORWARD" in
    true) want_pf=true ;;
    false) want_pf=false ;;
    *) [[ -t 1 ]] && want_pf=true ;;
  esac
  if [[ "$want_pf" == "true" && -n "$app_port" ]]; then
    echo
    info "Opening port-forwards (Ctrl-C to stop; re-run the two commands above any time)"
    # Global, not `local`: the EXIT trap runs after finish() has returned, when a local
    # array is already gone -- and under `set -u` that turned a clean run into exit 1.
    PF_PIDS=()
    kc -n "$NAMESPACE" port-forward "svc/${RELEASE}-frontend" "${app_port}:$(svc_port frontend 3000)" >/dev/null &
    PF_PIDS+=($!)
    kc -n "$NAMESPACE" port-forward "svc/${RELEASE}-api-gateway" "${api_port:-8080}:$(svc_port api-gateway 8080)" >/dev/null &
    PF_PIDS+=($!)
    trap 'stop_port_forwards' EXIT INT TERM
    wait "${PF_PIDS[@]}" || true
  fi
}

usage() {
  sed -n '3,33p' "$0" | sed 's/^# \{0,1\}//'
}

main() {
  local arg
  for arg in "$@"; do
    case "$arg" in
      --render-only|--dry-run) RENDER_ONLY=true ;;
      --port-forward) PORT_FORWARD=true ;;
      --no-port-forward) PORT_FORWARD=false ;;
      -h|--help) usage; exit 0 ;;
      *) die "Unknown argument '${arg}'. Try --help." ;;
    esac
  done

  echo -e "${BOLD}rsync.ai — Kubernetes installer${NC}"
  mkdir -p "$INSTALL_DIR"
  chmod 700 "$INSTALL_DIR"

  section "Checking prerequisites"
  ensure_helm
  info "helm: $("$HELM" version --template '{{.Version}}')"
  HELM_CTX=()
  local line
  while IFS= read -r line; do HELM_CTX+=("$line"); done < <(helm_ctx_args)
  if [[ "$RENDER_ONLY" != "true" ]]; then
    ensure_kubectl
    info "kubectl: $(kubectl version --client -o json 2>/dev/null | sed -n 's/.*"gitVersion": *"\([^"]*\)".*/\1/p' | head -1)"
  fi

  load_settings
  if [[ "$RENDER_ONLY" != "true" ]]; then preflight_cluster; fi

  section "Secrets and settings"
  ensure_secrets
  write_values
  info "Wrote ${VALUES_PATH}"

  render_check
  if [[ "$RENDER_ONLY" == "true" ]]; then
    info "Render-only: no cluster was touched."
    exit 0
  fi
  run_helm
  finish
}

main "$@"
