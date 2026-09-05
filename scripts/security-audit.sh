#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Free, self-hosted security audit for rsync-ai (multi-language monorepo).
#
# Runs SAST + dependency-CVE + secret + container/IaC scanners across the whole
# stack (Python / Go / TypeScript / Docker). Every tool is 100% free and OSS.
# All non-local tools run via their official Docker images, so nothing except
# gitleaks is installed on the host. Read-only: no source files are modified.
#
# Usage:
#   scripts/security-audit.sh              # run everything
#   scripts/security-audit.sh secrets sast # run only chosen sections
#
# Sections: secrets  deps  sast  container  go  python
# Results (JSON/SARIF/text) land in ./security-audit-results/
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${REPO}/security-audit-results"
mkdir -p "$OUT"
cd "$REPO"

SECTIONS="${*:-secrets deps sast container go python}"
run() { [[ " $SECTIONS " == *" $1 "* ]]; }
hr()  { printf '\n\033[1;36m══ %s ══\033[0m\n' "$1"; }
have(){ command -v "$1" >/dev/null 2>&1; }

docker info >/dev/null 2>&1 || { echo "Docker not running — start Docker Desktop first."; exit 1; }

# ── 1. SECRETS ───────────────────────────────────────────────────────────────
# gitleaks: git-tracked files only (avoids node_modules/.next false positives).
if run secrets; then
  hr "SECRETS — gitleaks (git history + tracked tree)"
  if have gitleaks; then
    gitleaks git . --config .gitleaks.toml --redact --no-banner \
      --report-format json --report-path "$OUT/gitleaks.json" || true
    echo "→ $OUT/gitleaks.json"
  else
    docker run --rm -v "$REPO:/repo" -w /repo zricethezav/gitleaks:latest \
      git . --config .gitleaks.toml --redact --no-banner \
      --report-format json --report-path /repo/security-audit-results/gitleaks.json || true
  fi
fi

# ── 2. DEPENDENCIES (all ecosystems, one pass) ───────────────────────────────
# Trivy fs: CVEs for Python/Go/npm deps + embedded secrets + license issues.
if run deps; then
  hr "DEPENDENCIES + SECRETS — Trivy filesystem scan"
  docker run --rm -v "$REPO:/src" aquasec/trivy:latest fs /src \
    --scanners vuln,secret --severity HIGH,CRITICAL --format json \
    --output /src/security-audit-results/trivy-fs.json \
    --skip-dirs frontend/node_modules,frontend/.next || true
  echo "→ $OUT/trivy-fs.json"

  hr "DEPENDENCIES — npm audit (frontend)"
  ( cd frontend && npm audit --json > "$OUT/npm-audit.json" 2>/dev/null || true )
  echo "→ $OUT/npm-audit.json"

  hr "DEPENDENCIES — OSV-Scanner (cross-ecosystem, Google OSV DB)"
  docker run --rm -v "$REPO:/src" ghcr.io/google/osv-scanner:latest \
    scan --recursive --format json /src > "$OUT/osv.json" 2>/dev/null || true
  echo "→ $OUT/osv.json"
fi

# ── 3. SAST (multi-language) ─────────────────────────────────────────────────
# Semgrep: py + go + ts/tsx with OWASP Top-10, security-audit & secrets rules.
if run sast; then
  hr "SAST — Semgrep (OWASP Top-10 + security-audit + secrets)"
  docker run --rm -v "$REPO:/src" -w /src semgrep/semgrep:latest semgrep \
    --config p/security-audit --config p/secrets --config p/owasp-top-ten \
    --sarif --output /src/security-audit-results/semgrep.sarif \
    --exclude frontend/node_modules --exclude frontend/.next --exclude '*_test.go' \
    --metrics=off . || true
  echo "→ $OUT/semgrep.sarif"
fi

# ── 4. CONTAINERS / IaC ──────────────────────────────────────────────────────
if run container; then
  hr "CONTAINERS/IaC — Trivy config (Dockerfile + compose misconfig)"
  docker run --rm -v "$REPO:/src" aquasec/trivy:latest config /src \
    --severity HIGH,CRITICAL --format json \
    --output /src/security-audit-results/trivy-config.json || true
  echo "→ $OUT/trivy-config.json"

  hr "DOCKERFILES — hadolint (best-practice + security lint)"
  : > "$OUT/hadolint.txt"
  while IFS= read -r df; do
    echo "### $df" >> "$OUT/hadolint.txt"
    docker run --rm -i hadolint/hadolint < "$df" >> "$OUT/hadolint.txt" 2>&1 || true
  done < <(find . -name 'Dockerfile*' -not -path '*/node_modules/*')
  echo "→ $OUT/hadolint.txt"
fi

# ── 5. GO-SPECIFIC ───────────────────────────────────────────────────────────
if run go; then
  hr "GO SAST — gosec (all 3 modules)"
  docker run --rm -v "$REPO:/src" -w /src securego/gosec:latest \
    -fmt=json -out=/src/security-audit-results/gosec.json -exclude-dir=frontend ./... || true
  echo "→ $OUT/gosec.json"

  if have go; then
    hr "GO DEPS — govulncheck (reachability-aware, official Go DB)"
    for m in api-gateway backend-orchestrator backend-temporal-adapter; do
      ( cd "$m" && go run golang.org/x/vuln/cmd/govulncheck@latest -json ./... ) \
        > "$OUT/govulncheck-$m.json" 2>/dev/null || true
      echo "→ $OUT/govulncheck-$m.json"
    done
  fi
fi

# ── 6. PYTHON-SPECIFIC ───────────────────────────────────────────────────────
if run python; then
  hr "PYTHON SAST — Bandit (llm-service)"
  docker run --rm -v "$REPO:/src" -w /src ghcr.io/pycqa/bandit/bandit:latest \
    -r llm-service -f json -o /src/security-audit-results/bandit.json \
    -x '*/test*,*/versions/*/test*' || true
  echo "→ $OUT/bandit.json"

  hr "PYTHON DEPS — pip-audit (llm-service requirements)"
  docker run --rm -v "$REPO:/src" -w /src python:3.12-slim sh -c \
    "pip install -q pip-audit && pip-audit -r llm-service/requirements.txt -f json" \
    > "$OUT/pip-audit.json" 2>/dev/null || true
  echo "→ $OUT/pip-audit.json"
fi

hr "DONE — results in $OUT"
ls -la "$OUT"
