#!/usr/bin/env bash
set -euo pipefail

# ─── rsync.ai — One-command installer ────────────────────────────────────────
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install.sh | bash
#
# Every RSYNC_* setting below goes on the `bash` side of the pipe:
#   curl -sSL .../install.sh | RSYNC_PROFILES= bash
# Written before `curl` it is exported to `curl`, which never reads it, and this
# script runs with the default -- no error, just a setting silently ignored.
#
# Copyright (c) 2025 Infini Data Solution (Rahul Kumar Vishnoi)
# Licensed under the Elastic License 2.0 — https://rsync.ai/license
# ─────────────────────────────────────────────────────────────────────────────

# The repository the installer pulls its compose files from. Overridable so a
# fork, a mirror or a pre-release branch can be installed without editing this
# script -- and so the slug lives in exactly one place when it changes.
RSYNC_REPO="${RSYNC_REPO:-rsync-ai/rsync}"
# Defaults to the newest release tag, not `main`. `main` is a moving target on the
# compose half and a "last publish" pointer on the image half, so the two halves
# advance at different rates and a curl-pipe install is not reproducible. A tag
# takes both halves from the same commit. Pass RSYNC_REF=main to track the branch.
RSYNC_REF="${RSYNC_REF:-v0.1.2}"
# The image tag that pairs with RSYNC_REF. Both halves of an install have to name
# the same code: the compose file is fetched from RSYNC_REF, and the images that
# compose file starts are pulled at this tag. Left independent they drift, and did
# -- the compose tracked a moving `main` while the images resolved to `latest`,
# which docker-publish.yml mints only on a tag (`github.ref_type == 'tag'`) and so
# still pointed at v0.1.1. That handed the installer a compose file wiring
# settings the pulled images had never heard of (PUBLIC_URL, RSYNC_COOKIE_SECURE,
# RSYNC_DEMO_DESTINATION_DSN), and three services -- connector-deployer,
# llm-service-oss, connector-lifecycle -- that v0.1.1 never built at all. There
# are zero `build:` directives in the quickstart, so those three are a hard
# failure of `docker compose pull`, not a slow local build.
#
# Cutting a tag also fixes it, but only until the next commit touches the compose
# file: one side tracks a branch and the other a tag, so the gap reopens on its
# own. Deriving one from the other closes it structurally instead.
#
# The mapping is the release workflow's, not this script's: docker/metadata-action
# emits `type=semver,pattern={{version}}` for a tag (v0.1.1 -> 0.1.1) and
# `type=ref,event=branch` for a branch (main -> main, slashes to dashes). An
# explicit RSYNC_VERSION still wins, so deliberately pairing one ref's compose
# with another ref's images stays available to anyone who needs it.
if [[ "${RSYNC_REF}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]; then
  RSYNC_VERSION="${RSYNC_VERSION:-${RSYNC_REF#v}}"
else
  RSYNC_VERSION="${RSYNC_VERSION:-${RSYNC_REF//\//-}}"
fi
RAW_BASE="https://raw.githubusercontent.com/${RSYNC_REPO}/${RSYNC_REF}"
COMPOSE_FILE="docker-compose.quickstart.yml"
COMPOSE_URL="${RAW_BASE}/${COMPOSE_FILE}"
# The bring-your-own overlays. Downloaded unconditionally and layered only when
# the .env points somewhere external, so "set the host and re-run" is the whole
# procedure. Before this they were reachable only by cloning the repo -- the one
# thing the curl-pipe install deliberately avoids.
BYO_PG_FILE="docker-compose.byo-postgres.yml"
BYO_KAFKA_FILE="docker-compose.byo-kafka.yml"
# The bundled-LLM overlay. Same deal as the two above -- downloaded
# unconditionally, layered only when the .env asks for it -- except that it ADDS
# services rather than parking any: an Ollama server, and a run-once job that
# pulls the model into it before anything that would ask for one starts.
OLLAMA_FILE="docker-compose.ollama.yml"
# The one file this script does not take from RSYNC_REF, and the reason is
# specific rather than convenient. At v0.1.2 this overlay is a stub that starts
# an empty Ollama and leaves `docker exec rsync-ollama ollama pull` to the
# operator -- the manual step the bundle exists to remove -- so taking it from
# the pinned ref would ship the very defect the pin is meant to protect against.
# The other way to fix that is to make v0.1.2 mean two different things.
#
# It is safe for this file and would not be for the quickstart, because of what
# this file contains: ollama/ollama images only -- no ghcr.io/rsync-ai image and
# no ${RSYNC_VERSION} -- so it has no half that can drift against the images
# RSYNC_VERSION pulls. Everything else in it is `environment` and `depends_on`
# on three services the pinned quickstart already defines, and
# llm-service/tests/test_the_internal_llm_needs_no_manual_step.py fails the
# build if either of those stops holding. The residual is worth stating rather
# than hiding: an overlay that one day names a service the PINNED quickstart
# lacks would break a pinned install, and the answer to that is a new tag, not
# a second exception.
OLLAMA_REF="${RSYNC_OLLAMA_REF:-main}"
OLLAMA_RAW_BASE="https://raw.githubusercontent.com/${RSYNC_REPO}/${OLLAMA_REF}"
COMPOSE_ARGS=()
# Set by build_compose_args when the overlay above goes on, and read by
# start_stack, which behaves differently on that path: the first `up` blocks for
# the length of a multi-gigabyte download.
OLLAMA_BUNDLED=0
# Which optional compose profiles this install activates. `cdc` is in the
# default because the change-data-capture services are not an add-on: pick a
# streaming sync in the UI without them and the orchestrator's pre-flight polls
# three absent containers for two minutes and then fails the run with
# "kafka-connect is not reachable" -- a message about a container that was never
# started, on a stack whose install reported success. The profile existed in
# docker-compose.quickstart.yml and nothing in this script ever activated it, so
# every install shipped that failure.
#
# `-` and not `:-` on purpose: RSYNC_PROFILES= (explicitly empty) is the
# opt-out, and it has to be distinguishable from unset.
RSYNC_PROFILES="${RSYNC_PROFILES-cdc}"
ENV_FILE=".env"
# HOME is absent from exactly the environments setup_tty() exists to serve. A GCE
# or cloud-init startup script, a systemd unit, a `docker run` without -e HOME and
# most CI steps all run with it unset, and `set -u` (line 2) makes reading it there
# a fatal error -- here, at the top level, before the banner, before check_docker,
# and long before the no-terminal branch in prompt_env() that documents this exact
# caller. So the one install path that cannot answer a prompt also could not start,
# and the failure is `install.sh: line 104: HOME: unbound variable` with nothing
# else printed. Every clean-room verification of this script ran over an
# interactive ssh session, where HOME is always set, so none of them could see it.
#
# The passwd database knows this uid's home whether or not the environment does.
# `2>/dev/null` because getent is a glibc tool and does not exist on macOS, where
# HOME is set anyway; PWD is the last resort, for a uid with no passwd entry at
# all (`docker run -u 1234`), and bash always sets it.
#
# The `|| true` is the same trap this file already documents at generate_secret:
# under `set -o pipefail` a missing getent fails the whole pipeline, and under
# `set -e` a failing command substitution takes the assignment -- and the script
# -- down with it. Without it the fix merely moves the silent death two lines
# later on any host that has no getent, which is every Mac.
_home="${HOME:-}"
[[ -n "$_home" ]] || _home="$(getent passwd "$(id -u)" 2>/dev/null | cut -d: -f6 || true)"
INSTALL_DIR="${RSYNC_INSTALL_DIR:-${_home:-$PWD}/rsync-ai}"
# The floor tracks what the install actually starts. 6 sized the 18 unprofiled
# services this file has always started. The cdc profile adds three more
# containers -- one of them a JVM -- whose mem_limit lines in
# docker-compose.quickstart.yml come to 2816MB on top of that, so the default
# set needs 8.
#
# Two floors and not one because RSYNC_PROFILES= starts exactly the 18 services
# 6 was sizing. Warning that operator about a JVM they excluded would be the
# same defect this change fixes: a message about a container that was never
# started.
MIN_RAM_GB_BATCH=6
MIN_RAM_GB_CDC=8
MIN_RAM_GB=$MIN_RAM_GB_BATCH
# Padded with spaces so the match is on a whole word: `nocdc` must not select
# the cdc floor. Commas become spaces first -- RSYNC_PROFILES takes either
# separator, and build_compose_args splits it the same way.
case " ${RSYNC_PROFILES//,/ } " in
  *" cdc "*) MIN_RAM_GB=$MIN_RAM_GB_CDC ;;
esac
# What the bundled LLM needs, and not a third opinion on the two floors above: a
# 7B model at 4-bit is ~5GB resident ON TOP of them, and it stays resident for
# as long as the container runs. Checked separately, in build_compose_args,
# because nothing knows whether the LLM is bundled until the .env exists.
MIN_RAM_GB_WITH_LLM=12
# Written by check_ram so that later check can reuse the reading. 0 means "could
# not read this platform", never "no RAM" -- see check_ram's three outcomes.
DETECTED_RAM_GB=0

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${GREEN}✓${NC} $*"; }
warn()    { echo -e "${YELLOW}⚠${NC}  $*"; }
error()   { echo -e "${RED}✗${NC} $*" >&2; }
section() { echo -e "\n${BOLD}${BLUE}▶ $*${NC}"; }

# ─── Prompting ───────────────────────────────────────────────────────
#
# The usage line at the top of this file is `curl ... | bash`, which hands bash
# the SCRIPT on stdin. A bare `read` then reads from that same pipe -- so it
# consumes this file's own next line and assigns it to the variable, and
# execution silently resumes one line further down. It does not error. Under the
# old code the first prompt ate a line of source, the API-key prompt "accepted" a
# fragment of shell as an OpenAI key, and the install proceeded from there.
#
# So every prompt goes through fd 3, opened on the controlling terminal. When
# there is no terminal at all (cloud-init, CI, a Dockerfile RUN) prompting is
# impossible rather than broken, and the answers must come from the environment.
TTY_OK=0
setup_tty() {
  if [[ -t 0 ]]; then
    exec 3<&0
    TTY_OK=1
  # The braces are load-bearing. In `exec 3</dev/tty 2>/dev/null` the shell
  # applies redirections left to right, so the failing open on fd 3 is reported
  # BEFORE 2 has been pointed at /dev/null -- the probe prints
  # "/dev/tty: Device not configured" to the real stderr every time it runs
  # somewhere without a terminal, which is exactly when it is expected to fail.
  # Redirecting the group instead puts 2>/dev/null in place first.
  elif { exec 3</dev/tty; } 2>/dev/null; then
    TTY_OK=1
  fi
}

# ask VAR "prompt" [default] -- reads from the terminal, falls back to $default.
# The prompt goes to stdout, which is still the terminal even when stdin is a pipe.
ask() {
  local __var=$1 __prompt=$2 __default=${3:-} __reply=""
  if (( TTY_OK )); then
    printf '%s' "$__prompt"
    IFS= read -r __reply <&3 || __reply=""
  fi
  printf -v "$__var" '%s' "${__reply:-$__default}"
}

banner() {
cat << 'EOF'

  ██████╗ ███████╗██╗   ██╗███╗   ██╗ ██████╗     █████╗ ██╗
  ██╔══██╗██╔════╝╚██╗ ██╔╝████╗  ██║██╔════╝    ██╔══██╗██║
  ██████╔╝███████╗ ╚████╔╝ ██╔██╗ ██║██║         ███████║██║
  ██╔══██╗╚════██║  ╚██╔╝  ██║╚██╗██║██║         ██╔══██║██║
  ██║  ██║███████║   ██║   ██║ ╚████║╚██████╗    ██║  ██║██║
  ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═══╝ ╚═════╝    ╚═╝  ╚═╝╚═╝

  Agentic Data Pipelines — Move data without engineers
  https://rsync.ai  |  © 2025 Infini Data Solution

EOF
}

# ─── Pre-flight checks ───────────────────────────────────────────────────────

check_docker() {
  if ! command -v docker &>/dev/null; then
    error "Docker is not installed."
    echo "  Install it from: https://docs.docker.com/get-docker/"
    exit 1
  fi
  # `docker info` fails three ways that need three different fixes: the daemon is
  # not running, it is running but this user is not in the `docker` group, or the
  # active context points somewhere unreachable. The old message asserted the
  # first, so a permissions failure sent the operator to restart a daemon that was
  # already up. Print what the daemon actually said and name both remedies.
  local docker_err=""
  if ! docker_err=$(docker info 2>&1 >/dev/null); then
    error "Cannot talk to the Docker daemon. Docker said:"
    printf '    %s\n' "$(printf '%s' "$docker_err" | head -3)" >&2
    echo "  If Docker is not running, start Docker Desktop (or: sudo systemctl start docker)." >&2
    echo "  If it IS running, this user may not be able to reach it:" >&2
    echo "    sudo usermod -aG docker \"\$USER\"   # then log out and back in" >&2
    exit 1
  fi
  if ! docker compose version &>/dev/null; then
    error "Docker Compose v2 is required."
    echo "  Install it from: https://docs.docker.com/compose/install/"
    exit 1
  fi
  # Not `grep -oP`: PCRE is a GNU extension, absent from the BSD grep on macOS,
  # and on a GNU box the pattern also matches the digits inside the build hash
  # ("build e180ab8" -> "180"). Ask the daemon; fall back to parsing the CLI.
  local dv=""
  dv=$(docker version --format '{{.Server.Version}}' 2>/dev/null) || dv=""
  [[ -n "$dv" ]] || dv=$(docker --version | sed -n 's/^Docker version \([^,]*\).*/\1/p')
  info "Docker ${dv:-(version unknown)} detected"
}

check_arch() {
  # Every ghcr.io/rsync-ai/* image is built by .github/workflows/docker-publish.yml
  # on `runs-on: ubuntu-latest`, and NEITHER docker/build-push-action step sets a
  # `platforms:` key -- so buildx tags each image for the runner's own platform and
  # nothing else. All 14 images the quickstart compose pulls are linux/amd64, with
  # no arm64 entry in the manifest index.
  #
  # Without this check the operator pays the entire prompt sequence, the .env
  # write and the compose download before finding out, and what they get is
  # `no matching manifest for linux/arm64/v8 in the manifest list entries` from
  # deep inside `docker compose pull` -- a message that names neither the cause
  # nor anything to do about it.
  #
  # Ask the DAEMON, not `uname -m`. The daemon resolves the manifest, it can be
  # remote, and on Docker Desktop for Apple Silicon a host `uname -m` of arm64
  # still runs amd64 images under emulation. `{{.Server.Arch}}` is the canonical
  # Go spelling (`amd64`/`arm64`); `docker info --format '{{.Architecture}}'` is
  # the uname spelling (`x86_64`/`aarch64`) and would need two cases apiece.
  local darch=""
  darch=$(docker version --format '{{.Server.Arch}}' 2>/dev/null) || darch=""

  if [[ "$darch" == "amd64" ]]; then
    info "linux/amd64 daemon — matches the published images"
    return 0
  fi

  # Three outcomes, not two -- the same rule check_ram follows. A pre-flight
  # check that cannot read the machine must say so; it must not certify it, and
  # it must not condemn it either.
  if [[ -z "$darch" ]]; then
    warn "Could not read the Docker daemon's architecture."
    warn "rsync.ai publishes linux/amd64 images only — if this host is arm64, the pull will fail."
    return 0
  fi

  # arm64 and anything else. Deliberately NOT a hard exit: Docker Desktop on
  # Apple Silicon runs amd64 images under emulation -- slower, but correct, and a
  # legitimate way to evaluate the product. On a Linux arm64 host (Oracle
  # A1.Flex, AWS Graviton, Azure Ampere, a Raspberry Pi) the qemu-x86_64 binfmt
  # handler is usually not registered, and every container instead dies with
  # `exec format error` after the pull appears to succeed. Exiting here would
  # break the first case to protect the second; naming both lets the operator
  # decide, which is the only thing this script can honestly do.
  warn "This Docker daemon is linux/${darch}; rsync.ai publishes linux/amd64 images only."
  echo "  On Docker Desktop (Apple Silicon) these images run under emulation — correct, but slow."
  echo "  On a Linux ${darch} host without qemu-x86_64 binfmt registered, containers will instead"
  echo "  fail with 'exec format error' after the pull succeeds. Register it with:"
  echo "    docker run --privileged --rm tonistiigi/binfmt --install amd64"
  echo "  To run natively instead, build from source: https://github.com/rsync-ai/rsync"
  local go_on=""
  ask go_on "  Continue anyway? [y/N] " "n"
  case "$go_on" in
    [yY]*) warn "Continuing on linux/${darch} — expect emulation overhead or exec format errors." ;;
    *) error "Stopped before writing anything. Re-run on an amd64 host, or build from source."
       exit 1 ;;
  esac
}

check_ram() {
  local ram_gb=0
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    ram_gb=$(awk '/MemTotal/ { printf "%.0f", $2/1024/1024 }' /proc/meminfo)
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    ram_gb=$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))
  fi
  # Stashed for build_compose_args, which re-checks this same reading against a
  # higher floor once it knows whether an LLM is being bundled. Reading the
  # machine twice would be harmless; disagreeing with itself would not.
  DETECTED_RAM_GB=$ram_gb
  # Three outcomes, not two. `ram_gb` stays at its 0 initialiser on any platform
  # neither branch above matches (a BSD, a busybox container, WSL reporting an
  # unexpected $OSTYPE), and 0 is excluded from the "too small" test by the
  # `> 0` guard -- so the unread case fell to the else and printed the green
  # "0GB RAM — sufficient". A pre-flight check that cannot read the machine must
  # say so; it must not certify it.
  if (( ram_gb == 0 )); then
    warn "Could not read this machine's RAM (unrecognised platform: ${OSTYPE:-unknown})."
    warn "rsync.ai recommends at least ${MIN_RAM_GB}GB — continuing without the check."
  elif (( ram_gb < MIN_RAM_GB )); then
    warn "Only ${ram_gb}GB RAM detected. rsync.ai recommends at least ${MIN_RAM_GB}GB."
    warn "The stack may be slow or unstable on this machine."
  else
    info "${ram_gb}GB RAM — sufficient"
  fi
}

# ─── Environment setup ───────────────────────────────────────────────────────

generate_secret() {
  # 32 characters, because ENCRYPTION_KEY is truncated to its first 32 bytes by
  # the services that read it -- a short key is not rejected, it is padded out of
  # existence, and every connection encrypted under it becomes undecryptable.
  #
  # No `head` on the OUTPUT side of a pipe. `head -c 32` closes the pipe as soon
  # as it has its bytes, the upstream producer takes SIGPIPE and exits 141, and
  # `set -o pipefail` (line 2) turns that into the failure of the whole
  # assignment -- so `set -e` killed the installer at its first generated secret
  # on any box without openssl. Verified: exit 141, before any output.
  #
  # Bounding the INPUT instead (`head -c 4096 /dev/urandom`) has no such
  # downstream close: head reads its 4096 bytes and exits, tr drains them and
  # exits 0. Roughly a quarter of random bytes are alphanumeric, so ~1000
  # candidates for a 32-character need.
  local raw=""
  if command -v openssl &>/dev/null; then
    raw=$(openssl rand -base64 96 | LC_ALL=C tr -dc 'a-zA-Z0-9')
  else
    raw=$(head -c 4096 /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9')
  fi
  if (( ${#raw} < 32 )); then
    error "Could not generate a 32-character secret (got ${#raw} chars)."
    echo "  Install openssl, or report this with your OS and shell version." >&2
    exit 1
  fi
  printf '%s' "${raw:0:32}"
}

prompt_env() {
  section "Configuration"
  echo "  We need a few values to set up rsync.ai."
  echo "  Press Enter to accept defaults where shown."
  echo ""

  if (( ! TTY_OK )); then
    warn "No terminal available — running non-interactively."
    echo "  Values come from the environment; anything unset takes its default."
    echo "  Recognised: OPENAI_API_KEY, OPENAI_BASE_URL, LLM_PROVIDER, LLM_MODEL,"
    echo "              OLLAMA_URL, PUBLIC_HOST, ADMIN_EMAIL, RSYNC_VERSION"
    echo ""
  fi

  # LLM provider: OpenAI (cloud) or Ollama (local, fully offline — no API key)
  echo "  LLM provider:"
  echo "    1) OpenAI  — cloud, needs an API key (best quality)"
  echo "    2) Ollama  — local, fully offline, no key (one is started for you, model included)"
  # Without a terminal, an OPENAI_API_KEY in the environment is a clear enough
  # statement of intent to pick provider 1; with nothing set, the offline
  # provider is the only one that can work unattended.
  _llm_default=1
  if [[ -z "${OPENAI_API_KEY:-}" && "${LLM_PROVIDER:-}" != "openai" ]] && (( ! TTY_OK )); then
    _llm_default=2
  fi
  # Records that the OPERATOR asked for Ollama, as opposed to the branch above
  # defaulting to it because nothing else could work. Only the second case needs
  # to be announced. `|| true` because a failing [[ ]] is the last command in this
  # sequence and `set -e` would take the whole script down with it.
  LLM_PROVIDER_EXPLICIT=0
  [[ "${LLM_PROVIDER:-}" == "ollama" ]] && { _llm_default=2; LLM_PROVIDER_EXPLICIT=1; } || true
  ask _llm_choice "  Choose [${_llm_default}]: " "$_llm_default"
  # A terminal answer of "2" is also a deliberate choice, not a fallback.
  (( TTY_OK )) && [[ "${_llm_choice:-}" == "2" ]] && LLM_PROVIDER_EXPLICIT=1 || true
  if [[ "${_llm_choice:-1}" == "2" ]]; then
    LLM_PROVIDER="ollama"
    LLM_MODEL="${LLM_MODEL:-qwen2.5:7b}"
    # The bundled service, not the host. Until docker-compose.ollama.yml grew a
    # pull job this defaulted to host.docker.internal, which is the right answer
    # only when the operator already runs an Ollama -- and nothing here started
    # one, so the common case was a stack that came up green and answered every
    # prompt with a connection error. An OLLAMA_URL already in the environment
    # still wins, which is how you keep pointing at a host or a remote Ollama;
    # build_compose_args reads this value back out of the .env and layers the
    # overlay only when it names the bundle.
    OLLAMA_URL="${OLLAMA_URL:-http://ollama:11434}"
    OPENAI_API_KEY=""
    # Ollama is addressed by OLLAMA_URL; carrying an OpenAI-compatible base URL
    # into an offline install would only be a live pointer at a cloud endpoint.
    OPENAI_BASE_URL=""
    info "Using local Ollama at ${OLLAMA_URL} (model ${LLM_MODEL})."
    # Conditional, because it is only true of an Ollama this script does not
    # start. On the bundled path the overlay's ollama-pull job downloads the
    # model before any service that would ask for one starts, so printing the
    # manual step there tells the operator to do work that is already done.
    if [[ "$OLLAMA_URL" == *"//ollama:"* ]]; then
      info "An Ollama is bundled with the stack; its model is pulled on first start."
    else
      warn "Ensure Ollama is running and the model is pulled: ollama pull ${LLM_MODEL}"
    fi
    # host.docker.internal is free on Docker Desktop and absent on Linux Docker.
    # The compose file now maps it to host-gateway, so the name resolves -- but
    # host-gateway is the bridge address, and Ollama listens on 127.0.0.1 by
    # default, which the bridge cannot reach. Say so here rather than let it
    # surface later as an LLM that never answers.
    if [[ "$OLLAMA_URL" == *host.docker.internal* && "$(uname -s)" == "Linux" ]]; then
      warn "On Linux, also start Ollama on all interfaces or containers cannot reach it:"
      echo "    OLLAMA_HOST=0.0.0.0 ollama serve"
    fi
    # An unattended run reaches this branch by DEFAULT whenever no OPENAI_API_KEY
    # is set -- nobody chose Ollama, the script did. Name that, because otherwise
    # the first symptom is a stack that comes up healthy and cannot answer a prompt.
    if (( ! TTY_OK )) && [[ "${LLM_PROVIDER_EXPLICIT:-0}" != "1" ]]; then
      warn "No terminal and no OPENAI_API_KEY, so Ollama was selected for you."
      echo "  If you meant to use OpenAI, re-run with: OPENAI_API_KEY=sk-... bash"
    fi
  else
    LLM_PROVIDER="openai"
    LLM_MODEL="${LLM_MODEL:-gpt-4o}"
    OLLAMA_URL="${OLLAMA_URL:-http://host.docker.internal:11434}"
    # "openai" here names the wire protocol, not the vendor. Vertex AI, Azure,
    # Groq, OpenRouter, Together and vLLM all serve it, and OPENAI_BASE_URL is
    # what points at one of them. Carried through to the generated .env below;
    # empty means api.openai.com, which is the client's own default.
    OPENAI_BASE_URL="${OPENAI_BASE_URL:-}"
    if [[ -z "${OPENAI_API_KEY:-}" ]]; then
      if (( ! TTY_OK )); then
        error "OpenAI was selected but OPENAI_API_KEY is not set, and there is no"
        echo "  terminal to ask on. Either:" >&2
        echo "    curl -sSL <url> | OPENAI_API_KEY=sk-... bash" >&2
        echo "    curl -sSL <url> | LLM_PROVIDER=ollama bash   # fully offline, no key" >&2
        exit 1
      fi
      # Bounded. `ask` reads fd 3, and a read that hits EOF leaves the variable
      # at its previous value and returns -- which is indistinguishable here from
      # the operator pressing Enter. An unbounded `while true` over that turns a
      # closed stdin into an infinite loop printing the same error forever, on the
      # one prompt most likely to be reached by a piped `curl | bash`.
      # The sk- shape belongs to OpenAI and to nobody else. When OPENAI_BASE_URL
      # points at another OpenAI-compatible endpoint the key is that vendor's
      # (Groq's gsk_, an OpenRouter sk-or-, a Vertex OAuth token), so demanding
      # sk- there would reject every key that could possibly work.
      local key_prompt="  OpenAI API Key (sk-...): "
      local require_sk=1
      if [[ -n "${OPENAI_BASE_URL:-}" ]]; then
        key_prompt="  API key for ${OPENAI_BASE_URL}: "
        require_sk=0
      fi
      local key_tries=0
      while true; do
        ask OPENAI_API_KEY "$key_prompt"
        local key_ok=0
        if (( require_sk )); then
          if [[ "$OPENAI_API_KEY" == sk-* ]]; then key_ok=1; fi
        else
          if [[ -n "$OPENAI_API_KEY" ]]; then key_ok=1; fi
        fi
        if (( key_ok )); then break; fi
        key_tries=$(( key_tries + 1 ))
        if (( key_tries >= 3 )); then
          error "  No usable API key after 3 attempts."
          echo "    Pass one non-interactively, or install with no key at all:" >&2
          echo "      curl -sSL <url> | OPENAI_API_KEY=sk-... bash" >&2
          echo "      curl -sSL <url> | LLM_PROVIDER=ollama bash   # fully offline" >&2
          exit 1
        fi
        if (( require_sk )); then
          error "  Must start with 'sk-'. Get yours at https://platform.openai.com/api-keys"
        else
          error "  Empty key. Enter the key ${OPENAI_BASE_URL} expects."
        fi
      done
    else
      info "OPENAI_API_KEY already set in environment"
    fi
  fi

  # Domain / URL
  #
  # The compose file this script downloads publishes exactly two ports, both on
  # 127.0.0.1, and contains no proxy, no 443 listener and no TLS. So naming a host
  # here is not on its own enough to make that host reachable -- and the failure was
  # invisible, because wait_healthy polls localhost and therefore passed no matter
  # what was typed. The install then printed https://<host> and was believed.
  #
  # Two ways to actually serve it, and they need opposite settings, so ask:
  #   direct  -> publish the ports on 0.0.0.0 and hand out http://<host>:3000
  #   proxied -> leave the ports on loopback (a proxy on the host reaches them)
  #              and hand out https://<host>
  ask PUBLIC_HOST "  Your domain or server IP (default: localhost): " "${PUBLIC_HOST:-localhost}"

  RSYNC_BIND_ADDR="127.0.0.1"
  if [[ "$PUBLIC_HOST" == "localhost" ]]; then
    PUBLIC_URL="http://localhost:5001"
    PUBLIC_WS_URL="ws://localhost:5001/ws"
    NEXTAUTH_URL="http://localhost:3000"
  else
    echo ""
    echo "  Is TLS terminated by a reverse proxy (nginx/Caddy/Traefik) in front of this stack?"
    echo "    1) No  — publish the ports directly; you get http://${PUBLIC_HOST}:3000 (no TLS)"
    echo "    2) Yes — keep the ports on loopback; you get https://${PUBLIC_HOST}"
    # Default 1, because it is the only answer that works with nothing else
    # installed, and an unattended run has no way to conjure a proxy. Answering 2
    # when no proxy exists produces the exact unreachable-URL failure this block
    # was written to remove, so the default must be the self-sufficient one.
    ask _proxy_choice "  Choose [1]: " "1"
    if [[ "${_proxy_choice:-1}" == "2" ]]; then
      PUBLIC_URL="https://${PUBLIC_HOST}"
      PUBLIC_WS_URL="wss://${PUBLIC_HOST}/ws"
      NEXTAUTH_URL="https://${PUBLIC_HOST}"
      info "Ports stay on 127.0.0.1. Point your proxy at 127.0.0.1:3000 (UI) and 127.0.0.1:5001 (API)."
    else
      RSYNC_BIND_ADDR="0.0.0.0"
      PUBLIC_URL="http://${PUBLIC_HOST}:5001"
      PUBLIC_WS_URL="ws://${PUBLIC_HOST}:5001/ws"
      NEXTAUTH_URL="http://${PUBLIC_HOST}:3000"
      warn "Ports 3000 and 5001 will be published on ALL interfaces, over plain HTTP."
      echo "  Anyone who can reach this machine on those ports can reach rsync.ai, and"
      echo "  logins cross the network in the clear. Before using this for anything real,"
      echo "  restrict them at your firewall or security group, and put TLS in front —"
      echo "  see docs/deployment/self-hosting.md for a reverse-proxy overlay."
    fi
    echo ""
  fi

  # A browser silently discards a Secure cookie that arrives over plain http,
  # with one exception: localhost, which it treats as a trustworthy origin. So
  # this cannot be left to ENVIRONMENT -- that would work on a laptop and fail on
  # every server install, with a successful 200 on the login request and an
  # immediate bounce back to the form. Derived from whichever scheme the branches
  # above settled on, so the two can never disagree.
  if [[ "${PUBLIC_URL}" == https://* ]]; then
    RSYNC_COOKIE_SECURE="true"
  else
    RSYNC_COOKIE_SECURE="false"
  fi

  # Admin email
  ask ADMIN_EMAIL "  Admin email for rsync.ai login: " "${ADMIN_EMAIL:-admin@rsync.ai}"

  # Auto-generate secrets
  POSTGRES_PASSWORD=$(generate_secret)
  REDIS_PASSWORD=$(generate_secret)
  # The bundled demo warehouse (docker-compose.quickstart.yml). It has a working
  # literal default so a bare `docker compose up` still works, but an install
  # gets a real one -- it is a separate credential from POSTGRES_PASSWORD on
  # purpose, because the demo destination is one any workspace member can run SQL
  # against.
  RSYNC_DEMO_WAREHOUSE_PASSWORD=$(generate_secret)
  JWT_SECRET=$(generate_secret)
  ENCRYPTION_KEY=$(generate_secret)
  # Shared service-to-service secret. Its absence makes the api-gateway internal
  # route 503 (internal_service_not_configured), breaking OAuth token refresh.
  INTERNAL_SERVICE_SECRET=$(generate_secret)
  # Generated HERE, not inline in write_env's heredoc. `KEY=$(generate_secret)`
  # written inside `cat <<EOF` puts the call in a command substitution whose
  # `exit 1` exits only that subshell, and a heredoc's expansions do not set the
  # exit status of the owning `cat`. So a generate_secret failure there is
  # structurally invisible to `set -e`: MinIO silently gets an empty access key
  # and secret key, and the installer carries on to print success. As a plain
  # assignment at this level, the same failure aborts.
  MINIO_ACCESS_KEY=$(generate_secret)
  MINIO_SECRET_KEY=$(generate_secret)

  info "Secrets generated"
}

write_env() {
  cat > "${INSTALL_DIR}/${ENV_FILE}" <<EOF
# rsync.ai environment — generated by install.sh $(date '+%Y-%m-%d %H:%M:%S')
# DO NOT commit this file to git.

# ── Required ──────────────────────────────────────────────────────────────────
OPENAI_API_KEY=${OPENAI_API_KEY}

# ── Database ──────────────────────────────────────────────────────────────────
POSTGRES_USER=rsync
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
# Bundled demo warehouse — the destination half of the zero-credential try-it
# path. Separate from POSTGRES_PASSWORD above by design.
RSYNC_DEMO_WAREHOUSE_PASSWORD=${RSYNC_DEMO_WAREHOUSE_PASSWORD}

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_PASSWORD=${REDIS_PASSWORD}

# ── Security (auto-generated, do not change after first run) ──────────────────
JWT_SECRET=${JWT_SECRET}
ENCRYPTION_KEY=${ENCRYPTION_KEY}
INTERNAL_SERVICE_SECRET=${INTERNAL_SERVICE_SECRET}

# ── URLs ──────────────────────────────────────────────────────────────────────
PUBLIC_URL=${PUBLIC_URL}
PUBLIC_WS_URL=${PUBLIC_WS_URL}
NEXTAUTH_URL=${NEXTAUTH_URL}
FRONTEND_URL=${NEXTAUTH_URL}
RSYNC_CORS_ORIGINS=${NEXTAUTH_URL}
# Secure flag on the session and CSRF cookies. Follows the scheme above: true for
# https, false for plain http, because browsers drop Secure cookies on http and
# the login would appear to succeed while no session ever persists. Set this to
# true the moment you put TLS in front.
RSYNC_COOKIE_SECURE=${RSYNC_COOKIE_SECURE}
# Interface the two published ports bind to. 127.0.0.1 keeps them reachable only
# from this machine (correct for a laptop, and for a reverse proxy running on the
# host); 0.0.0.0 publishes them to the network, which a server install needs if
# nothing is proxying in front. Changing this needs a restart, not just a reload.
RSYNC_BIND_ADDR=${RSYNC_BIND_ADDR}

# ── Admin ─────────────────────────────────────────────────────────────────────
RSYNC_ADMIN_EMAILS=${ADMIN_EMAIL}

# ── LLM ───────────────────────────────────────────────────────────────────────
LLM_PROVIDER=${LLM_PROVIDER}
LLM_MODEL=${LLM_MODEL}
OLLAMA_URL=${OLLAMA_URL}
# Any OpenAI-compatible endpoint: Vertex AI, Azure OpenAI, Groq, OpenRouter,
# Together, a local vLLM. Empty = OpenAI's own api.openai.com. LLM_PROVIDER stays
# "openai" for all of them — it names the wire protocol, not the vendor — and
# LLM_MODEL must then be a name that endpoint's catalog actually has.
OPENAI_BASE_URL=${OPENAI_BASE_URL}

# ── Object Storage (internal MinIO) ───────────────────────────────────────────
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY}

# ── OAuth (optional — leave blank to use email login only) ────────────────────
GITHUB_CLIENT_ID=
GITHUB_CLIENT_SECRET=
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=

# ── Version ───────────────────────────────────────────────────────────────────
# See the notes at the end of this file. Derived from RSYNC_REF at install time so
# the images match the compose file this .env sits next to; written out rather
# than left empty so the pairing survives re-running compose by hand later.
RSYNC_VERSION=${RSYNC_VERSION}
# The ref the line above was derived from, recorded so a LATER run can tell an
# upgrade from an ordinary re-run. Same ref, and that run touches nothing here;
# a different one, and it re-downloads the compose files and rewrites the
# version above to match. Delete this line and the next run treats the install
# as unrecorded and refreshes both halves once.
RSYNC_INSTALLED_REF=${RSYNC_REF}
EOF

  # A SECOND heredoc, and quoted: <<'EOF'. Everything above needs interpolation
  # (that is how the generated secrets get in), which means everything above is
  # also a shell context -- a backtick or a $( in a COMMENT there is executed and
  # its text is silently deleted from the file. That is not hypothetical; it
  # happened to this block while it was being written, and the .env came out with
  # holes where the prose had been. Documentation carries punctuation, so it goes
  # somewhere punctuation is inert.
  cat >> "${INSTALL_DIR}/${ENV_FILE}" <<'EOF'

# ── About RSYNC_VERSION ───────────────────────────────────────────────────────
# One knob for every rsync.ai image in the stack. The installer filled it in for
# you, from the same RSYNC_REF it fetched this stack's compose file from, so both
# halves name the same code. Change it only to deliberately pin somewhere else.
#
# It is written out rather than left blank on purpose. Blank falls back to
# `latest`, which the release workflow mints only on a tag -- so a blank line
# silently pairs a compose file from one ref with images from whichever release
# happened to be newest, which is how three services the compose starts came to
# be ones the resolved tag had never built.
#
# Note the image tag is NOT spelled like the git tag: the release workflow
# publishes `{{version}}`, which strips the leading `v`. Git tag v0.1.1 becomes
# image tag 0.1.1, so RSYNC_VERSION=0.1.1 is right and RSYNC_VERSION=v0.1.1 is a
# tag that was never pushed -- every image in the stack then fails to pull at
# once. The published tags are listed at
# https://github.com/orgs/rsync-ai/packages.
#
# It is deliberately all-or-nothing. Pinning some images and floating others
# gives you a stack whose halves disagree about wire formats, and that surfaces
# later than startup -- at whichever message the two versions read differently.

# ── Optional profiles ─────────────────────────────────────────────────────────
# RSYNC_PROFILES holds the set this install activates; it defaults to `cdc`.
#
#   cdc       change-data-capture (Kafka Connect + Debezium + the sink worker).
#             ON by default. Absent, a streaming pipeline does not degrade --
#             the orchestrator's infra pre-flight requires all three services
#             together and fails the run once they do not answer.
#   generate  connector generation against live API docs. OFF by default, and it
#             is the other case: the generator probes context7-mcp with a 3s
#             timeout and carries on without it, so its absence costs a
#             documentation lookup, not a run.
#
# Turn CDC off on a machine that will only ever run batch syncs:
#
#   curl -sSL .../install.sh | RSYNC_PROFILES= bash
#
# Or run both:
#
#   curl -sSL .../install.sh | RSYNC_PROFILES=cdc,generate bash
#
# The resolved set is also written to the generated .env as COMPOSE_PROFILES, so
# a compose command typed by hand in the install directory starts the same
# services this script did.

# ── Bring-your-own Kafka (optional) ───────────────────────────────────────────
# Unset, the stack runs the Kafka broker defined in this compose file, over
# PLAINTEXT on a private network, and none of the following applies.
#
# Point KAFKA_BROKERS at a managed cluster (MSK, Confluent Cloud, Aiven,
# Redpanda) and you MUST also set the security profile. With the protocol left
# empty every client defaults to PLAINTEXT and dials anonymously; a TLS listener
# refuses that, and the refusal does not surface as a connection error -- it
# surfaces as a pipeline that reports `completed` with an empty destination.
# Layer docker-compose.byo-kafka.yml on top to drop the bundled broker.
#
#   KAFKA_BROKERS=b-1.example.com:9096,b-2.example.com:9096
#   KAFKA_SECURITY_PROTOCOL=SASL_SSL
#   KAFKA_SASL_MECHANISM=SCRAM-SHA-512
#   KAFKA_SASL_USERNAME=
#   KAFKA_SASL_PASSWORD=
#
# The cdc profile adds a JVM (Kafka Connect) that reads the same credentials in
# JAAS form. It is not derivable from the two lines above: the password crosses
# two grammars on its way in, so escaping it once corrupts it in exactly the way
# not escaping it does. cdc is in the default profile set, so an external broker
# needs this line set even if you never asked for CDC -- or RSYNC_PROFILES=
# to leave the JVM out entirely:
#
#   KAFKA_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";
#
# TLS material is read from paths INSIDE the containers -- mount the files
# yourself; this file cannot guess a host path.
#
#   KAFKA_SSL_CA_LOCATION=/certs/ca.pem

# -- Bring-your-own PostgreSQL (optional) --------------------------------------
# Unset, the stack runs the postgres container defined in the compose file and
# none of the following applies. This is the METADATA database -- pipeline
# definitions, run history, and your encrypted connection credentials. Copied
# rows never pass through it.
#
# Set POSTGRES_HOST to anything other than `postgres` and re-run this installer:
# it layers docker-compose.byo-postgres.yml for you and the bundled database
# never starts.
#
#   POSTGRES_HOST=my-instance.abc123.us-east-1.rds.amazonaws.com
#   POSTGRES_PORT=5432
#   POSTGRES_DB=pipeline_db
#   POSTGRES_SSLMODE=require
#
# Create the database and a role that can make tables in it first: api-gateway
# and orchestrator run their own migrations into it on first boot.
#
# Then create Temporal's two databases by hand — always, not only where CREATE
# DATABASE is forbidden. docker-compose.byo-postgres.yml sets SKIP_DB_CREATE=true,
# so auto-setup will not create them, and left enabled it would crash-loop the
# container on any role without CREATEDB anyway:
#
#   CREATE DATABASE temporal            OWNER rsync;
#   CREATE DATABASE temporal_visibility OWNER rsync;
#
# TLS is not one switch. POSTGRES_SSLMODE covers the three Go services;
# Temporal ignores it, and Temporal is two programs -- a schema tool that runs
# first and the server -- each reading different names for the same fact. The
# compose file feeds both from the POSTGRES_TLS_* keys below, so set these too
# against a database that mandates TLS. Miss them and the schema step fails
# before the server binds: no workflow engine, every pipeline hangs, and no log
# line anywhere says TLS.
#
#   POSTGRES_TLS_ENABLED=true
#   POSTGRES_TLS_SERVER_NAME=my-instance.abc123.us-east-1.rds.amazonaws.com
#   POSTGRES_TLS_CA_FILE=/certs/rds-ca.pem
#
# Hostname verification is the one pair you set twice, because the schema tool
# states it inverted and compose cannot negate a value:
#
#   POSTGRES_TLS_VERIFY_HOST=true
#   POSTGRES_TLS_SKIP_HOST_VERIFY=false
#
# The CA path is read INSIDE the container -- mount the file yourself; this
# file cannot guess a host path.
EOF
  chmod 600 "${INSTALL_DIR}/${ENV_FILE}"
  info ".env written to ${INSTALL_DIR}/${ENV_FILE}"
}

# ─── Install ─────────────────────────────────────────────────────────────────

fetch() {
  local url="$1" dest="$2"
  if command -v curl &>/dev/null; then
    curl -fsSL "$url" -o "$dest"
  elif command -v wget &>/dev/null; then
    wget -qO "$dest" "$url"
  else
    error "curl or wget is required."
    exit 1
  fi
  # `wget -qO "$dest"` creates and TRUNCATES $dest before the transfer, so an
  # HTTP error leaves a zero-byte file behind. main()'s `[[ -f ... ]] || fetch`
  # gates then read that as "already downloaded" and skip the re-fetch on every
  # subsequent run -- a permanent wedge, on a host with wget and no curl, that no
  # later run repairs. Delete the artefact so a retry is a retry.
  if [[ ! -s "$dest" ]]; then
    rm -f "$dest"
    error "Downloaded an empty file from ${url}"
    echo "  Check that ref '${RSYNC_REF}' exists in ${RSYNC_REPO}." >&2
    exit 1
  fi
}

download_compose() {
  section "Downloading rsync.ai"
  mkdir -p "$INSTALL_DIR"
  fetch "$COMPOSE_URL"                  "${INSTALL_DIR}/${COMPOSE_FILE}"
  fetch "${RAW_BASE}/${BYO_PG_FILE}"    "${INSTALL_DIR}/${BYO_PG_FILE}"
  fetch "${RAW_BASE}/${BYO_KAFKA_FILE}" "${INSTALL_DIR}/${BYO_KAFKA_FILE}"
  fetch "${OLLAMA_RAW_BASE}/${OLLAMA_FILE}" "${INSTALL_DIR}/${OLLAMA_FILE}"
  info "compose files downloaded (quickstart + both bring-your-own overlays + bundled LLM)"
}

# Which -f files this install actually runs with. Keyed off the .env on disk
# rather than the sourced shell variables, so an unrelated POSTGRES_HOST already
# exported in the operator's environment cannot silently disable the bundled
# database. Each overlay parks its bundled service in a profile that is never
# activated; the base file carries the matching `required: false` on every
# depends_on, without which Compose refuses the whole project.
build_compose_args() {
  COMPOSE_ARGS=( -f "${INSTALL_DIR}/${COMPOSE_FILE}" )
  # Unquoted on purpose -- this is the word split that turns "cdc,generate" into
  # two flags. An empty RSYNC_PROFILES yields zero iterations, which is the
  # opt-out. Flags rather than an exported COMPOSE_PROFILES because an exported
  # variable dies with this process, while these flags reach the operator: the
  # status / logs / retry commands printed at the end of a run are built from
  # COMPOSE_CMD, and COMPOSE_CMD is built from this array.
  local profile
  for profile in ${RSYNC_PROFILES//,/ }; do
    COMPOSE_ARGS+=( --profile "$profile" )
  done
  local envf="${INSTALL_DIR}/${ENV_FILE}" pg="" kb="" llm="" ourl=""
  if [[ -f "$envf" ]]; then
    # `|| true` is load-bearing, not defensive noise: this script runs under
    # `set -o pipefail`, so a grep that matches nothing fails the whole pipeline
    # and aborts the installer. Neither key is present in a default .env, so
    # without this EVERY standard install dies here.
    pg=$(grep -E '^[[:space:]]*POSTGRES_HOST=' "$envf" | tail -1 | cut -d= -f2- || true)
    kb=$(grep -E '^[[:space:]]*KAFKA_BROKERS=' "$envf" | tail -1 | cut -d= -f2- || true)
    llm=$(grep -E '^[[:space:]]*LLM_PROVIDER=' "$envf" | tail -1 | cut -d= -f2- || true)
    ourl=$(grep -E '^[[:space:]]*OLLAMA_URL=' "$envf" | tail -1 | cut -d= -f2- || true)
    pg="${pg//\"/}"; pg="${pg//\'/}"
    kb="${kb//\"/}"; kb="${kb//\'/}"
    llm="${llm//\"/}"; llm="${llm//\'/}"
    ourl="${ourl//\"/}"; ourl="${ourl//\'/}"
  fi
  # `postgres` and `kafka:29092` are the in-compose defaults -- naming them
  # explicitly still means "use the bundled one", not "I have my own".
  if [[ -n "$pg" && "$pg" != "postgres" ]]; then
    COMPOSE_ARGS+=( -f "${INSTALL_DIR}/${BYO_PG_FILE}" )
    info "External PostgreSQL configured (${pg}) — bundled database disabled."
  fi
  if [[ -n "$kb" && "$kb" != "kafka:29092" ]]; then
    COMPOSE_ARGS+=( -f "${INSTALL_DIR}/${BYO_KAFKA_FILE}" )
    info "External Kafka configured (${kb}) — bundled broker disabled."
  fi
  # The bundled LLM. Two conditions, and the second is the load-bearing one:
  # LLM_PROVIDER=ollama says the LLM tier speaks Ollama, it does not say WHICH
  # Ollama. Every .env written before this overlay grew a pull job carries
  # OLLAMA_URL=http://host.docker.internal:11434 -- an Ollama on the operator's
  # own machine. Layering the overlay on one of those would start a second,
  # empty server that nothing talks to, download several GB into it, and leave
  # the operator's own Ollama serving exactly as before. So it goes on only when
  # the URL names the bundled service, or is blank -- which write_env never
  # writes, but a hand-edited file can.
  if [[ "$llm" == "ollama" ]] && [[ -z "$ourl" || "$ourl" == *"//ollama:"* ]]; then
    COMPOSE_ARGS+=( -f "${INSTALL_DIR}/${OLLAMA_FILE}" )
    OLLAMA_BUNDLED=1
    info "Internal LLM configured — bundling an Ollama and pulling its model."
    # Same three outcomes as check_ram, against the higher floor: a reading we
    # could not take must not certify the machine.
    if (( DETECTED_RAM_GB == 0 )); then
      warn "RAM unread on this platform; the bundled model wants ${MIN_RAM_GB_WITH_LLM}GB total."
    elif (( DETECTED_RAM_GB < MIN_RAM_GB_WITH_LLM )); then
      warn "Only ${DETECTED_RAM_GB}GB RAM, and the bundled LLM wants ${MIN_RAM_GB_WITH_LLM}GB total"
      warn "(the model is resident on top of the stack). Expect swapping and slow answers."
      echo "  For a cloud model instead: set LLM_PROVIDER=openai and OPENAI_API_KEY in"
      echo "  ${envf}, then re-run this installer."
    fi
  elif [[ "$llm" == "ollama" ]]; then
    info "External Ollama configured (${ourl}) — no LLM container bundled."
  fi
  COMPOSE_CMD="docker compose $(printf '%s ' "${COMPOSE_ARGS[@]}")"
}

pull_images() {
  section "Pulling Docker images"
  echo "  This may take a few minutes on first run..."
  docker compose "${COMPOSE_ARGS[@]}" --env-file "${INSTALL_DIR}/${ENV_FILE}" pull --quiet
  info "Images ready"
}

start_stack() {
  section "Starting rsync.ai"
  # `up -d` normally returns in seconds. On the bundled-LLM path it BLOCKS until
  # the model is on disk, because the three Python services wait on ollama-pull
  # completing successfully -- deliberately so, since an Ollama with no model
  # answers every prompt with `model not found` while every container around it
  # reads healthy. Say that before the terminal goes quiet for several minutes.
  if (( OLLAMA_BUNDLED )); then
    warn "First start downloads the LLM into a docker volume — several GB, once."
    echo "  This blocks until it finishes. Watch it: docker logs -f rsync-ollama-pull"
  fi
  docker compose \
    "${COMPOSE_ARGS[@]}" \
    --env-file "${INSTALL_DIR}/${ENV_FILE}" \
    up -d --remove-orphans

  # `up -d` returning 0 means every container was CREATED and started, not that
  # any of them is still alive. Only a service that is somebody else's
  # `service_healthy` / `service_completed_successfully` dependency can fail this
  # command by being unhealthy; compose's short-list `depends_on` is
  # `service_started`, so a container that starts and immediately exits leaves
  # rc=0 behind. Name them now, while the exit code is still on screen.
  local dead=""
  dead=$(docker compose "${COMPOSE_ARGS[@]}" --env-file "${INSTALL_DIR}/${ENV_FILE}" \
    ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' 2>/dev/null \
    | awk '$2 != "running" { print "    " $1 " (" $2 ", exit " $3 ")" }' || true)
  if [[ -n "$dead" ]]; then
    warn "These containers are not running:"
    printf '%s\n' "$dead" >&2
    echo "  They may still be starting. If the wait below fails, look here first." >&2
  fi
}

# Set by probe_ready to whatever the gateway said about itself, so the failure
# path can print the diagnosis rather than a generic timeout.
READY_REASON=""

probe_ready() {
  local out code
  READY_REASON=""
  # /ready, not /health. api-gateway's /health is a hardcoded 200 literal, and
  # cmd/server/main.go logs-and-CONTINUES when db.Init() fails ("using mock
  # data") and when db.Migrate fails -- so a gateway with no database answers
  # /health 200 forever. That is precisely the stack this installer used to
  # certify. /ready (cmd/server/ready.go readinessVerdict) 503s with
  # db_not_connected / db_ping_failed / schema_not_migrated instead.
  #
  # No `-f`: it makes curl exit 22 on a 503 and DISCARD the body, and the body is
  # the diagnosis. A refused connection is still rc != 0; a 503 is rc 0 with a
  # code of 503, which is the distinction this function is built on.
  out=$(curl -s --max-time 5 -w '\n%{http_code}' http://localhost:5001/ready 2>/dev/null) || return 1
  code="${out##*$'\n'}"
  case "$code" in
    200) return 0 ;;
    404|405)
      # An image predating /ready. Fall back to liveness so this installer still
      # works against an older pinned RSYNC_VERSION, and say that it did.
      READY_REASON="this image has no /ready endpoint; fell back to /health"
      if ! curl -sf --max-time 5 http://localhost:5001/health >/dev/null 2>&1; then
        READY_REASON="no /ready in this image, and /health did not answer either"
        return 1
      fi
      return 0
      ;;
    *)
      READY_REASON=$(printf '%s' "${out%$'\n'*}" | tr -d '\n' | cut -c1-200)
      if [[ -z "$READY_REASON" ]]; then READY_REASON="HTTP ${code:-no response}"; fi
      return 1
      ;;
  esac
}

wait_healthy() {
  section "Waiting for services to become healthy"
  # A wall-clock deadline, not a count of sleeps. The old loop added 3 to
  # `elapsed` per iteration and ignored the time each curl spent, so a socket
  # that accepted and never answered made the advertised 60-second budget
  # unbounded. SECONDS is bash's own monotonic counter.
  local deadline=$(( SECONDS + 300 ))
  echo -n "  "
  while ! probe_ready; do
    if (( SECONDS >= deadline )); then
      echo ""
      error "The stack did not become ready within 5 minutes."
      if [[ -n "$READY_REASON" ]]; then
        echo "  The API gateway answered: ${READY_REASON}" >&2
      fi
      # `return 1`, not a bare `return`. A bare `return` carries the status of
      # the previous command -- after an `echo`, that is 0 -- so this branch used
      # to report SUCCESS to main(), which printed the green "rsync.ai is
      # running!" banner and a login URL over a stack that had never come up.
      return 1
    fi
    echo -n "."
    sleep 3
  done
  echo ""
  if [[ -n "$READY_REASON" ]]; then
    warn "rsync.ai is up (${READY_REASON})"
  else
    info "rsync.ai is up!"
  fi

  # The loop above proves the gateway is serving on loopback; it says nothing
  # about the URL this script is about to print. That is how a server install
  # could report success and hand the operator an address that answered nothing.
  # A warning, not a failure: a closed cloud security group is outside this
  # script's reach and may be a deliberate choice.
  #
  # Keyed on RSYNC_BIND_ADDR and NEXTAUTH_URL, both of which write_env persists.
  # It used to key on PUBLIC_HOST, which prompt_env asks for and write_env never
  # writes -- so on the re-run path ("Existing .env found") the variable was
  # always empty, the condition was always false, and the probe added to stop
  # this script printing an unreachable URL never ran. Re-running over an
  # existing .env is the documented bring-your-own-Postgres workflow, so that was
  # the common path, not the corner case.
  if [[ "${RSYNC_BIND_ADDR:-}" == "0.0.0.0" && "${NEXTAUTH_URL:-}" != *localhost* ]]; then
    if ! curl -sf --max-time 5 "${NEXTAUTH_URL}" &>/dev/null; then
      warn "${NEXTAUTH_URL} did not answer from here."
      echo "  The stack is healthy on this machine, so the usual cause is the port not"
      echo "  being open to you: check the cloud security group / firewall for TCP 3000"
      echo "  and 5001, and that the host in ${NEXTAUTH_URL} resolves to this server."
    else
      info "${NEXTAUTH_URL} is reachable."
    fi
  fi
  return 0
}

print_failure() {
  echo ""
  echo -e "${RED}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${RED}${BOLD}  rsync.ai did not come up.${NC}"
  echo -e "${RED}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo ""
  echo "  Nothing has been torn down — the containers are still there to inspect."
  echo ""
  echo -e "  ${BOLD}Look here first:${NC}"
  echo "    Status:   ${COMPOSE_CMD}ps -a"
  echo "    Logs:     ${COMPOSE_CMD}logs --tail=100 api-gateway"
  echo "    Retry:    ${COMPOSE_CMD}up -d"
  echo "    Remove:   ${COMPOSE_CMD}down -v"
  echo ""
  # Named because it is a real mode with a specific fix, not a guess. The
  # gateway's schema check is a one-shot latch set only by a successful migration
  # in that process (internal/db/db.go), and the process does not exit when it
  # loses the cold-boot race with Postgres -- so `restart: unless-stopped` never
  # fires and /ready stays 503 indefinitely. Restarting just that container
  # re-runs the migration against a Postgres that is now up.
  echo -e "  ${BOLD}If the gateway reported schema_not_migrated or db_ping_failed:${NC}"
  echo "    it lost the startup race with Postgres. Restart that one container:"
  echo "      ${COMPOSE_CMD}restart api-gateway"
  echo ""
  echo -e "  ${BOLD}Anything else:${NC} https://github.com/${RSYNC_REPO}/issues"
  echo ""
}

print_success() {
  echo ""
  echo -e "${GREEN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${GREEN}${BOLD}  rsync.ai is running!${NC}"
  echo -e "${GREEN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo ""
  echo -e "  ${BOLD}Open in browser:${NC}  ${NEXTAUTH_URL}"
  echo -e "  ${BOLD}Admin email:${NC}      ${ADMIN_EMAIL}"
  echo -e "  ${BOLD}Install dir:${NC}      ${INSTALL_DIR}"
  echo ""
  echo -e "  ${BOLD}Useful commands:${NC}"
  echo "    Stop:     ${COMPOSE_CMD}down"
  echo "    Logs:     ${COMPOSE_CMD}logs -f"
  echo "    Update:   ${COMPOSE_CMD}pull && ${COMPOSE_CMD}up -d"
  echo ""
  echo -e "  ${BOLD}Documentation:${NC}  https://rsync.ai/docs"
  echo -e "  ${BOLD}License:${NC}        Elastic License 2.0 — free to self-host"
  echo ""
}

# ─── Main ────────────────────────────────────────────────────────────────────

# Read ONE key out of the .env as DATA.
#
# The old code did `source "${INSTALL_DIR}/${ENV_FILE}"`, which EXECUTES it. The
# file this very script writes carries a commented Kafka block telling operators
# to add a KAFKA_SASL_JAAS_CONFIG line, and the example they are told to copy in
# ends `... username="user" password="pass";`. Sourced, the trailing `;` splits
# it into a second command and the unquoted words make the line a command
# invocation, not an assignment. And `source file || true` does NOT suppress
# errexit inside the sourced file on bash 3.2.57 -- which is what
# `/usr/bin/env bash` selects on a stock Mac -- so the installer died there with
# the call site's `2>/dev/null` swallowing the reason: a blank screen and a
# non-zero exit, on the documented re-run path.
#
# Grepping the value out cannot execute anything.
env_value() {
  local key="$1" file="${INSTALL_DIR}/${ENV_FILE}" line=""
  [[ -f "$file" ]] || return 0
  line=$(grep -E "^[[:space:]]*${key}=" "$file" | tail -1 || true)
  line="${line#*=}"
  # Strip one matched pair of surrounding quotes, the way compose's own env-file
  # parser does. Anything else is passed through verbatim.
  line="${line%\"}"; line="${line#\"}"
  line="${line%\'}"; line="${line#\'}"
  printf '%s' "$line"
}

# The write half of env_value, and it exists for the same reason: this file is
# read and edited, never sourced. Replaces the key in place rather than
# appending a second line that would win by being later -- the .env is a
# document an operator reads, and two RSYNC_VERSION lines with different values,
# only one of them live, is precisely the confusion the notes at the end of that
# file exist to prevent. Duplicates already present collapse to the one line.
set_env_value() {
  local key="$1" value="$2" file="${INSTALL_DIR}/${ENV_FILE}" tmp
  tmp="$(mktemp "${file}.XXXXXX")"
  # Before a byte is written, not after: this file carries generated secrets and
  # the temp copy is about to BECOME it. Same directory, so the mv is atomic and
  # a crash mid-write cannot leave a half-written .env behind.
  chmod 600 "$tmp"
  # awk -v, not sed: a ref may contain a slash (release/1.0) and a value an
  # ampersand, both live in a sed replacement and both inert in an awk variable.
  awk -v k="$key" -v v="$value" '
    $0 ~ "^[[:space:]]*" k "=" { if (!seen) { print k "=" v; seen = 1 } ; next }
    { print }
    END { if (!seen) print k "=" v }
  ' "$file" > "$tmp"
  mv "$tmp" "$file"
}

main() {
  setup_tty
  banner
  section "Pre-flight checks"
  check_docker
  check_arch
  check_ram

  # If .env already exists in install dir, skip prompts
  if [[ -f "${INSTALL_DIR}/${ENV_FILE}" ]]; then
    warn "Existing .env found at ${INSTALL_DIR}/${ENV_FILE} — using it."
    warn "Delete it to reconfigure."
    # Read, never source. See env_value's comment for what sourcing this file does.
    NEXTAUTH_URL="$(env_value NEXTAUTH_URL)"
    NEXTAUTH_URL="${NEXTAUTH_URL:-http://localhost:3000}"
    ADMIN_EMAIL="$(env_value RSYNC_ADMIN_EMAILS)"
    ADMIN_EMAIL="${ADMIN_EMAIL:-admin@rsync.ai}"
    # wait_healthy's public-URL probe keys on this; it is written by write_env.
    RSYNC_BIND_ADDR="$(env_value RSYNC_BIND_ADDR)"
    # Backfill INTERNAL_SERVICE_SECRET into .env files created before it existed
    # (its absence 503s internal OAuth-refresh). Append once; keep any existing value.
    if ! grep -q '^INTERNAL_SERVICE_SECRET=' "${INSTALL_DIR}/${ENV_FILE}"; then
      # Into a variable FIRST. Written as `echo "K=$(generate_secret)" >> file`
      # the failure sits inside a command substitution, where `exit 1` exits only
      # the subshell: the redirect still succeeds, an EMPTY value is appended,
      # and the "Backfilled" line below prints over it. As a plain assignment the
      # same failure aborts under `set -e`.
      local backfilled_secret
      backfilled_secret=$(generate_secret)
      echo "INTERNAL_SERVICE_SECRET=${backfilled_secret}" >> "${INSTALL_DIR}/${ENV_FILE}"
      warn "Backfilled a missing INTERNAL_SERVICE_SECRET into the existing .env."
    fi

    # An upgrade is the only reason to re-run this script, and until this block
    # existed a re-run could not perform one. BOTH halves of an install stayed
    # pinned to the first run's ref, by two separate mechanisms that each look
    # correct alone: the compose file is refreshed only when MISSING (the
    # `[[ -f ]] ||` line below, which repairs a deleted file and was never a
    # version check), and RSYNC_VERSION is written by write_env, which this
    # branch skips. So `RSYNC_REF=main` over a v0.1.2 install re-read the v0.1.2
    # compose file, re-pulled the 0.1.2 images it already had, recreated
    # nothing, and printed the success banner -- an upgrade that upgraded
    # nothing and said it worked, which is worse than one that fails.
    #
    # Keyed on the ref recorded in the .env rather than on the files' contents.
    # It is the one fact that says which code this directory is meant to be
    # running, and comparing it costs nothing in the ordinary case -- same ref,
    # touch nothing -- which is the case where overwriting a hand-edited compose
    # file would be the damage.
    #
    # An install written before that record existed has none, so it reads empty
    # and every ref differs from it. That is the intended reading, not a corner
    # case: those are exactly the installs stuck at their first ref, and one
    # re-run adopts the requested ref and records it for next time.
    local recorded_ref
    recorded_ref="$(env_value RSYNC_INSTALLED_REF)"
    if [[ "$recorded_ref" != "$RSYNC_REF" ]]; then
      if [[ -n "$recorded_ref" ]]; then
        info "Installed at ref ${recorded_ref}, ${RSYNC_REF} requested -- refreshing compose files and image tag."
      else
        info "This install predates ref tracking -- adopting ${RSYNC_REF} and refreshing compose files and image tag."
      fi
      # The compose file is the only artifact here an operator may have edited
      # by hand, and this is the one path that overwrites it. Keep the outgoing
      # copy so the edit is recoverable rather than merely lost.
      if [[ -f "${INSTALL_DIR}/${COMPOSE_FILE}" ]]; then
        cp "${INSTALL_DIR}/${COMPOSE_FILE}" "${INSTALL_DIR}/${COMPOSE_FILE}.previous"
      fi
      download_compose
      # Both halves, together, or this fixes half the bug: the compose file
      # names ${RSYNC_VERSION} for every image it starts, and compose reads that
      # from the .env on disk -- never from this script's environment, which is
      # why deriving it correctly at the top was not enough on this path.
      set_env_value RSYNC_VERSION "${RSYNC_VERSION}"
      set_env_value RSYNC_INSTALLED_REF "${RSYNC_REF}"
    fi
  else
    prompt_env
    download_compose
    write_env
  fi

  # Unconditional, because the two artifacts are independent: the branch above
  # keys off the .env alone, so an install dir that kept its .env but lost the
  # compose file (a cleanup, a partial copy, a hand-seeded .env) went straight to
  # `docker compose -f <missing file> pull` and failed on the file, not on
  # anything the operator could see was missing. Downloading is idempotent.
  [[ -f "${INSTALL_DIR}/${COMPOSE_FILE}" ]] || download_compose
  # Overlays post-date the first release, so an install dir from an older run has
  # the quickstart file and neither overlay. Fetch whichever is missing.
  [[ -f "${INSTALL_DIR}/${BYO_PG_FILE}" ]]    || fetch "${RAW_BASE}/${BYO_PG_FILE}"    "${INSTALL_DIR}/${BYO_PG_FILE}"
  [[ -f "${INSTALL_DIR}/${BYO_KAFKA_FILE}" ]] || fetch "${RAW_BASE}/${BYO_KAFKA_FILE}" "${INSTALL_DIR}/${BYO_KAFKA_FILE}"
  # Not merely "missing", unlike the two above. An install dir from a v0.1.2 run
  # holds the overlay that shipped in that tag, which starts an Ollama and
  # leaves the model pull to the operator; `[[ -f ]]` reads that as done, so the
  # re-run repairs nothing and the operator is back where they started. Key the
  # guard on the thing that was absent instead of on the filename.
  if [[ ! -f "${INSTALL_DIR}/${OLLAMA_FILE}" ]] \
     || ! grep -qE '^[[:space:]]*ollama-pull:' "${INSTALL_DIR}/${OLLAMA_FILE}"; then
    fetch "${OLLAMA_RAW_BASE}/${OLLAMA_FILE}" "${INSTALL_DIR}/${OLLAMA_FILE}"
  fi

  # Record the profile set in the .env, so a compose command typed by hand --
  # without the --profile flags this script passes -- starts the same services
  # the install did. Compose reads .env from the project directory, which for an
  # absolute -f path is the directory holding the compose file, i.e. this one.
  #
  # Appended once and never rewritten, the way INTERNAL_SERVICE_SECRET above is,
  # so an operator who edits the line keeps their edit. The consequence is worth
  # stating rather than hiding: opting out on a RE-RUN (RSYNC_PROFILES=) leaves
  # an earlier `cdc` in the .env, so this run's own compose commands drop the CDC
  # services while a later bare `docker compose up -d` in that directory would
  # still start them. Delete the line to reset it.
  if ! grep -q '^COMPOSE_PROFILES=' "${INSTALL_DIR}/${ENV_FILE}"; then
    echo "COMPOSE_PROFILES=${RSYNC_PROFILES}" >> "${INSTALL_DIR}/${ENV_FILE}"
  fi

  build_compose_args

  pull_images
  start_stack
  # The whole point. wait_healthy now returns non-zero when the stack never came
  # up, and this is the branch that used to not exist: `wait_healthy` was called
  # for its side effects and `print_success` ran unconditionally after it, so
  # every timeout ended with the green banner and a login URL.
  if wait_healthy; then
    print_success
  else
    print_failure
    exit 1
  fi
}

main "$@"
