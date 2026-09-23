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
RSYNC_REF="${RSYNC_REF:-v0.1.5}"
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
# Whether RSYNC_REF names a release tag -- which is the same question as whether
# the ref is allowed to point somewhere new tomorrow. Two decisions read it: the
# version mapping just below, and whether a re-run may trust the compose file
# already on disk (in main(), where a branch that moved is invisible to a
# name-to-name comparison). One function, so the second reader cannot drift from
# the first, and so a guard can execute the real test instead of restating it.
ref_is_release_tag() {
  [[ "$1" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]
}
if ref_is_release_tag "${RSYNC_REF}"; then
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
# Which optional compose profiles this install activates. `cdc` is in the
# default because the change-data-capture services are not an add-on: pick a
# streaming sync in the UI without them and the orchestrator's pre-flight polls
# three absent containers for two minutes and then fails the run with
# "kafka-connect is not reachable" -- a message about a container that was never
# started, on a stack whose install reported success. The profile existed and
# nothing in this script ever activated it, so every install shipped that
# failure.
#
# `-` and not `:-` on purpose: RSYNC_PROFILES= (explicitly empty) is the
# opt-out, and it has to be distinguishable from unset.
RSYNC_PROFILES="${RSYNC_PROFILES-cdc}"
# Set by build_compose_args when the overlay above goes on, and read by
# start_stack, which behaves differently on that path: the first `up` blocks for
# the length of a multi-gigabyte download.
OLLAMA_BUNDLED=0
# Set by check_ports_and_running_install when containers from this install were
# already running before this run, so the banner can leave out the "create the
# admin account" hint on an upgrade of a live install. Keyed on running
# containers, not on "this run wrote the .env": a first run that stopped at a
# port conflict has written the .env, and the re-run after the fix is still the
# first time the stack comes up, so its operator still needs the hint.
STACK_WAS_RUNNING=0
# The compose project name and the host port api-gateway publishes, both read
# out of the rendered compose model by check_ports_and_running_install. Empty
# until then; probe_ready falls back to 5001, the quickstart's own value.
COMPOSE_PROJECT=""
API_HOST_PORT=""
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
# The floor tracks what the install actually starts, the same way the LLM floor
# below does. 6 sized the 18 unprofiled services this file has always started.
# The cdc profile adds three more containers -- one of them a JVM -- and their
# mem_limit lines in docker-compose.quickstart.yml come to 2816MB on top of
# that, so the default set needs 8.
#
# Two floors and not one because RSYNC_PROFILES= starts exactly the 18 services
# 6 was sizing. Warning that operator about a JVM they excluded would be the
# same defect this profile change fixes: a message about a container that was
# never started.
MIN_RAM_GB_BATCH=6
MIN_RAM_GB_CDC=8
MIN_RAM_GB=$MIN_RAM_GB_BATCH
# Padded with spaces so the match is on a whole word: `nocdc` must not select
# the cdc floor. Commas become spaces first -- RSYNC_PROFILES takes either
# separator, and build_compose_args splits it the same way.
case " ${RSYNC_PROFILES//,/ } " in
  *" cdc "*) MIN_RAM_GB=$MIN_RAM_GB_CDC ;;
esac
# What the bundled LLM needs, and not a second opinion on the line above: a 7B
# model at 4-bit is ~5GB resident ON TOP of the stack MIN_RAM_GB sizes, and it
# stays resident for as long as the container runs.
MIN_RAM_GB_WITH_LLM=12
# Written by check_ram so a later check can reuse the reading. 0 means "could
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

check_ram() {
  local ram_gb=0
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    ram_gb=$(awk '/MemTotal/ { printf "%.0f", $2/1024/1024 }' /proc/meminfo)
  elif [[ "$OSTYPE" == "darwin"* ]]; then
    ram_gb=$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))
  fi
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

# The provider menu: OpenAI or any OpenAI-compatible endpoint, Ollama, or none.
# $1 is LLM_PROVIDER lower-cased.
prompt_llm_menu() {
  local provider="$1" key_source
  key_source="$(printf '%s' "${OPENAI_API_KEY_SOURCE:-}" | tr '[:upper:]' '[:lower:]')"
  case "$key_source" in
    ""|env|gcp-metadata) ;;
    *)
      # Not echoed: a key pasted into the wrong variable must not reach the screen.
      warn "OPENAI_API_KEY_SOURCE holds a value this installer does not know; the one supported"
      echo "  value is gcp-metadata. Using OPENAI_API_KEY as the key."
      key_source=""
      ;;
  esac
  [[ "$key_source" == "gcp-metadata" ]] || OPENAI_API_KEY_SOURCE=""
  case "$provider" in
    ""|openai|ollama|none|disabled|off|false|0) ;;
    *)
      # Not echoed, for the same reason.
      warn "LLM_PROVIDER holds a name this installer does not know. The known ones are openai,"
      echo "  groq, azure, ollama and none. For Vertex AI, OpenRouter or any other OpenAI-compatible"
      echo "  endpoint use LLM_PROVIDER=openai with OPENAI_BASE_URL."
      provider=""
      ;;
  esac

  # LLM provider: OpenAI (cloud), Ollama (local, fully offline), or none yet.
  # An LLM is optional. Pipelines parse their intent without one, the Data
  # Explorer runs raw SQL without one, and existing connectors need none; the
  # features that do need a model answer "Set up an LLM first" until one is set.
  echo "  LLM provider:"
  echo "    1) OpenAI  — cloud, needs an API key (best quality)"
  echo "    2) Ollama  — local, fully offline, no key (one is started for you, model included)"
  echo "    3) None    — set up later; pipelines, raw SQL and existing connectors work without one"
  # Without a terminal, an OPENAI_API_KEY in the environment is a clear enough
  # statement of intent to pick provider 1. With nothing set, install without an
  # LLM: bundling Ollama nobody asked for costs a multi-GB model download and
  # several GB of resident RAM, and Ollama is only used when the operator names it.
  # OPENAI_API_KEY_SOURCE=gcp-metadata is the same statement without a key.
  _llm_default=1
  if [[ -z "${OPENAI_API_KEY:-}" && "$provider" != "openai" && "$key_source" != "gcp-metadata" ]] \
    && (( ! TTY_OK )); then
    _llm_default=3
  fi
  # Records that the OPERATOR chose, as opposed to the branch above defaulting
  # because nothing else could work unattended. Only the second case needs to be
  # announced. `|| true` because a failing [[ ]] is the last command in this
  # sequence and `set -e` would take the whole script down with it.
  LLM_PROVIDER_EXPLICIT=0
  [[ "$provider" == "ollama" ]] && { _llm_default=2; LLM_PROVIDER_EXPLICIT=1; } || true
  # The same spellings llm-service reads as "no LLM" (openai_client._NO_LLM_CHOICES).
  case "$provider" in
    none|disabled|off|false|0) _llm_default=3; LLM_PROVIDER_EXPLICIT=1 ;;
  esac
  ask _llm_choice "  Choose [${_llm_default}]: " "$_llm_default"
  # A terminal answer of "2" is also a deliberate choice, not a fallback.
  (( TTY_OK )) && [[ "${_llm_choice:-}" == "2" ]] && LLM_PROVIDER_EXPLICIT=1 || true
  if [[ "${_llm_choice:-1}" == "3" ]]; then
    LLM_PROVIDER="none"
    # Left empty so compose's own defaults apply once a provider is chosen.
    LLM_MODEL=""
    # Empty rather than a host address: if the operator later sets
    # LLM_PROVIDER=ollama and re-runs this installer, an empty OLLAMA_URL is what
    # makes build_compose_args bundle the Ollama overlay.
    OLLAMA_URL=""
    OPENAI_API_KEY=""
    OPENAI_BASE_URL=""
    clear_hosted_llm_settings
    info "No LLM set up. Pipelines, raw SQL in the Data Explorer and existing connectors work without one."
    echo "  Chat beyond pipeline commands, natural-language SQL, pipeline diagnosis and"
    echo "  connector generation will say \"Set up an LLM first\" until you add one:"
    echo "    in ${INSTALL_DIR}/${ENV_FILE} set LLM_PROVIDER=openai and OPENAI_API_KEY=sk-..."
    echo "    (or LLM_PROVIDER=ollama for a local model), then re-run this installer."
    if (( ! TTY_OK )) && [[ "${LLM_PROVIDER_EXPLICIT:-0}" != "1" ]]; then
      warn "No terminal and no OPENAI_API_KEY, so rsync was installed without an LLM."
      echo "  To install with one: OPENAI_API_KEY=sk-... bash, or LLM_PROVIDER=ollama bash"
    fi
  elif [[ "${_llm_choice:-1}" == "2" ]]; then
    LLM_PROVIDER="ollama"
    LLM_MODEL="${LLM_MODEL:-qwen2.5:7b}"
    # The bundled service, not the host. Until docker-compose.ollama.yml existed
    # this defaulted to host.docker.internal, which is the right answer only when
    # the operator already runs an Ollama -- and nothing here started one, so the
    # common case was a stack that came up green and met every prompt with a
    # connection error. An OLLAMA_URL already in the environment still wins, which
    # is how you keep pointing at a host or remote Ollama; build_compose_args
    # reads this value back and layers the overlay only when it names the bundle.
    OLLAMA_URL="${OLLAMA_URL:-http://ollama:11434}"
    OPENAI_API_KEY=""
    # Ollama is addressed by OLLAMA_URL; carrying an OpenAI-compatible base URL
    # into an offline install would only be a live pointer at a cloud endpoint.
    OPENAI_BASE_URL=""
    clear_hosted_llm_settings
    info "Using local Ollama at ${OLLAMA_URL} (model ${LLM_MODEL})."
    # Conditional, because it is only true of an Ollama this script does not
    # start. On the bundled path the overlay's ollama-pull job downloads the
    # model before any service that would ask for it starts, so printing the
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
  else
    LLM_PROVIDER="openai"
    OLLAMA_URL="${OLLAMA_URL:-http://host.docker.internal:11434}"
    # "openai" here names the wire protocol, not the vendor. Vertex AI, Azure,
    # Groq, OpenRouter, Together and vLLM all serve it, and OPENAI_BASE_URL is
    # what points at one of them. Carried through to the generated .env below;
    # empty means api.openai.com, which is the client's own default.
    OPENAI_BASE_URL="${OPENAI_BASE_URL:-}"
    if [[ "$key_source" == "gcp-metadata" ]]; then
      # A Vertex AI access token lasts an hour, so one pasted into OPENAI_API_KEY
      # stops working an hour after the stack starts. llm-service fetches the VM
      # service account's token itself and renews it, and sends it only to
      # googleapis.com; refuse here rather than install a stack that has no key.
      OPENAI_API_KEY_SOURCE="gcp-metadata"
      # No key is asked for, and write_env expands this under set -u.
      OPENAI_API_KEY="${OPENAI_API_KEY:-}"
      if ! base_url_is_googleapis "$OPENAI_BASE_URL"; then
        error "OPENAI_API_KEY_SOURCE=gcp-metadata sends this VM's Google service account token,"
        echo "  so OPENAI_BASE_URL must be an https://*.googleapis.com endpoint. For Vertex AI:" >&2
        echo "    OPENAI_BASE_URL=https://<location>-aiplatform.googleapis.com/v1/projects/<project>/locations/<location>/endpoints/openapi" >&2
        exit 1
      fi
      info "Using this VM's service account token for ${OPENAI_BASE_URL}, renewed before it expires."
    elif [[ -z "${OPENAI_API_KEY:-}" ]]; then
      if (( ! TTY_OK )); then
        error "OpenAI was selected but OPENAI_API_KEY is not set, and there is no"
        echo "  terminal to ask on. Either:" >&2
        echo "    curl -sSL <url> | OPENAI_API_KEY=sk-... bash" >&2
        echo "    curl -sSL <url> | LLM_PROVIDER=ollama bash   # fully offline, no key" >&2
        echo "    curl -sSL <url> | LLM_PROVIDER=none bash     # no LLM; set one up later" >&2
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
          echo "    Or re-run and choose 3 to install without an LLM for now." >&2
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
    # gpt-4o is OpenAI's name for a model. Any other endpoint has its own catalog
    # (Vertex AI google/gemini-2.5-flash, OpenRouter openai/gpt-4o), and asking it
    # for gpt-4o installs a stack whose every LLM call fails. Only the operator
    # knows the right name, so ask for it instead of defaulting.
    if [[ -n "$OPENAI_BASE_URL" ]]; then
      require_llm_value LLM_MODEL "  Model name as ${OPENAI_BASE_URL} lists it: " \
        "LLM_PROVIDER=openai OPENAI_BASE_URL=${OPENAI_BASE_URL} LLM_MODEL=<model name> ..."
    else
      LLM_MODEL="${LLM_MODEL:-gpt-4o}"
    fi
  fi
}

# LLM_PROVIDER=groq or azure, named in the environment. The menu offers neither,
# and these used to go through it: the choice was replaced with "openai" and the
# install asked for an OpenAI key, or, with no terminal and no OPENAI_API_KEY, it
# installed with no LLM at all. Either way the provider named was not the one set up.
prompt_llm_named_provider() {
  local provider="$1"
  LLM_PROVIDER="$provider"
  OPENAI_API_KEY_SOURCE=""
  OLLAMA_URL="${OLLAMA_URL:-http://host.docker.internal:11434}"
  # write_env expands both under set -u, and neither is asked for on this path.
  OPENAI_API_KEY="${OPENAI_API_KEY:-}"
  OPENAI_BASE_URL="${OPENAI_BASE_URL:-}"
  # Left empty unless given: llm-service then uses the provider's own default
  # (llama-3.3-70b-versatile on Groq, the deployment on Azure). gpt-4o, the
  # OpenAI default, is a model neither serves under that name.
  LLM_MODEL="${LLM_MODEL:-}"
  if [[ "$provider" == "groq" ]]; then
    require_llm_value GROQ_API_KEY "  Groq API key (gsk_...): " \
      "LLM_PROVIDER=groq GROQ_API_KEY=gsk_..."
    info "Using Groq (model ${LLM_MODEL:-llama-3.3-70b-versatile})."
    return 0
  fi
  require_llm_value AZURE_OPENAI_ENDPOINT "  Azure OpenAI endpoint (https://<resource>.openai.azure.com): " \
    "LLM_PROVIDER=azure AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com AZURE_OPENAI_API_KEY=..."
  # llm-service falls back to OPENAI_API_KEY for the Azure key, so either one counts.
  if [[ -z "${AZURE_OPENAI_API_KEY:-}" && -n "${OPENAI_API_KEY:-}" ]]; then
    info "Using OPENAI_API_KEY as the Azure OpenAI key."
  else
    require_llm_value AZURE_OPENAI_API_KEY "  Azure OpenAI API key: " \
      "LLM_PROVIDER=azure AZURE_OPENAI_ENDPOINT=... AZURE_OPENAI_API_KEY=..."
  fi
  # An Azure model is a deployment the operator created and named, so no default
  # can be right. With neither this nor LLM_MODEL, llm-service asks Azure for a
  # deployment called gpt-4o-mini.
  if [[ -z "${AZURE_OPENAI_DEPLOYMENT:-}" && -z "$LLM_MODEL" ]]; then
    require_llm_value AZURE_OPENAI_DEPLOYMENT "  Azure OpenAI deployment name: " \
      "LLM_PROVIDER=azure AZURE_OPENAI_ENDPOINT=... AZURE_OPENAI_API_KEY=... AZURE_OPENAI_DEPLOYMENT=<deployment>"
  fi
  info "Using Azure OpenAI (deployment ${LLM_MODEL:-${AZURE_OPENAI_DEPLOYMENT:-}})."
}

# Sets the named variable from the environment or, with a terminal, by asking,
# three tries at most (see the OpenAI key loop for why the bound matters). With
# neither, the install stops and prints $3, the settings that supply the value.
# Values are never echoed back: most of these are keys.
require_llm_value() {
  local var="$1" prompt="$2" example="$3" tries=0
  if [[ -n "${!var:-}" ]]; then
    info "${var} already set in environment"
    return 0
  fi
  if (( ! TTY_OK )); then
    error "LLM_PROVIDER=${LLM_PROVIDER} needs ${var}, which is not set, and there is no"
    echo "  terminal to ask on. Either:" >&2
    echo "    curl -sSL <url> | ${example} bash" >&2
    echo "    curl -sSL <url> | LLM_PROVIDER=none bash     # no LLM; set one up later" >&2
    exit 1
  fi
  while (( tries < 3 )); do
    ask "$var" "$prompt"
    if [[ -n "${!var:-}" ]]; then
      return 0
    fi
    tries=$(( tries + 1 ))
    error "  ${var} is required for LLM_PROVIDER=${LLM_PROVIDER}."
  done
  echo "    Pass it non-interactively, or install with no LLM for now:" >&2
  echo "      curl -sSL <url> | ${example} bash" >&2
  echo "      curl -sSL <url> | LLM_PROVIDER=none bash" >&2
  exit 1
}

# Offline and no-LLM installs carry no hosted-provider settings into the .env,
# for the same reason they carry no OPENAI_API_KEY. A loop, not one NAME="" line
# each: gitleaks' generic-api-key rule reads the next line's name as the key.
clear_hosted_llm_settings() {
  local name
  for name in OPENAI_API_KEY_SOURCE GROQ_API_KEY AZURE_OPENAI_ENDPOINT \
    AZURE_OPENAI_API_KEY AZURE_OPENAI_API_VERSION AZURE_OPENAI_DEPLOYMENT; do
    printf -v "$name" '%s' ""
  done
}

# The host test llm-service applies (openai_client._base_url_is_google): https,
# and a host that is googleapis.com or under it. Userinfo, port, path, query and
# fragment are stripped first, so https://evil.example#.googleapis.com is refused.
base_url_is_googleapis() {
  local url host
  url="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  [[ "$url" == https://* ]] || return 1
  host="${url#https://}"
  host="${host%%[/?#]*}"
  host="${host##*@}"
  host="${host%%:*}"
  [[ "$host" == "googleapis.com" || "$host" == *.googleapis.com ]]
}

prompt_env() {
  section "Configuration"
  echo "  We need a few values to set up rsync.ai."
  echo "  Press Enter to accept defaults where shown."
  echo ""

  if (( ! TTY_OK )); then
    warn "No terminal available — running non-interactively."
    echo "  Values come from the environment; anything unset takes its default."
    echo "  Recognised: OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_API_KEY_SOURCE, LLM_PROVIDER,"
    echo "              LLM_MODEL, OLLAMA_URL, GROQ_API_KEY, AZURE_OPENAI_ENDPOINT,"
    echo "              AZURE_OPENAI_API_KEY, AZURE_OPENAI_API_VERSION, AZURE_OPENAI_DEPLOYMENT,"
    echo "              PUBLIC_HOST, RSYNC_VERSION"
    echo ""
  fi

  # LLM_PROVIDER, lower-cased the way llm-service reads it.
  local _llm_named
  _llm_named="$(printf '%s' "${LLM_PROVIDER:-}" | tr '[:upper:]' '[:lower:]')"
  if [[ "$_llm_named" == "groq" || "$_llm_named" == "azure" ]]; then
    prompt_llm_named_provider "$_llm_named"
  else
    prompt_llm_menu "$_llm_named"
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

  # No admin email question. This used to ask for one, default it to a fixed
  # address, and write it to RSYNC_ADMIN_EMAILS -- a variable no service reads.
  # The admin is whoever creates the FIRST account: the api-gateway signup handler
  # grants role admin while the users table is empty (internal/handlers/auth.go),
  # and the admin routes check that role (admin_middleware.go), never an email
  # list. A question whose answer changes nothing is worse than no question: it
  # hands the operator an address to log in with that no account exists for.

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

# ── LLM key (optional) ────────────────────────────────────────────────────────
# Empty is supported: pipelines, raw SQL and existing connectors need no model.
# Features that do need one answer "Set up an LLM first" until LLM_PROVIDER below
# names a provider that is set up.
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
# There is no admin setting. The first account created in the UI becomes the
# admin, so create yours before anyone else can reach this stack.

# ── LLM ───────────────────────────────────────────────────────────────────────
LLM_PROVIDER=${LLM_PROVIDER}
LLM_MODEL=${LLM_MODEL}
OLLAMA_URL=${OLLAMA_URL}
# Any OpenAI-compatible endpoint: Vertex AI, Azure OpenAI, Groq, OpenRouter,
# Together, a local vLLM. Empty = OpenAI's own api.openai.com. LLM_PROVIDER stays
# "openai" for all of them — it names the wire protocol, not the vendor — and
# LLM_MODEL must then be a name that endpoint's catalog actually has.
OPENAI_BASE_URL=${OPENAI_BASE_URL}
# gcp-metadata = send this VM's Google service account token instead of
# OPENAI_API_KEY, renewed before its hour runs out. Only used when OPENAI_BASE_URL
# is an https://*.googleapis.com endpoint (Vertex AI).
OPENAI_API_KEY_SOURCE=${OPENAI_API_KEY_SOURCE:-}
# LLM_PROVIDER=groq
GROQ_API_KEY=${GROQ_API_KEY:-}
# LLM_PROVIDER=azure. The key falls back to OPENAI_API_KEY, the deployment to
# LLM_MODEL; an empty API version means 2024-10-21.
AZURE_OPENAI_ENDPOINT=${AZURE_OPENAI_ENDPOINT:-}
AZURE_OPENAI_API_KEY=${AZURE_OPENAI_API_KEY:-}
AZURE_OPENAI_API_VERSION=${AZURE_OPENAI_API_VERSION:-}
AZURE_OPENAI_DEPLOYMENT=${AZURE_OPENAI_DEPLOYMENT:-}

# ── Object Storage (internal MinIO) ───────────────────────────────────────────
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY}

# ── OAuth (optional — leave blank to use email login only) ────────────────────
GITHUB_CLIENT_ID=
GITHUB_CLIENT_SECRET=
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=

# ── Failure alerts (optional — but this is how you learn a pipeline broke) ─────
# A pipeline that fails or is stopped always records a notification you can read
# in the UI. Filling either block below also pushes it to your team, which is the
# difference between finding out now and finding out when someone asks where the
# data went.
#
# Slack: one incoming-webhook URL, no app install needed.
#   https://api.slack.com/messaging/webhooks
NOTIFIER_SLACK_WEBHOOK_URL=
# Email: BOTH SMTP_HOST and SMTP_FROM are needed. Setting only the host leaves
# email off and looks exactly like leaving it off on purpose.
SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=
SMTP_FROM=

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
#             ON by default. Absent, a streaming pipeline does not
#             degrade -- the orchestrator's infra pre-flight requires all
#             three services together and fails the run once they do not
#             answer.
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
# Either way the resolved flags are written into the generated compose.sh, so
# re-running compose by hand keeps whatever this install chose.

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

# The `-f` set is a computed thing, and until now it was computed only in here.
# An operator who edited the .env and then ran a bare `docker compose up -d` in
# the install dir got the quickstart file alone: no bring-your-own overlay, no
# bundled Ollama, and no error either -- just a stack quietly missing whatever
# the overlays were adding. Write the resolved command out so re-running with
# the right set is one command, and re-running this installer regenerates it.
write_compose_helper() {
  local helper="${INSTALL_DIR}/compose.sh"
  {
    echo '#!/usr/bin/env bash'
    echo '# Generated by install.sh -- do not edit; edit .env and re-run install.sh.'
    echo '# Runs docker compose with the overlay set your .env selected:'
    echo '#   ./compose.sh ps                ./compose.sh logs -f api-gateway'
    echo '#   ./compose.sh up -d             ./compose.sh down'
    echo 'set -euo pipefail'
    # Relative --env-file below only resolves from here, and the operator is
    # likely to invoke this from anywhere.
    echo 'cd "$(dirname "$0")"'
    # %q, not %s: INSTALL_DIR is operator-supplied through RSYNC_INSTALL_DIR, and
    # a space in it would split one -f path into two arguments in the file we are
    # writing -- a breakage that would surface later, in a shell that is not this
    # one, as compose complaining about a path nobody typed.
    printf 'exec docker compose'
    printf ' %q' "${COMPOSE_ARGS[@]}"
    printf ' --env-file %q "$@"\n' "$ENV_FILE"
  } > "$helper"
  chmod +x "$helper"
  info "Wrote ${helper} — re-runs compose with this .env's overlay set."
}

# Which -f files this install actually runs with. Keyed off the .env on disk
# rather than the sourced shell variables, so an unrelated POSTGRES_HOST already
# exported in the operator's environment cannot silently disable the bundled
# database. Each BRING-YOUR-OWN overlay parks its bundled service in a profile
# that is never activated; the base file carries the matching `required: false`
# on every depends_on, without which Compose refuses the whole project. The LLM
# overlay is the other shape -- it parks nothing and adds two services -- so the
# `required: false` reasoning above does not carry over to it, and must not: its
# depends_on is a completion gate that has to be able to fail.
build_compose_args() {
  COMPOSE_ARGS=( -f "${INSTALL_DIR}/${COMPOSE_FILE}" )
  # Unquoted on purpose -- this is the word split that turns "cdc,generate" into
  # two flags. An empty RSYNC_PROFILES yields zero iterations, which is the
  # opt-out. `--profile` rather than exporting COMPOSE_PROFILES because these
  # flags are also what write_compose_helper bakes into compose.sh: an exported
  # variable lives in this process and is gone by the time the operator runs the
  # helper, and the helper is the whole point of writing it.
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
  # Ollama. Every .env written before this overlay existed carries
  # OLLAMA_URL=http://host.docker.internal:11434 -- an Ollama on the operator's
  # own machine. Layering the overlay on one of those would start a second,
  # empty server that nothing talks to, download several GB into it, and leave
  # the operator's own Ollama serving exactly as before. So it goes on only when
  # the URL names the bundled service, or is blank -- which write_env never
  # writes but a hand-edited file can.
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
  elif [[ "$llm" == "none" ]]; then
    info "No LLM configured — the stack runs without one; no LLM container bundled."
  fi
  COMPOSE_CMD="docker compose $(printf '%s ' "${COMPOSE_ARGS[@]}")"
  write_compose_helper
}

# ─── Checks before anything starts ───────────────────────────────────────────
#
# Both run after build_compose_args, on every run, and before the first pull. A
# re-run used to check nothing. An .env from an older release that lacked a
# variable the new compose file requires got as far as `docker compose pull` and
# died on compose's own interpolation error; a port some other program held got
# as far as `up -d` and died on "address already in use" with part of the stack
# already recreated. Both are cheaper to find while nothing has changed.

# Secrets a check may GENERATE into an existing .env when the compose file
# requires one the .env lacks. The test for membership: the value lives only in
# the .env and in containers started from it, every reader takes the new value
# when compose recreates them, and nothing persisted was written under the old
# one.
#   INTERNAL_SERVICE_SECRET  service-to-service header, compared per request
#   JWT_SECRET               a startup guard only; sessions are opaque DB tokens
#   REDIS_PASSWORD           written to redis.conf at every container start; the
#                            data file carries no password
#   MINIO_ACCESS_KEY/SECRET  MinIO's root login, read at start by the server and
#                            by every client from this same .env
# Deliberately NOT in the list, and reported instead:
#   POSTGRES_PASSWORD  the database volume was created with the old one; a new
#                      value locks every service out of the existing data (and
#                      for bring-your-own Postgres it is someone else's password)
#   ENCRYPTION_KEY     every saved connection credential is encrypted under the
#                      old one; a new value makes all of them unreadable
ENV_BACKFILLABLE_SECRETS="INTERNAL_SERVICE_SECRET JWT_SECRET REDIS_PASSWORD MINIO_ACCESS_KEY MINIO_SECRET_KEY"

# Every ${VAR:?} in the -f files this install runs with, one per line. The same
# extraction as scripts/check-env-templates.sh required_vars(). A static grep is
# the right reading, not an approximation: compose interpolates each whole file
# before it applies profiles, so a `:?` inside a service no profile starts still
# stops the render.
compose_required_vars() {
  local files=() i=0
  while (( i < ${#COMPOSE_ARGS[@]} )); do
    if [[ "${COMPOSE_ARGS[$i]}" == "-f" ]] && (( i + 1 < ${#COMPOSE_ARGS[@]} )); then
      files+=( "${COMPOSE_ARGS[$((i + 1))]}" )
    fi
    i=$(( i + 1 ))
  done
  # Guarded: "${files[@]}" on an empty array is an unbound variable under
  # `set -u` on bash 3.2, which is what a stock Mac runs this script with.
  (( ${#files[@]} > 0 )) || return 0
  { grep -ohE '\$\{[A-Z_][A-Z0-9_]*:\?' "${files[@]}" 2>/dev/null || true; } \
    | sed 's/^\${//; s/:?$//' | sort -u
}

# Whether $1 is exported into this process with a non-empty value. compose reads
# the process environment ahead of --env-file, so such a value satisfies `:?` for
# this run. `declare -p` rather than env or printenv, so the value is compared
# here and never printed.
var_is_exported_nonempty() {
  local decl="" flags=""
  decl=$(declare -p "$1" 2>/dev/null) || return 1
  flags=${decl#declare -}
  flags=${flags%% *}
  [[ "$flags" == *x* && -n "${!1:-}" ]]
}

# Report every variable the compose files require that the .env does not set.
# Generates the ones ENV_BACKFILLABLE_SECRETS allows; stops, naming the rest.
# Never replaces a value that is already there. An EMPTY line (`JWT_SECRET=`) is
# treated as missing, because `:?` rejects empty exactly as it rejects unset.
check_env_required() {
  local envf="${INSTALL_DIR}/${ENV_FILE}" required="" v="" secret="" total=0
  local fill="" blocked="" env_only=""
  required=$(compose_required_vars)
  for v in $required; do
    total=$(( total + 1 ))
    [[ -z "$(env_value "$v")" ]] || continue
    if var_is_exported_nonempty "$v"; then
      env_only="${env_only} ${v}"
      continue
    fi
    case " ${ENV_BACKFILLABLE_SECRETS} " in
      *" ${v} "*) fill="${fill} ${v}" ;;
      *)          blocked="${blocked} ${v}" ;;
    esac
  done

  if (( total == 0 )); then
    warn "Found no required variables in the compose files, so the .env was not checked against them."
    return 0
  fi

  # Stop BEFORE generating anything, so a run that is going to exit adds no
  # secrets the operator did not see land; the re-run after the fix fills them.
  if [[ -n "$blocked" ]]; then
    error "${envf} is missing variables this version's compose file requires:"
    for v in $blocked; do
      case "$v" in
        POSTGRES_PASSWORD)
          echo "    ${v} — set it to the password your database was first created with." >&2
          echo "      Do not invent a new one: the existing database would reject every service." >&2 ;;
        ENCRYPTION_KEY)
          echo "    ${v} — set it to the key this install has always used." >&2
          echo "      A new key makes every saved connection credential unreadable." >&2 ;;
        *)
          echo "    ${v} — no safe default exists; docs/deployment/env-vars.md says what it holds." >&2 ;;
      esac
    done
    echo "  Add each one to ${envf} as a NAME=value line, then re-run this installer." >&2
    echo "  Nothing has been pulled or started, and this check wrote nothing to the .env." >&2
    exit 1
  fi

  for v in $env_only; do
    warn "${v} is set in this shell's environment but not in ${envf}."
    echo "  This run uses it; ${INSTALL_DIR}/compose.sh run later from another shell will not."
    echo "  Add it to ${envf} to make it permanent."
  done

  for v in $fill; do
    # Into a variable first, for the reason the INTERNAL_SERVICE_SECRET backfill
    # in main spells out: inside a command substitution, generate_secret's
    # `exit 1` would leave an empty value behind instead of stopping the run.
    secret=$(generate_secret)
    set_env_value "$v" "$secret"
    warn "Added a generated ${v} to ${envf}: this version requires it and the file had no value."
  done

  info "All ${total} variables the compose files require are set in ${envf}."
}

# The published host ports in a rendered compose model (`docker compose config`
# on stdin), as tab-separated lines:
#   project  <name>
#   port     <service>  <host_ip>  <published>  <target>
# Read from the RENDERED model rather than the raw YAML, so the ports are the
# ones `up` will bind: overlays merged, ${RSYNC_BIND_ADDR} resolved from the
# .env, services outside the active profiles gone. That model carries every
# secret in the .env, so only these fields leave this function; it is never
# printed or written to disk. The shape parsed here is compose v2's normalised
# output -- two-space service keys, and each port as a `- mode:` item with
# host_ip, target, published and protocol under it.
compose_published_ports() {
  # Each item is printed when it ENDS -- at the next item, the next key, the next
  # service or the end of input -- so the fields can come in any order.
  awk '
    function flush() {
      # host_ip is absent when a mapping names no address, which binds every
      # interface. Written out as 0.0.0.0 rather than left empty: the reader
      # splits on tabs, and bash `read` collapses an empty tab-separated field.
      if (pub ~ /^[0-9]+$/) print "port\t" svc "\t" (hip == "" ? "0.0.0.0" : hip) "\t" pub "\t" (tgt == "" ? "?" : tgt)
      hip = ""; tgt = ""; pub = ""
    }
    /^name:/ { n = $0; sub(/^name:[[:space:]]*/, "", n); gsub(/"/, "", n); print "project\t" n; next }
    /^services:/ { insvc = 1; next }
    /^[^[:space:]]/ { flush(); insvc = 0; inports = 0; next }
    !insvc { next }
    /^  [^[:space:]#][^:]*:[[:space:]]*$/ {
      flush(); svc = $0; sub(/^  /, "", svc); sub(/:[[:space:]]*$/, "", svc); inports = 0; next
    }
    /^    [^[:space:]]/ { flush(); inports = ($0 ~ /^    ports:[[:space:]]*$/); next }
    !inports { next }
    /^      - / { flush() }
    {
      kv = $0; sub(/^[[:space:]]*(- )?/, "", kv)
      k = kv; sub(/:.*/, "", k)
      val = kv; sub(/^[^:]*:[[:space:]]*/, "", val); gsub(/"/, "", val)
      if (k == "host_ip") hip = val
      else if (k == "target") tgt = val
      else if (k == "published") pub = val
    }
    END { flush() }
  '
}

# Every running container: name, compose project, compose project dir, ports.
running_containers() {
  docker ps --format '{{.Names}}\t{{.Label "com.docker.compose.project"}}\t{{.Label "com.docker.compose.project.working_dir"}}\t{{.Ports}}' 2>/dev/null || true
}

# The containers in $1 (running_containers output) that publish host TCP port
# $2, as "name<TAB>project" lines. Docker's Ports column reads like
# `127.0.0.1:5001->8080/tcp, [::]:5001->8080/tcp`, with a range as 8000-8010.
containers_on_port() {
  printf '%s\n' "$1" | awk -F'\t' -v p="$2" '
    {
      n = split($4, parts, ", ")
      for (i = 1; i <= n; i++) {
        e = parts[i]
        if (e !~ /->/ || e !~ /\/tcp$/) continue
        sub(/->.*/, "", e); sub(/.*:/, "", e)
        lo = e; hi = e
        if (e ~ /-/) { sub(/-.*/, "", lo); sub(/.*-/, "", hi) }
        if (p + 0 >= lo + 0 && p + 0 <= hi + 0) { print $1 "\t" $2; break }
      }
    }'
}

# What is listening on TCP port $1. First line: the tool that answered. Then the
# local address of each listener, one per line.
#
# ss (Linux) and netstat (macOS, and Linux with net-tools) come first because
# they list every process's sockets without root. lsof comes after them because
# without root it sees only this user's processes, so its silence proves little;
# it is used when it DOES find something. A bash /dev/tcp connect is the last
# resort, needs no tool at all, and sees only the address it dials ($2). No
# branch can fail the script: a missing tool falls through to the next.
port_listeners() {
  local port=$1 dial=${2:-127.0.0.1} out=""
  if command -v ss >/dev/null 2>&1 && out=$(ss -tln 2>/dev/null); then
    echo "ss"
    printf '%s\n' "$out" | awk -v p="$port" '
      $1 == "LISTEN" && $4 ~ ("[.:]" p "$") { print substr($4, 1, length($4) - length(p) - 1) }'
    return 0
  fi
  if command -v netstat >/dev/null 2>&1 && out=$(netstat -an 2>/dev/null); then
    echo "netstat"
    printf '%s\n' "$out" | awk -v p="$port" '
      $1 ~ /^tcp/ && $NF ~ /^LISTEN/ && $4 ~ ("[.:]" p "$") { print substr($4, 1, length($4) - length(p) - 1) }'
    return 0
  fi
  if command -v lsof >/dev/null 2>&1 && out=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null); then
    echo "lsof"
    printf '%s\n' "$out" | awk -v p="$port" '
      NR > 1 { for (i = 1; i <= NF; i++) if ($i ~ ("[.:]" p "$")) print substr($i, 1, length($i) - length(p) - 1) }'
    return 0
  fi
  echo "bash /dev/tcp"
  # The redirect opens the connection and `:` closes it at once. A refused
  # connect on a local address returns immediately.
  if (: <>"/dev/tcp/${dial}/${port}") 2>/dev/null; then
    echo "$dial"
  fi
  return 0
}

# Whether a listener on local address $1 stops Docker from binding $2 on the
# same port. A wildcard on either side collides with everything; two specific
# addresses collide only when they are the same one.
addr_collides() {
  local l=$1 b=$2
  l=${l#[}; l=${l%]}; l=${l%%%*}
  case "$l" in ""|"*"|0.0.0.0|::|::ffff:0.0.0.0) return 0 ;; esac
  case "$b" in ""|0.0.0.0|::) return 0 ;; esac
  [[ "$l" == "$b" || "$l" == "::ffff:${b}" ]]
}

# "command (pid N)" for the process listening on port $1, or nothing when this
# user cannot see it -- lsof and ss -p both need root for another user's process.
port_process_name() {
  local port=$1 out=""
  if command -v lsof >/dev/null 2>&1; then
    out=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR == 2 { print $1 " (pid " $2 ")" }' || true)
  fi
  if [[ -z "$out" ]] && command -v ss >/dev/null 2>&1; then
    out=$(ss -tlnp 2>/dev/null | awk -v p="$port" '
      $4 ~ ("[.:]" p "$") && match($0, /users:\(\("[^"]*",pid=[0-9]+/) {
        s = substr($0, RSTART + 9, RLENGTH - 9); name = s; sub(/".*/, "", name)
        pid = s; sub(/.*pid=/, "", pid); print name " (pid " pid ")"; exit
      }' || true)
  fi
  printf '%s' "$out"
}

# Announces an install that is already running, then checks every host port the
# compose model publishes. Ports held by this install's own containers are fine:
# compose releases each one when it recreates the container that holds it. Any
# other holder -- another container, or a process -- stops the run here, before
# a pull, naming the port, the holder, and the ways out.
check_ports_and_running_install() {
  local envf="${INSTALL_DIR}/${ENV_FILE}" plan="" cfg_rc=0 containers="" ours="" count=0
  plan=$(docker compose "${COMPOSE_ARGS[@]}" --env-file "$envf" config 2>/dev/null | compose_published_ports) || cfg_rc=$?
  if (( cfg_rc != 0 )); then
    # Not fatal on its own. The pull right after this renders the same model and
    # fails with compose's full message; this check has nothing to add to that.
    warn "Could not read the compose files' ports, so the port check was skipped. Compose said:"
    # Cut at the first quote. Compose's complaint about a malformed .env line
    # quotes that line, value and all (`unterminated quoted value "...`,
    # `Invalid template: "${...`), and the .env holds every secret. What stays is
    # the file, the line number and the kind of error, which is enough to find it.
    { docker compose "${COMPOSE_ARGS[@]}" --env-file "$envf" config 2>&1 >/dev/null || true; } \
      | head -3 | sed -e "s/[\"'].*\$/[rest hidden: it can quote a line of the .env]/" -e 's/^/    /'
    return 0
  fi
  COMPOSE_PROJECT=$(printf '%s\n' "$plan" | awk -F'\t' '$1 == "project" { print $2; exit }')
  API_HOST_PORT=$(printf '%s\n' "$plan" | awk -F'\t' '$1 == "port" && $2 == "api-gateway" { print $4; exit }')
  containers=$(running_containers)

  if [[ -n "$COMPOSE_PROJECT" ]]; then
    ours=$(printf '%s\n' "$containers" | awk -F'\t' -v p="$COMPOSE_PROJECT" '$1 != "" && $2 == p' || true)
  fi
  if [[ -n "$ours" ]]; then
    STACK_WAS_RUNNING=1
    count=$(printf '%s\n' "$ours" | awk 'END { print NR }')
    info "Found ${count} running containers from an existing rsync.ai install (compose project ${COMPOSE_PROJECT})."
    echo "  This run upgrades them in place: containers whose image or settings changed are"
    echo "  recreated, the rest keep running, and the data volumes are kept."
    # Compose knows a project only by its name, and the dev compose file in the
    # source tree uses the same one. Containers started from somewhere else are
    # still "ours" to compose and will be taken over; say so, because the
    # operator may not mean it.
    local here="" here_phys="" elsewhere=""
    here=$(cd "$INSTALL_DIR" 2>/dev/null && pwd) || here="$INSTALL_DIR"
    here_phys=$(cd "$INSTALL_DIR" 2>/dev/null && pwd -P) || here_phys="$here"
    elsewhere=$(printf '%s\n' "$ours" | awk -F'\t' -v a="$here" -v b="$here_phys" \
      '$3 != "" && $3 != a && $3 != b { print $3 }' | sort -u || true)
    if [[ -n "$elsewhere" ]]; then
      warn "Some of them were started from a different directory:"
      printf '%s\n' "$elsewhere" | sed 's/^/    /'
      echo "  Compose treats every stack named ${COMPOSE_PROJECT} as one, so this run recreates"
      echo "  those containers from ${INSTALL_DIR} with its .env. If that stack is not this"
      echo "  install (for example the development stack of a source checkout, which uses the"
      echo "  same name), stop it first (from its own directory: docker compose stop) and re-run."
    fi
  fi

  local kind="" svc="" hip="" port="" tgt="" mine="" other="" seen="" tool="" addrs="" addr=""
  local holder="" collides=0 checked=0 conflicts=""
  while IFS=$'\t' read -r kind svc hip port tgt <&4; do
    [[ "$kind" == "port" ]] || continue
    checked=$(( checked + 1 ))
    mine=$(containers_on_port "$containers" "$port" | awk -F'\t' -v p="$COMPOSE_PROJECT" 'p != "" && $2 == p { print $1; exit }')
    if [[ -n "$mine" ]]; then
      info "Port ${port} (${svc}) is held by this install's own ${mine}; it is handed over when that container is recreated."
      continue
    fi
    other=$(containers_on_port "$containers" "$port" | awk -F'\t' '{ print; exit }')
    holder=""
    if [[ -n "$other" ]]; then
      holder="Docker container ${other%%$'\t'*}"
      [[ -z "${other#*$'\t'}" ]] || holder="${holder} (compose project ${other#*$'\t'})"
    else
      # Dial the bind address itself when it is a specific one; loopback stands
      # in for "all interfaces", where any listener would collide anyway.
      case "$hip" in ""|0.0.0.0|::) seen=$(port_listeners "$port" 127.0.0.1) ;;
                     *)             seen=$(port_listeners "$port" "$hip") ;;
      esac
      tool=${seen%%$'\n'*}
      addrs=""
      [[ "$seen" != *$'\n'* ]] || addrs=${seen#*$'\n'}
      collides=0
      while IFS= read -r addr; do
        if [[ -n "$addr" ]] && addr_collides "$addr" "$hip"; then collides=1; fi
      done <<< "$addrs"
      if (( collides )); then
        holder=$(port_process_name "$port")
        [[ -n "$holder" ]] || holder="a process this user cannot identify (seen by ${tool}; find it with: sudo lsof -nP -iTCP:${port} -sTCP:LISTEN)"
      fi
    fi
    if [[ -n "$holder" ]]; then
      conflicts="${conflicts}${port}"$'\t'"${svc}"$'\t'"${hip:-0.0.0.0}"$'\t'"${tgt}"$'\t'"${holder}"$'\n'
    fi
  done 4<<< "$plan"

  if (( checked == 0 )); then
    warn "The compose files publish no host ports, so there were none to check."
    return 0
  fi
  if [[ -z "$conflicts" ]]; then
    info "All ${checked} host ports rsync.ai publishes are free or already held by this install."
    return 0
  fi

  local cname=""
  while IFS=$'\t' read -r port svc hip tgt holder; do
    [[ -n "$port" ]] || continue
    error "Port ${port} is already in use, and rsync.ai's ${svc} needs it (${hip}:${port})."
    echo "    Held by: ${holder}" >&2
    if [[ "$holder" == "Docker container "* ]]; then
      cname=${holder#Docker container }
      cname=${cname%% *}
      echo "    To free it: docker stop ${cname}" >&2
    fi
  done <<< "$conflicts"
  echo "  To free a port: stop the container or process holding it, then re-run this installer." >&2
  echo "  To use another port instead: in ${INSTALL_DIR}/${COMPOSE_FILE}, change the host side" >&2
  echo "  (the number before the last colon) of that service's ports line, change the same port" >&2
  echo "  in the URLs in ${envf}, and re-run. An upgrade downloads that file again, so" >&2
  echo "  repeat the edit after one (the old copy is kept next to it as .previous)." >&2
  if [[ "${RSYNC_SKIP_PORT_CHECK:-}" == "1" ]]; then
    warn "Continuing anyway because RSYNC_SKIP_PORT_CHECK=1. Starting the stack will fail if the port really is taken."
    return 0
  fi
  echo "  If you are sure the port is free, run the installer again the same way, with" >&2
  echo "  RSYNC_SKIP_PORT_CHECK=1 on the bash side of the pipe:" >&2
  echo "    curl -sSL ${RAW_BASE}/install.sh | RSYNC_SKIP_PORT_CHECK=1 bash" >&2
  echo "  Nothing has been pulled or started." >&2
  exit 1
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
  #
  # The port is the one the compose model publishes for api-gateway, not a
  # literal, so an operator who moved it off a busy 5001 (the port check's own
  # advice) is probed where the gateway actually listens.
  out=$(curl -s --max-time 5 -w '\n%{http_code}' "http://localhost:${API_HOST_PORT:-5001}/ready" 2>/dev/null) || return 1
  code="${out##*$'\n'}"
  case "$code" in
    200) return 0 ;;
    404|405)
      # An image predating /ready. Fall back to liveness so this installer still
      # works against an older pinned RSYNC_VERSION, and say that it did.
      READY_REASON="this image has no /ready endpoint; fell back to /health"
      if ! curl -sf --max-time 5 "http://localhost:${API_HOST_PORT:-5001}/health" >/dev/null 2>&1; then
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
  # Left out only when this install's containers were already running before
  # this run: that is an upgrade of a live install, whose admin almost always
  # exists. Every other success is, or may be, the first time this stack has
  # served the UI -- including the re-run after a first run that stopped at the
  # port check -- and there the hint is the whole point.
  if (( ! STACK_WAS_RUNNING )); then
    echo -e "  ${BOLD}Admin:${NC}            the first account you sign up with becomes the admin — if this install has none yet, create it now"
  fi
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
  # `export KEY=value` is a line compose's env-file parser accepts and reads as
  # KEY, so it is read here too. Missing it made a set value look absent, and the
  # checks that act on "absent" then either appended a second, later line that
  # overrode the operator's value, or refused to start over a key that was there.
  line=$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$file" | tail -1 || true)
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
  # An `export KEY=` line is the same key (see env_value) and keeps its prefix.
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

main() {
  setup_tty
  banner
  section "Pre-flight checks"
  check_docker
  check_ram
  # Documented by earlier versions of this script as a recognised variable, so
  # an automated install may still pass it. Say it does nothing rather than
  # accept it silently.
  if [[ -n "${ADMIN_EMAIL:-}" ]]; then
    warn "ADMIN_EMAIL is set but no longer used: the first account created in the UI becomes the admin."
  fi

  # If .env already exists in install dir, skip prompts
  if [[ -f "${INSTALL_DIR}/${ENV_FILE}" ]]; then
    warn "Existing .env found at ${INSTALL_DIR}/${ENV_FILE} — using it."
    warn "Delete it to reconfigure."
    # Read, never source. See env_value's comment for what sourcing this file does.
    NEXTAUTH_URL="$(env_value NEXTAUTH_URL)"
    NEXTAUTH_URL="${NEXTAUTH_URL:-http://localhost:3000}"
    # wait_healthy's public-URL probe keys on this; it is written by write_env.
    RSYNC_BIND_ADDR="$(env_value RSYNC_BIND_ADDR)"
    # Backfill INTERNAL_SERVICE_SECRET into .env files created before it existed
    # (its absence 503s internal OAuth-refresh). Keep any existing value, however
    # it is written (`export` included); fill an empty line where it stands.
    if [[ -z "$(env_value INTERNAL_SERVICE_SECRET)" ]]; then
      # Into a variable FIRST. Written as `echo "K=$(generate_secret)" >> file`
      # the failure sits inside a command substitution, where `exit 1` exits only
      # the subshell: the redirect still succeeds, an EMPTY value is appended,
      # and the "Backfilled" line below prints over it. As a plain assignment the
      # same failure aborts under `set -e`.
      local backfilled_secret
      backfilled_secret=$(generate_secret)
      set_env_value INTERNAL_SERVICE_SECRET "$backfilled_secret"
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
    # Two questions, not one: has the ref CHANGED, and can the ref MOVE. The
    # second was missing, and it is the only one a branch-tracking install can
    # answer. A release tag is fixed, so recorded == requested genuinely means
    # there is nothing to fetch. `main` is not fixed, and it moves under a name
    # that never changes -- so this comparison, which is name-to-name, read
    # "main" against "main", skipped the whole block, and left the `[[ -f ]]`
    # repair below to keep the compose file the FIRST run downloaded, for the
    # life of the install directory. Measured on the deploy host: a re-run 27
    # hours after the first install re-read that first file and died pulling an
    # image the branch had already moved off -- with the fix for that image
    # sitting in main, fetched by nothing.
    if [[ "$recorded_ref" != "$RSYNC_REF" ]] || ! ref_is_release_tag "$RSYNC_REF"; then
      if [[ "$recorded_ref" == "$RSYNC_REF" ]]; then
        info "Tracking ${RSYNC_REF}, which moves -- re-fetching compose files in case it has."
      elif [[ -n "$recorded_ref" ]]; then
        info "Installed at ref ${recorded_ref}, ${RSYNC_REF} requested -- refreshing compose files and image tag."
      else
        info "This install predates ref tracking -- adopting ${RSYNC_REF} and refreshing compose files and image tag."
      fi
      # The compose file is the only artifact here an operator may have edited
      # by hand, and this is the one path that overwrites it. Keep the outgoing
      # copy so the edit is recoverable rather than merely lost -- but keep it
      # only when the download actually changed something. This block now runs
      # on EVERY re-run of a branch-tracking install, so rotating
      # unconditionally would replace that backup with an identical copy on the
      # first re-run that fetched nothing new, losing the one edit it exists to
      # protect.
      local outgoing="${INSTALL_DIR}/${COMPOSE_FILE}.outgoing"
      # A crash between the copy and the compare leaves this behind, and a stale
      # one would read as this run's snapshot of a compose file that is missing.
      rm -f "$outgoing"
      if [[ -f "${INSTALL_DIR}/${COMPOSE_FILE}" ]]; then
        cp "${INSTALL_DIR}/${COMPOSE_FILE}" "$outgoing"
      fi
      download_compose
      if [[ -f "$outgoing" ]]; then
        # $(<file) rather than cmp: this installer's dependencies are curl,
        # docker and coreutils, and cmp ships in diffutils, which a minimal
        # host need not have. Both sides lose trailing newlines identically.
        if [[ "$(<"$outgoing")" == "$(<"${INSTALL_DIR}/${COMPOSE_FILE}")" ]]; then
          rm -f "$outgoing"
        else
          mv "$outgoing" "${INSTALL_DIR}/${COMPOSE_FILE}.previous"
        fi
      fi
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

  build_compose_args

  # Both before the first pull, so a run that stops here has pulled no image and
  # recreated no container. It may already have changed files: an upgrade run has
  # fetched the new compose file (the old one kept as .previous) and rewritten
  # RSYNC_VERSION and RSYNC_INSTALLED_REF above, and check_env_required writes the
  # secrets it can generate before the port check runs. check_env_required never
  # writes when it stops. The .env check goes first because rendering the compose
  # model -- which the port check reads -- itself fails on a missing required
  # variable.
  section "Checking settings and ports"
  check_env_required
  check_ports_and_running_install

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
