#!/usr/bin/env python3
"""Kafka security matrix: every rsync Kafka client path against a real broker.

    python3 deploy/helm/rsync-ai/test/kind/kafka-matrix/run.py [--rows REGEX] [--keep]

One throwaway KRaft broker exposes one listener per security mode (PLAINTEXT,
TLS, SASL PLAIN/SCRAM/OAUTHBEARER over TLS, mutual TLS) next to a test OIDC
provider. Two matrices run against it:

CLIENT rows -- one container per (row, runtime), configured by environment only,
exactly as a service is:
  go-sarama / go-kafkago  shared/go/kafkaclient (FromEnvForService -> saramaauth / kgoauth)
  python                  llm-service/src/utils/kafka_security.py + kafka-python
  jvm                     the Debezium connector's schema-history builder
                          (_schema_history_security) -> the kafka-connect image's
                          own kafka-clients

CONNECT rows -- the real kafka-connect image, its env rendered from
docker-compose.yml (`docker compose config`, hermetic), per row:
  worker   the worker boots and joins its group
  tasks    the producer./consumer. clients the worker hands to tasks round-trip
  history  the schema-history props debezium-mcp would emit round-trip, run
           INSIDE that worker (the paths they name must exist there)
docker-compose.quickstart.yml must render the same security env for both
services; a difference is reported by variable NAME only.

Every row that must fail asserts WHY it failed (a regex over the error), so a
row that "fails" for an unrelated reason -- a typo, a dead broker -- is a
mismatch, not a pass.

Isolation: every container, the network and the images carry the label
rsync.kmatrix=<prefix> and a unique name prefix; they are removed on exit
(--keep leaves them for debugging). Credentials are random per run, written only
to chmod-600 env files passed with --env-file, and redacted from all output.

Needs: docker (with compose v2), go, git. Exit 0 = every cell as expected,
1 = a mismatch, 2 = the harness itself could not run.
"""
import argparse
import concurrent.futures
import json
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=HERE, check=True,
                      capture_output=True, text=True).stdout.strip()
# CI sets KMX_PREFIX so its `if: always()` step can remove exactly this run's
# resources when the job is killed before cleanup() runs.
PFX = os.environ.get("KMX_PREFIX") or f"kmx-{time.strftime('%m%d%H%M%S')}-{os.getpid()}"
if not re.fullmatch(r"kmx-[a-z0-9][a-z0-9-]{0,40}", PFX):
    sys.exit(f"harness: KMX_PREFIX={PFX!r} must match kmx-[a-z0-9-]+ (it names containers and images)")
LABEL = f"rsync.kmatrix={PFX}"
NET = f"{PFX}-net"
BROKER = f"{PFX}-kafka"
WRONG_HOST = f"{PFX}-wrongname"  # a second DNS name for the broker, NOT in its SAN
OIDC = f"{PFX}-oidc"
IMG_CONNECT, IMG_OIDC, IMG_PY = f"{PFX}-connect", f"{PFX}-oidc", f"{PFX}-py"
BROKER_IMAGE = "apache/kafka:3.7.0"  # same broker as ../broker-up.sh
RUNTIMES = ("go-sarama", "go-kafkago", "python", "jvm")
CELL_TIMEOUT = 150
WORKER_TIMEOUT = 180
WORKER_READY = "Finished starting connectors and tasks"
JAVA = ["java", "-Dlog4j.configuration=file:/kmx/log4j.properties", "-cp", "/kafka/libs/*",
        "/kmx/RoundTrip.java"]

W = PKI = ""  # the per-run work dir (0700) and its pki/; set in main()
SECRET = {k: secrets.token_hex(16) for k in ("PLAIN_PW", "SCRAM_PW", "OIDC_SECRET")}


# ---------------------------------------------------------------- helpers

def redact(text):
    for value in SECRET.values():
        text = text.replace(value, "***")
    return text


def run(args, *, timeout=None, input=None, check=False, env=None):
    """Run a command. Never echoes args: none carry secrets, but output might."""
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                              input=input, check=check, env=env)
    except subprocess.CalledProcessError as e:
        tail = redact((e.stdout or "") + (e.stderr or ""))[-3000:]
        raise SystemExit(f"harness: `{' '.join(args[:4])} ...` failed (rc={e.returncode}):\n{tail}")


def docker(*args, **kw):
    return run(["docker", *args], **kw)


def write_env(path, env):
    """A docker --env-file: one KEY=VALUE per line, value taken literally."""
    bad = [k for k, v in env.items() if "\n" in str(v) or "\r" in str(v)]
    if bad:
        raise SystemExit(f"harness: env var(s) {bad} contain a newline; --env-file cannot carry them")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        for key, value in env.items():
            fh.write(f"{key}={value}\n")
    return path


def result_line(text):
    for line in reversed(text.splitlines()):
        if line.startswith("RESULT "):
            parts = line.split(" ", 3)
            return parts[1], parts[3] if len(parts) > 3 else ""
    return None


def cleanup(keep):
    if keep:
        print(f"--keep: left containers/network/images labelled {LABEL} and {W}")
        return
    ids = docker("ps", "-aq", "--filter", f"label={LABEL}").stdout.split()
    if ids:
        docker("rm", "-f", *ids)
    docker("network", "rm", NET)
    docker("rmi", "-f", IMG_CONNECT, IMG_OIDC, IMG_PY)
    shutil.rmtree(W, ignore_errors=True)


# ---------------------------------------------------------------- setup

def build():
    print(f"[{PFX}] building images and the Go probe")
    arch = docker("version", "--format", "{{.Server.Arch}}", check=True).stdout.strip()
    go = subprocess.run(["go", "build", "-o", os.path.join(W, "kmatrix-probe"), "./cmd/kmatrix-probe"],
                        cwd=os.path.join(ROOT, "shared/go/kafkaclient"), capture_output=True, text=True,
                        env={**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": arch})
    if go.returncode != 0:
        raise SystemExit(f"harness: building kmatrix-probe failed:\n{go.stderr[-3000:]}")

    # The Python image takes its base and pins from the services under test.
    with open(os.path.join(ROOT, "llm-service/Dockerfile")) as fh:
        base = next(line.split()[1] for line in fh if line.startswith("FROM "))
    kafka_python = _pin(os.path.join(ROOT, "llm-service/requirements.txt"), "kafka-python")
    httpx = _pin(os.path.join(connector_dir(), "requirements.txt"), "httpx")

    jobs = {
        IMG_CONNECT: ["build", "--label", LABEL, "-t", IMG_CONNECT,
                      os.path.join(ROOT, "shared/internal/infra/kafka-connect")],
        IMG_OIDC: ["build", "--label", LABEL, "-t", IMG_OIDC, os.path.join(HERE, "..", "oidc")],
        IMG_PY: ["build", "--label", LABEL, "-t", IMG_PY, "-f", os.path.join(HERE, "py.Dockerfile"),
                 "--build-arg", f"PYTHON_BASE={base}", "--build-arg", f"KAFKA_PYTHON_SPEC={kafka_python}",
                 "--build-arg", f"HTTPX_SPEC={httpx}", HERE],
    }
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        futs = {pool.submit(docker, *args, timeout=1800): img for img, args in jobs.items()}
        futs[pool.submit(docker, "pull", "-q", BROKER_IMAGE, timeout=600)] = BROKER_IMAGE
        for fut in concurrent.futures.as_completed(futs):
            res = fut.result()
            if res.returncode != 0:
                raise SystemExit(f"harness: building {futs[fut]} failed:\n{res.stderr[-3000:]}")


def _pin(requirements, package):
    with open(requirements) as fh:
        for line in fh:
            spec = line.split("#")[0].strip()
            if re.match(rf"{re.escape(package)}\s*[<>=~!]", spec):
                return spec
    raise SystemExit(f"harness: no {package} pin in {requirements}")


def connector_dir():
    base = os.path.join(ROOT, "shared/mcp-connectors/internal/debezium")
    with open(os.path.join(base, "latest.json")) as fh:
        return os.path.join(base, "versions", json.load(fh)["current_version"])


SERVER_PROPERTIES = """\
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9099
controller.listener.names=CONTROLLER
inter.broker.listener.name=ADMIN
listeners=ADMIN://0.0.0.0:9091,CLEAR://0.0.0.0:9092,TLS://0.0.0.0:9093,SPLAIN://0.0.0.0:9095,SSCRAM://0.0.0.0:9096,SOAUTH://0.0.0.0:9097,MTLS://0.0.0.0:9098,CONTROLLER://0.0.0.0:9099
advertised.listeners=ADMIN://localhost:9091,CLEAR://@BROKER@:9092,TLS://@BROKER@:9093,SPLAIN://@BROKER@:9095,SSCRAM://@BROKER@:9096,SOAUTH://@BROKER@:9097,MTLS://@BROKER@:9098
listener.security.protocol.map=ADMIN:PLAINTEXT,CLEAR:PLAINTEXT,TLS:SSL,SPLAIN:SASL_SSL,SSCRAM:SASL_SSL,SOAUTH:SASL_SSL,MTLS:SSL,CONTROLLER:PLAINTEXT
log.dirs=/tmp/kraft-logs
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
# The defining property of a managed cluster: nothing springs into existence.
auto.create.topics.enable=false
ssl.keystore.type=PEM
ssl.keystore.location=/etc/kafka/secrets/broker-keystore.pem
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/secrets/ca.crt
ssl.client.auth=none
listener.name.mtls.ssl.client.auth=required
listener.name.splain.sasl.enabled.mechanisms=PLAIN
listener.name.splain.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_alice="@PLAIN_PW@";
listener.name.sscram.sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.sscram.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.soauth.sasl.enabled.mechanisms=OAUTHBEARER
listener.name.soauth.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
listener.name.soauth.oauthbearer.sasl.server.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler
sasl.oauthbearer.jwks.endpoint.url=http://@OIDC@:8080/jwks
sasl.oauthbearer.expected.audience=kafka
sasl.oauthbearer.expected.issuer=http://@OIDC@:8080
"""

# Secrets arrive by env; this script itself holds none.
BROKER_START = """\
set -eu
sed -e "s|@BROKER@|$BROKER|g" -e "s|@OIDC@|$OIDC|g" -e "s|@PLAIN_PW@|$PLAIN_PW|g" \\
  /cfg/server.properties.in > /tmp/server.properties
/opt/kafka/bin/kafka-storage.sh format -t kmatrixClusterId01234A -c /tmp/server.properties \\
  --add-scram "SCRAM-SHA-512=[name=alice,password=$SCRAM_PW]" >/dev/null
exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties
"""


def wait_for(what, probe, tries=90):
    for _ in range(tries):
        if probe():
            return
        time.sleep(1)
    raise SystemExit(f"harness: {what} never became ready")


def infra():
    print(f"[{PFX}] PKI, OIDC provider, broker")
    docker("network", "create", "--label", LABEL, NET, check=True)
    os.makedirs(PKI)
    docker("run", "--rm", "--label", LABEL, "-u", f"{os.getuid()}:{os.getgid()}",
           "-v", f"{PKI}:/out", "-v", f"{HERE}:/kmx:ro", "--entrypoint", "python",
           IMG_OIDC, "/kmx/pki.py", "/out", BROKER, check=True)

    docker("run", "-d", "--name", OIDC, "--label", LABEL, "--network", NET,
           "--env-file", write_env(os.path.join(W, "oidc.env"),
                                   {"OIDC_CLIENT_SECRET": SECRET["OIDC_SECRET"]}),
           "-e", f"OIDC_ISSUER=http://{OIDC}:8080", "-e", "OIDC_AUDIENCE=kafka",
           "-e", "OIDC_CLIENT_ID=rsync", "-e", "OIDC_PORT=8080", "-e", "OIDC_KEY=/tmp/oidc.key",
           IMG_OIDC, check=True)
    wait_for("OIDC provider", lambda: docker(
        "exec", OIDC, "python", "-c",
        "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz')").returncode == 0)

    cfg = os.path.join(W, "broker")
    os.makedirs(cfg)
    for name, body in (("server.properties.in", SERVER_PROPERTIES), ("start.sh", BROKER_START)):
        with open(os.path.join(cfg, name), "w") as fh:
            fh.write(body)
        os.chmod(os.path.join(cfg, name), 0o644)
    docker("run", "-d", "--name", BROKER, "--label", LABEL, "--network", NET,
           "--network-alias", WRONG_HOST,
           "--env-file", write_env(os.path.join(W, "broker.env"), {
               "BROKER": BROKER, "OIDC": OIDC,
               "PLAIN_PW": SECRET["PLAIN_PW"], "SCRAM_PW": SECRET["SCRAM_PW"]}),
           "-e", "KAFKA_HEAP_OPTS=-Xms256m -Xmx512m",
           "-v", f"{PKI}:/etc/kafka/secrets:ro", "-v", f"{cfg}:/cfg:ro",
           "--entrypoint", "bash", BROKER_IMAGE, "/cfg/start.sh", check=True)
    topics = ["exec", BROKER, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9091"]

    def broker_ready():
        if docker("inspect", "-f", "{{.State.Running}}", BROKER).stdout.strip() != "true":
            raise SystemExit("harness: broker exited:\n" + redact(docker("logs", "--tail", "40", BROKER).stderr))
        return docker(*topics, "--list").returncode == 0
    wait_for("broker", broker_ready)
    docker(*topics, "--create", "--if-not-exists", "--topic", "kmatrix", "--partitions", "1",
           "--replication-factor", "1", check=True)


# ---------------------------------------------------------------- rows

CA = {"KAFKA_SSL_CA_LOCATION": "/pki/ca.crt"}
TOKEN_URL = f"http://{OIDC}:8080/token"

# Reasons. (?i) throughout: the three runtimes spell the same verdict differently.
R_BAD_CERT = r"(?i)bad.?certificate|certificate.?unknown|certificate.?required|fatal alert: ba"
R_BAD_PW = r"(?i)invalid username or password|invalid credentials|sasl ?authentication ?failed|saslauthenticationfailed|authentication failed"
R_BAD_CLIENT = r"(?i)invalid_client|\b401\b|loginexception|could not obtain"
R_UNKNOWN_CA = r"(?i)unknown authority|pkix path building failed|certificate_verify_failed|certificate verify failed"
R_WRONG_HOST = r"(?i)not \S*wrongname|no subject alternative|hostname mismatch|doesn't match"
R_BAD_TOKEN = r"(?i)invalid_token|invalid credentials|saslauthenticationfailed|authentication failed"
# The refusal must NAME its own escape hatch, in every runtime. A message that
# only says "use https" strands the operator whose IdP genuinely is on loopback.
R_INSECURE_ENDPOINT = r"KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT"


class Row:
    def __init__(self, name, port, expect, env, reason=None, host=None, per_runtime=None):
        self.name, self.expect, self.reason = name, expect, reason
        self.env = {"KAFKA_BROKERS": f"{host or BROKER}:{port}", **env}
        self.per_runtime = per_runtime or {}  # runtime -> (expect, reason)

    def expectation(self, runtime):
        return self.per_runtime.get(runtime, (self.expect, self.reason))


# THE one place in the repo that may set this. Every runtime refuses a plain-http
# token endpoint off loopback, because the client-credentials grant POSTs the
# client secret on every fetch -- and this harness's IdP is exactly that: plain
# http on a container hostname. It is a throwaway on a private docker network
# with a fixture secret, which is the case the opt-out exists for. Setting it in
# docker-compose.yml or docker-compose.prod.yml is forbidden; a test asserts so.
ALLOW_HTTP_IDP = {"KAFKA_SASL_OAUTHBEARER_ALLOW_INSECURE_TOKEN_ENDPOINT": "true"}


def oauth(secret, url=TOKEN_URL, allow_http=True):
    env = {"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", **CA, "KAFKA_SASL_MECHANISM": "OAUTHBEARER",
           "KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT": url,
           "KAFKA_SASL_OAUTHBEARER_CLIENT_ID": "rsync", "KAFKA_SASL_OAUTHBEARER_CLIENT_SECRET": secret}
    return {**env, **ALLOW_HTTP_IDP} if allow_http else env


def sasl(mechanism, password):
    return {"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", **CA, "KAFKA_SASL_MECHANISM": mechanism,
            "KAFKA_SASL_USERNAME": "alice", "KAFKA_SASL_PASSWORD": password}


def mtls(stem):
    return {"KAFKA_SSL_CERT_LOCATION": f"/pki/{stem}.crt", "KAFKA_SSL_KEY_LOCATION": f"/pki/{stem}.key"}


SSL = {"KAFKA_SECURITY_PROTOCOL": "SSL", **CA}

CLIENT_ROWS = [
    Row("01-plaintext", 9092, "PASS", {"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"}),
    Row("02-tls", 9093, "PASS", SSL),
    Row("03-sasl-plain", 9095, "PASS", sasl("PLAIN", SECRET["PLAIN_PW"])),
    Row("04-sasl-scram", 9096, "PASS", sasl("SCRAM-SHA-512", SECRET["SCRAM_PW"])),
    Row("05-oauth", 9097, "PASS", oauth(SECRET["OIDC_SECRET"])),
    Row("06-mtls", 9098, "PASS", {**SSL, **mtls("client"), "KAFKA_SSL_KEYSTORE_LOCATION": "/pki/client.pem"}),
    Row("07-mtls-no-cert", 9098, "FAIL", SSL, R_BAD_CERT),
    Row("08-mtls-rogue-cert", 9098, "FAIL",
        {**SSL, **mtls("rogue-client"), "KAFKA_SSL_KEYSTORE_LOCATION": "/pki/rogue-client.pem"}, R_BAD_CERT),
    Row("09a-plain-bad-pw", 9095, "FAIL", sasl("PLAIN", "wrong-password"), R_BAD_PW),
    Row("09b-scram-bad-pw", 9096, "FAIL", sasl("SCRAM-SHA-512", "wrong-password"), R_BAD_PW),
    Row("09c-oauth-bad-secret", 9097, "FAIL", oauth("wrong-secret"), R_BAD_CLIENT),
    Row("10a-tls-rogue-ca", 9093, "FAIL", {"KAFKA_SECURITY_PROTOCOL": "SSL",
                                           "KAFKA_SSL_CA_LOCATION": "/pki/rogue-ca.crt"}, R_UNKNOWN_CA),
    Row("10b-tls-wrong-host", 9093, "FAIL", SSL, R_WRONG_HOST, host=WRONG_HOST),
    # A token the IdP signed with a key it never published: the BROKER must refuse it.
    # kafka-python reports only the broker's disconnect here; PR B surfaces the real cause.
    Row("c1-oauth-forged-jwt", 9097, "FAIL", oauth(SECRET["OIDC_SECRET"], TOKEN_URL + "?rogue=1"),
        R_BAD_TOKEN, per_runtime={"python": ("FAIL", R_BAD_TOKEN + r"|socket disconnected|kafkatimeout")}),
    # mTLS configured the way the Go and Python clients take it -- two paths, no
    # keystore. The JVM needs one combined file; this row is the contract that
    # every runtime gets a working client cert from these two variables alone.
    Row("c2-mtls-certkey-only", 9098, "PASS", {**SSL, **mtls("client")}),
    # The SAME endpoint every passing OAuth row uses, minus the opt-out. It must
    # be refused BEFORE a socket is opened -- the point is that the client secret
    # never reaches the wire, so "it failed to connect" is not the assertion;
    # "it refused, and said which variable re-permits it" is.
    Row("c3-oauth-http-endpoint", 9097, "FAIL",
        oauth(SECRET["OIDC_SECRET"], allow_http=False), R_INSECURE_ENDPOINT),
]

# Connect-row extras: a private CA reaches the worker as a truststore PATH, set
# on all three clients (docker-compose.yml explains why these are bare keys).
TRUST = {"CONNECT_SSL_TRUSTSTORE_LOCATION": "/pki/ca.crt",
         "CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION": "/pki/ca.crt",
         "CONNECT_CONSUMER_SSL_TRUSTSTORE_LOCATION": "/pki/ca.crt",
         "KAFKA_SSL_TRUSTSTORE_TYPE": "PEM"}
SCRAM_JAAS = ('org.apache.kafka.common.security.scram.ScramLoginModule required '
              f'username="alice" password="{SECRET["SCRAM_PW"]}";')

CONNECT_ROWS = [
    Row("cx-01-plaintext", 9092, "PASS", {"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"}),
    Row("cx-02-tls-ca", 9093, "PASS", {**SSL, **TRUST}),
    Row("cx-03-mtls-keystore", 9098, "PASS",
        {**SSL, **TRUST, "KAFKA_SSL_KEYSTORE_LOCATION": "/pki/client.pem"}),
    Row("cx-04-mtls-certkey", 9098, "PASS", {**SSL, **TRUST, **mtls("client")}),
    Row("cx-05-scram-jaas", 9096, "PASS",
        {**sasl("SCRAM-SHA-512", SECRET["SCRAM_PW"]), **TRUST, "KAFKA_SASL_JAAS_CONFIG": SCRAM_JAAS}),
    Row("cx-06-scram-userpass", 9096, "PASS", {**sasl("SCRAM-SHA-512", SECRET["SCRAM_PW"]), **TRUST}),
    Row("cx-07-plain-userpass", 9095, "PASS", {**sasl("PLAIN", SECRET["PLAIN_PW"]), **TRUST}),
    Row("cx-08-oauth", 9097, "PASS", {**oauth(SECRET["OIDC_SECRET"]), **TRUST}),
    Row("cx-09-mtls-rogue-cert", 9098, "FAIL", {**SSL, **TRUST, **mtls("rogue-client")}, R_BAD_CERT),
    # A PKCS#1 key cannot go into a JVM PEM keystore: refuse at startup, naming the fix.
    Row("cx-10-mtls-pkcs1-key", 9098, "FAIL",
        {**SSL, **TRUST, "KAFKA_SSL_CERT_LOCATION": "/pki/client.crt",
         "KAFKA_SSL_KEY_LOCATION": "/pki/client-pkcs1.key"}, r"PKCS#8"),
    # Half an mTLS pair is a config error, not "no mTLS": refuse, naming the missing one.
    Row("cx-11-mtls-cert-only", 9098, "FAIL",
        {**SSL, **TRUST, "KAFKA_SSL_CERT_LOCATION": "/pki/client.crt"}, r"KAFKA_SSL_KEY_LOCATION"),
    # The JVM twin of c3. It matters separately: the JVM's own gate for this is a
    # -D system property (org.apache.kafka.sasl.oauthbearer.allowed.urls) whose
    # UNSET value means ALLOW ANY URL, so nothing in kafka-clients refuses this
    # on its own -- the entrypoint has to, before it ever builds a worker config.
    Row("cx-12-oauth-http-endpoint", 9097, "FAIL",
        {**oauth(SECRET["OIDC_SECRET"], allow_http=False), **TRUST}, R_INSECURE_ENDPOINT),
]


def grade(expect, reason, got, msg):
    if got != expect:
        return False
    return expect == "PASS" or reason is None or re.search(reason, msg) is not None


# ---------------------------------------------------------------- client matrix

def client_cell(row, runtime):
    try:
        return _client_cell(row, runtime)
    except subprocess.TimeoutExpired:
        docker("rm", "-f", f"{PFX}-{row.name}-{runtime}", f"{PFX}-{row.name}-{runtime}-props")
        return "ERROR", f"timed out after {CELL_TIMEOUT}s"


def _client_cell(row, runtime):
    envf = os.path.join(W, "rows", row.name + ".env")
    cname = f"{PFX}-{row.name}-{runtime}"
    pid = f"{row.name}-{runtime}-{secrets.token_hex(4)}"
    base = ["run", "--rm", "--name", cname, "--label", LABEL, "--network", NET, "--env-file", envf,
            "-e", f"PROBE_ID={pid}", "-v", f"{PKI}:/pki:ro", "-v", f"{HERE}:/kmx:ro"]
    stdin = None
    if runtime.startswith("go-"):
        args = base + ["-v", f"{W}/kmatrix-probe:/probe:ro", "--entrypoint", "/probe", IMG_PY, runtime[3:]]
    elif runtime == "python":
        args = base + ["-v", f"{ROOT}/llm-service/src/utils/kafka_security.py:/rsync/kafka_security.py:ro",
                       IMG_PY, "python", "/kmx/probe.py"]
    else:
        gen = docker("run", "--rm", "--name", cname + "-props", "--label", LABEL, "--env-file", envf,
                     "-v", f"{connector_dir()}:/connector:ro", "-v", f"{HERE}:/kmx:ro",
                     IMG_PY, "python", "/kmx/schema_history_props.py", timeout=CELL_TIMEOUT)
        if gen.returncode != 0:
            return result_line(gen.stderr) or ("ERROR", "props generator: " + gen.stderr[-400:])
        args, stdin = ["run", "-i", *base[1:], IMG_CONNECT, *JAVA, "-"], gen.stdout
    res = docker(*args, timeout=CELL_TIMEOUT, input=stdin)
    return result_line(res.stdout) or ("ERROR", "no RESULT line: " + (res.stdout + res.stderr)[-400:])


def client_matrix(rows):
    for row in rows:
        write_env(os.path.join(W, "rows", row.name + ".env"), row.env)
    results = {}
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        futs = {pool.submit(client_cell, row, rt): (row, rt) for row in rows for rt in RUNTIMES}
        for fut in concurrent.futures.as_completed(futs):
            results[futs[fut]] = fut.result()
    return [(row.name, rt, *row.expectation(rt), *results[(row, rt)]) for row in rows for rt in RUNTIMES]


# ---------------------------------------------------------------- connect matrix

SECURITY_KEY = re.compile(r"^(KAFKA_BROKERS|BOOTSTRAP_SERVERS|KAFKA_(SECURITY|SASL|SSL)_\w+"
                          r"|CONNECT_(PRODUCER_|CONSUMER_)?(SECURITY|SASL|SSL)_\w+)$")
# docker-compose.quickstart.yml aborts on these (`${VAR:?}`) -- none is read by the
# two services rendered here.
QUICKSTART_DUMMIES = {k: "kmatrix-render-only" for k in (
    "ENCRYPTION_KEY", "JWT_SECRET", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
    "POSTGRES_PASSWORD", "REDIS_PASSWORD")}


def render(compose_file, env, extra=()):
    """`docker compose config` with NOTHING from the caller's shell or a .env file."""
    envf = write_env(os.path.join(W, f"render-{secrets.token_hex(4)}.env"), env)
    clean = {k: os.environ[k] for k in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT") if k in os.environ}
    res = run(["docker", "compose", "-f", os.path.join(ROOT, compose_file), "--env-file", envf, *extra,
               "config", "--format", "json", "kafka-connect", "debezium-mcp"], env=clean)
    os.unlink(envf)
    if res.returncode != 0:
        raise SystemExit(f"harness: rendering {compose_file} failed:\n{redact(res.stderr[-2000:])}")
    svc = json.loads(res.stdout)["services"]
    return ({k: v for k, v in (svc["kafka-connect"].get("environment") or {}).items() if v is not None},
            {k: v for k, v in (svc["debezium-mcp"].get("environment") or {}).items() if v is not None})


def parity(main, quick):
    """Security-variable names whose presence or value differ between the two composes."""
    names = []
    for svc_main, svc_quick in zip(main, quick):
        a = {k: v for k, v in svc_main.items() if SECURITY_KEY.match(k)}
        b = {k: v for k, v in svc_quick.items() if SECURITY_KEY.match(k)}
        names += sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
    return names


def connect_row(row):
    main = render("docker-compose.yml", row.env)
    quick = render("docker-compose.quickstart.yml", {**row.env, **QUICKSTART_DUMMIES}, ("--profile", "cdc"))
    drift = parity(main, quick)
    worker_env, dbz_env = main
    cname = f"{PFX}-{row.name}"
    worker_env = {**worker_env,
                  "GROUP_ID": cname, "ADVERTISED_HOST_NAME": cname,
                  "CONFIG_STORAGE_TOPIC": f"{cname}-configs",
                  "OFFSET_STORAGE_TOPIC": f"{cname}-offsets",
                  "STATUS_STORAGE_TOPIC": f"{cname}-status",
                  "KAFKA_HEAP_OPTS": "-Xms256M -Xmx512M"}
    secrets_dir = os.path.join(W, "connect-secrets")
    os.makedirs(secrets_dir, exist_ok=True)
    docker("run", "-d", "--name", cname, "--label", LABEL, "--network", NET,
           "--env-file", write_env(os.path.join(W, "rows", row.name + ".worker.env"), worker_env),
           "-v", f"{PKI}:/pki:ro", "-v", f"{HERE}:/kmx:ro", "-v", f"{secrets_dir}:/connect-secrets:ro",
           IMG_CONNECT, check=True)

    cells = {"worker": None, "tasks": ("-", ""), "history": ("-", "")}
    deadline = time.time() + WORKER_TIMEOUT
    while cells["worker"] is None:
        # Liveness BEFORE logs, and never the other way round: a worker that
        # fails its config (cx-10's PKCS#1 key) dies within milliseconds of
        # `docker run -d` returning. Reading logs first lets it exit in the gap
        # between the two reads -- empty logs, yet already not running -- so the
        # cell failed for the right reason but reported "no output" and the
        # `reason` regex could not match. Reading liveness first means "not
        # running" was already true when the logs are fetched, so they are final.
        running = docker("inspect", "-f", "{{.State.Running}}", cname).stdout.strip() == "true"
        logs = redact(_logs(cname))
        if WORKER_READY in logs:
            cells["worker"] = ("PASS", "worker started")
        elif not running or time.time() > deadline:
            why = "exited" if not running else f"not started after {WORKER_TIMEOUT}s"
            cells["worker"] = ("FAIL", f"{why}: " + _cause(logs, row.reason))
        else:
            time.sleep(2)

    if cells["worker"][0] == "PASS":
        try:
            cells["tasks"] = _tasks_cell(row, cname)
            cells["history"] = _history_cell(row, cname, dbz_env)
        except subprocess.TimeoutExpired:
            for cell in ("tasks", "history"):
                if cells[cell][0] == "-":
                    cells[cell] = ("ERROR", f"timed out after {CELL_TIMEOUT}s")
    docker("rm", "-f", cname, cname + "-props")

    out = []
    for cell, (got, msg) in cells.items():
        if cell != "worker" and cells["worker"][0] != "PASS":
            expect, reason = "-", None
        elif cell == "worker":
            expect, reason = row.expect, row.reason
        else:
            expect, reason = "PASS", None
        out.append((row.name, cell, expect, reason, got, msg))
    out.append((row.name, "quickstart", "SAME", None, "DIFF" if drift else "SAME",
                ("differs: " + ", ".join(drift)) if drift else "security env identical"))
    return out


def _tasks_cell(row, cname):
    """The producer./consumer. clients this worker hands its tasks."""
    pid = f"{row.name}-tasks-{secrets.token_hex(4)}"
    res = docker("exec", "-e", f"PROBE_ID={pid}", cname, *JAVA, "--connect",
                 "/kafka/config/connect-distributed.properties", timeout=CELL_TIMEOUT)
    return result_line(res.stdout) or ("ERROR", (res.stdout + res.stderr)[-400:])


def _history_cell(row, cname, dbz_env):
    """What debezium-mcp would emit for schema history, run inside this worker."""
    gen = docker("run", "--rm", "--name", cname + "-props", "--label", LABEL,
                 "--env-file", write_env(os.path.join(W, "rows", row.name + ".dbz.env"), dbz_env),
                 "-v", f"{connector_dir()}:/connector:ro", "-v", f"{HERE}:/kmx:ro",
                 IMG_PY, "python", "/kmx/schema_history_props.py", timeout=CELL_TIMEOUT)
    if gen.returncode != 0:
        return result_line(gen.stderr) or ("ERROR", gen.stderr[-400:])
    pid = f"{row.name}-history-{secrets.token_hex(4)}"
    res = docker("exec", "-i", "-e", f"PROBE_ID={pid}", cname, *JAVA, "-",
                 timeout=CELL_TIMEOUT, input=gen.stdout)
    return result_line(res.stdout) or ("ERROR", (res.stdout + res.stderr)[-400:])


def _logs(name):
    res = docker("logs", name)
    return res.stdout + res.stderr


def _cause(logs, reason):
    # Java stack frames ("\tat org.apache...Exception...") match both patterns
    # below and would be reported instead of the message line that names the cause.
    lines = [ln for ln in logs.splitlines() if not re.match(r"\s+at ", ln)]
    if reason:
        hit = next((ln for ln in lines if re.search(reason, ln)), None)
        if hit:
            return hit.strip()
    pick = [ln for ln in lines if re.search(r"FATAL|ERROR|Exception", ln)]
    return (pick[-1] if pick else (lines[-1] if lines else "no output")).strip()


def connect_matrix(rows):
    if not rows:
        return []
    first, rest = rows[0], rows[1:]
    out = connect_row(first)  # alone: if the simplest worker cannot boot, stop here
    probes = [c for c in out if c[1] in ("worker", "tasks", "history")]
    if first.name == "cx-01-plaintext" and not all(grade(e, r, g, m) for _, _, e, r, g, m in probes):
        report(out)
        raise SystemExit("harness: the plaintext Connect control failed; the harness, not the code, is broken")
    with concurrent.futures.ThreadPoolExecutor(3) as pool:
        for cells in pool.map(connect_row, rest):
            out += cells
    return out


# ---------------------------------------------------------------- report

def report(rows):
    bad = 0
    for name, cell, expect, reason, got, msg in rows:
        ok = expect == "-" or grade(expect, reason, got, msg)
        bad += not ok
        why = " ".join(redact(msg).split())
        print(f"{'ok ' if ok else 'BAD'} {name:<24} {cell:<11} want {expect:<4} got {got:<5} {why[:170]}")
        if not ok and got == expect and reason:
            print(f"    wrong reason: expected /{reason}/")
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--rows", default="", help="only rows whose name matches this regex")
    ap.add_argument("--keep", action="store_true", help="leave containers, images and the work dir")
    opts = ap.parse_args()
    pick = re.compile(opts.rows)
    clients = [r for r in CLIENT_ROWS if pick.search(r.name)]
    connects = [r for r in CONNECT_ROWS if pick.search(r.name)]

    def on_signal(signum, _frame):
        raise SystemExit(f"interrupted by signal {signum}")
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    global W, PKI
    W = tempfile.mkdtemp(prefix="kmatrix-")
    PKI = os.path.join(W, "pki")
    try:
        os.makedirs(os.path.join(W, "rows"))
        build()
        infra()
        results = []
        if clients:
            control = [r for r in clients if r.name == "01-plaintext"]
            if control:
                ctl = client_matrix(control)
                if not all(grade(e, r, g, m) for _, _, e, r, g, m in ctl):
                    report(ctl)
                    raise SystemExit("harness: the plaintext control failed; the harness, not the code, is broken")
                results += ctl
            print(f"[{PFX}] client matrix: {len(clients)} rows x {len(RUNTIMES)} runtimes")
            results += client_matrix([r for r in clients if r.name != "01-plaintext"])
        if connects:
            print(f"[{PFX}] connect matrix: {len(connects)} rows")
            results += connect_matrix(connects)
        print(f"\n[{PFX}] results")
        bad = report(results)
        print(f"\n{len(results) - bad}/{len(results)} cells as expected")
        return 1 if bad else 0
    finally:
        cleanup(opts.keep)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit as e:
        if isinstance(e.code, str):  # a harness failure, not a matrix verdict
            print(redact(e.code), file=sys.stderr)
            sys.exit(2)
        raise
