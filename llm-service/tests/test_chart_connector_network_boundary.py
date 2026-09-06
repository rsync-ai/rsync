"""Connector pods must not reach the platform datastores.

Connectors are the one tier running code generated for a tenant and holding that
tenant's source credentials. Compose draws the line with networks: a user
connector joins ONLY the dedicated `rsync-ai-mcp` network and never the internal
`rsync-ai_default`, so it cannot reach postgres, redis, kafka or temporal, and
exactly four services bridge into `rsync-ai-mcp` to call it. That boundary is
named SEC-M-06 in scripts/mcp_generate_compose.py:12-16, and JIT-deployed
connectors land on the same network
(llm-service/src/agents/tool_generator/deployment/service.py:180-188).

KI-CHART-NO-CONNECTOR-NETWORK-BOUNDARY was that the Helm chart had no equivalent:
its `allow-intra-release` NetworkPolicy set both the target selector and the
`from` selector to "any pod in the release", and connector pods carry those
labels, so enabling networkPolicy handed connectors the datastores compose
withholds. The fix excludes them from that policy and gives them their own,
admitting only the bridging callers.

Three things have to stay true, and each is one test below:

  1. `allow-intra-release` excludes connector pods on BOTH halves. Excluding
     them only as a target still lets them reach postgres; only as a source
     still lets every pod reach them.
  2. The connector policy's caller list stays equal to compose's bridge set.
     This is the assertion with teeth: it is derived from docker-compose.yml
     rather than restated, so adding a bridge on one side and not the other
     fails here instead of becoming a quiet divergence.
  3. fleet.yaml keeps stamping `rsync.ai/connector-id` on the pod template.
     That label is what both policies key off. Rename it and connector pods
     stop matching `Exists`, start matching `DoesNotExist`, and fall straight
     back into `allow-intra-release` -- the original bug, reintroduced by a
     rename with no other visible effect.

Text-only because parsing the templates needs no `helm` binary and no render, so it holds on any checkout -- not
because "CI has no helm binary", which is what this said and was false as a
reason: ci.yml sets helm up for the job that collects this suite and asserts it
before pytest. See the `helm is present` step in .github/workflows/ci.yml.
"""

import os
import re

import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")
NETPOL = os.path.join(CHART_DIR, "templates", "networkpolicy.yaml")
FLEET = os.path.join(CHART_DIR, "templates", "connectors", "fleet.yaml")
VALIDATE = os.path.join(CHART_DIR, "templates", "validate.yaml")
COMPOSE = os.path.join(REPO_ROOT, "docker-compose.yml")

CONNECTOR_LABEL = "rsync.ai/connector-id"
MCP_NETWORK = "rsync-ai-mcp"
INTRA = "allow-intra-release"
CONNECTOR_POLICY = "allow-connector-ingress"

# compose service name -> chart `app.kubernetes.io/component` value.
# connector-deployer is deliberately absent: it has no counterpart in the chart
# because connectorDeployer.enabled hard-fails on Kubernetes, which the third
# assertion of test_the_caller_list_equals_composes_bridge_set re-proves.
COMPOSE_TO_COMPONENT = {
    "api-gateway": "api-gateway",
    "orchestrator": "orchestrator",
    "tool-generator": "tool-generator",
    "kafka-mcp-sink-mcp": "kafka-mcp-sink",
}
NO_CHART_COUNTERPART = {"connector-deployer"}


def _read(path):
    text = open(path, encoding="utf-8").read()
    assert text.strip(), "%s is empty -- every assertion below would be vacuous." % path
    return text


def _uncommented(text):
    """Strip {{/* ... */}} blocks. The rationale comments legitimately quote the
    very selectors these tests look for, so matching against raw text would pass
    on prose alone."""
    return re.sub(r"\{\{/\*.*?\*/\}\}", "", text, flags=re.S)


def _policy_block(name):
    """The chunk of networkpolicy.yaml belonging to one policy, comments removed."""
    blocks = re.split(r"(?m)^---\s*$", _uncommented(_read(NETPOL)))
    matching = [b for b in blocks if re.search(r"^\s*name:.*%s\s*$" % re.escape(name), b, re.M)]
    assert len(matching) == 1, (
        "expected exactly one %s policy in networkpolicy.yaml, found %d -- the "
        "block split is wrong and the assertions below would be reading the "
        "wrong text." % (name, len(matching))
    )
    return matching[0]


def _compose_bridge_services():
    """Services attached to BOTH the internal network and rsync-ai-mcp."""
    compose = yaml.safe_load(_read(COMPOSE))
    services = compose.get("services") or {}
    assert len(services) > 10, (
        "docker-compose.yml parsed to %d services -- too few to be the real "
        "stack, so the bridge set below would be empty for the wrong reason."
        % len(services)
    )
    bridges = set()
    for name, spec in services.items():
        nets = spec.get("networks") or []
        if isinstance(nets, dict):
            nets = list(nets.keys())
        if MCP_NETWORK in nets and any(n != MCP_NETWORK for n in nets):
            bridges.add(name)
    assert bridges, (
        "no service in docker-compose.yml joins both %s and an internal network. "
        "Either the network was renamed or the boundary was removed; either way "
        "the comparison below has no denominator." % MCP_NETWORK
    )
    return bridges


def test_intra_release_excludes_connectors_from_both_halves():
    block = _policy_block(INTRA)

    exclusions = re.findall(
        r"key:\s*%s\s*\n\s*operator:\s*DoesNotExist" % re.escape(CONNECTOR_LABEL), block
    )
    assert len(exclusions) == 2, (
        "%s must exclude connector pods twice -- once in its podSelector (so a "
        "connector is not a target of the release-wide allow) and once in "
        "ingress.from (so a connector is not an accepted source to postgres, "
        "redis, kafka or temporal). Found %d. One alone leaves half the hole "
        "open." % (INTRA, len(exclusions))
    )

    assert "Exists" not in re.sub(r"DoesNotExist", "", block), (
        "%s must not carry a bare `Exists` on the connector label -- that would "
        "invert the exclusion into an admission." % INTRA
    )


def test_the_caller_list_equals_composes_bridge_set():
    block = _policy_block(CONNECTOR_POLICY)

    target = re.search(
        r"key:\s*%s\s*\n\s*operator:\s*Exists" % re.escape(CONNECTOR_LABEL), block
    )
    assert target, (
        "%s must select connector pods by `%s: Exists`. Without it the policy "
        "targets something else and connectors keep whatever the release-wide "
        "policy gives them." % (CONNECTOR_POLICY, CONNECTOR_LABEL)
    )

    listed = re.search(
        r"key:\s*app\.kubernetes\.io/component\s*\n\s*operator:\s*In\s*\n\s*values:\s*\[([^\]]+)\]",
        block,
    )
    assert listed, "%s must name its allowed callers as a component `In` list." % CONNECTOR_POLICY
    chart_callers = {c.strip() for c in listed.group(1).split(",") if c.strip()}

    bridges = _compose_bridge_services()
    unmapped = bridges - set(COMPOSE_TO_COMPONENT) - NO_CHART_COUNTERPART
    assert not unmapped, (
        "docker-compose.yml bridges %s into %s, but this test does not know "
        "which chart component that is. A new bridge on the compose side needs "
        "a matching caller in %s (or an explicit entry in NO_CHART_COUNTERPART "
        "saying why it has none)." % (sorted(unmapped), MCP_NETWORK, CONNECTOR_POLICY)
    )

    expected = {COMPOSE_TO_COMPONENT[s] for s in bridges if s in COMPOSE_TO_COMPONENT}
    assert chart_callers == expected, (
        "the chart admits %s to connectors; compose bridges %s. The two must "
        "describe the same boundary -- a caller the chart adds unilaterally is "
        "access compose denies, and one it drops is a pipeline that stalls for "
        "120s in pre-flight rather than failing." % (sorted(chart_callers), sorted(expected))
    )

    # connector-deployer is excluded above on the grounds that it cannot run on
    # Kubernetes. That is only honest while the chart still refuses it.
    assert re.search(r"connectorDeployer\.enabled|connectorDeployer", _read(VALIDATE)), (
        "NO_CHART_COUNTERPART drops connector-deployer because "
        "templates/validate.yaml refuses it on Kubernetes. That guard is gone, "
        "so the exclusion is now unjustified."
    )


def test_fleet_stamps_the_label_both_policies_key_off():
    fleet = _uncommented(_read(FLEET))

    pod_template = fleet.split("template:", 1)
    assert len(pod_template) == 2, "fleet.yaml has no pod template -- nothing to label."
    assert CONNECTOR_LABEL in pod_template[1], (
        "fleet.yaml's pod template no longer sets %s. Both policies key off that "
        "label, so connector pods would stop matching `Exists`, start matching "
        "`DoesNotExist`, and fall back into %s -- reaching postgres, redis and "
        "kafka again. A rename with no other visible effect reintroduces "
        "KI-CHART-NO-CONNECTOR-NETWORK-BOUNDARY." % (CONNECTOR_LABEL, INTRA)
    )
