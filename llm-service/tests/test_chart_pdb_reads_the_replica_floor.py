"""A PodDisruptionBudget must be sized against the pods that will exist.

Under a HorizontalPodAutoscaler a Deployment stops emitting `replicas:` at all --
templates/apps/api-gateway.yaml wraps it in
`{{- if not .Values.apiGateway.autoscaling.enabled }}` -- so `replicaCount`
describes nothing the cluster is using and the autoscaler's `minReplicas` is the
real floor. KI-CHART-PDB-GATE-IGNORES-HPA was templates/pdb.yaml gating on
`replicaCount` anyway, which went wrong in both directions:

  replicaCount:1 + minReplicas:2  -> no PDB for a tier really running two pods.
  replicaCount:2 + minReplicas:1  -> a PDB whose minAvailable equals the floor,
                                     so disruptionsAllowed is 0, every eviction
                                     blocks, and a node drain hangs -- the exact
                                     deadlock pdb.yaml's own comment cites as
                                     its reason for existing.

Both directions are render-verified. What this file guards is that the plumbing
stays: that pdb.yaml keeps reading the shared floor helper and does not quietly
go back to reading replicaCount, and that the helper keeps consulting
autoscaling at all. Text-only -- parsing the templates needs no `helm` binary and no render, so it holds on any checkout. (Not
because "CI has no helm binary": that was false, and the `helm is present` step in .github/workflows/ci.yml carries the correction.)
"""

import os
import re

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")
PDB = os.path.join(CHART_DIR, "templates", "pdb.yaml")
HELPERS = os.path.join(CHART_DIR, "templates", "_helpers.tpl")
API_GATEWAY = os.path.join(CHART_DIR, "templates", "apps", "api-gateway.yaml")
HELPER_NAME = "rsync-ai.guaranteedReplicas"


def _read(path):
    text = open(path, encoding="utf-8").read()
    assert text.strip(), "%s is empty -- every assertion below would be vacuous." % path
    return text


def _uncommented(text):
    """Template source with {{/* ... */}} comments removed.

    The comments here name `replicaCount` while explaining why the code must not
    read it, so a naive grep would fire on the explanation.
    """
    return re.sub(r"\{\{/\*.*?\*/\}\}", "", text, flags=re.DOTALL)


def test_the_floor_helper_exists_and_consults_autoscaling():
    helpers = _read(HELPERS)
    assert 'define "%s"' % HELPER_NAME in helpers, (
        "%s is gone from _helpers.tpl. It is the one place that knows a tier's "
        "floor is minReplicas under an HPA and replicaCount otherwise; without "
        "it every caller re-derives that, which is how the bug happened."
        % HELPER_NAME
    )
    body = helpers.split('define "%s"' % HELPER_NAME, 1)[1].split("{{- end -}}", 1)[0]
    assert "autoscaling" in body and "minReplicas" in body, (
        "%s no longer consults autoscaling.minReplicas, so it returns "
        "replicaCount unconditionally -- which is the pre-fix behaviour under a "
        "different name." % HELPER_NAME
    )
    assert "replicaCount" in body, (
        "%s no longer falls back to replicaCount, so a tier with autoscaling "
        "disabled (the default, and the only mode the frontend has) would get "
        "no count at all." % HELPER_NAME
    )


def test_pdb_reads_the_floor_and_never_replicacount_directly():
    code = _uncommented(_read(PDB))
    assert HELPER_NAME in code, (
        "templates/pdb.yaml no longer calls %s. Whatever it reads instead is a "
        "second derivation of the pod floor, and the first one was wrong under "
        "an HPA." % HELPER_NAME
    )
    stray = re.findall(r"\.Values\.\w+\.replicaCount", code)
    assert not stray, (
        "templates/pdb.yaml reads %s directly. Under an HPA the Deployment emits "
        "no `replicas:` and that value is not what the cluster runs; read the "
        "floor through %s instead." % (", ".join(sorted(set(stray))), HELPER_NAME)
    )


def test_the_deployment_still_suppresses_replicas_under_an_hpa():
    """The premise of the whole fix. If this stops being true, revisit it."""
    code = _read(API_GATEWAY)
    assert re.search(
        r"if not \.Values\.apiGateway\.autoscaling\.enabled\s*\}\}\s*\n\s*replicas:", code
    ), (
        "templates/apps/api-gateway.yaml no longer suppresses `replicas:` under "
        "autoscaling.\n\n"
        "That suppression is WHY the PDB gate cannot read replicaCount. If the "
        "Deployment now emits a replica count under an HPA, re-derive the floor "
        "rather than leaving %s asserting against a premise that changed."
        % HELPER_NAME
    )
