"""The oss-leak-proof job runs on exactly the paths its script can see.

scripts/oss-leak-proof-test.sh builds Dockerfile.community and Dockerfile.oss
with ONE build context, `$CTX`, and reads nothing on the host outside it but
itself. So `$CTX/**` plus the script is the job's whole subject, and that is
what the `oss` filter in .github/paths-filters.yml lists.

The job used to gate on `llm`, a filter widened again and again so that other
guards reach their subjects (docs, compose, Go trees, the chart). Over PRs
#1214-#1253 that rebuilt both images on 34 PRs, and 11 of those changed nothing
the script can see.

The narrow filter holds only while the script's inputs stay inside `$CTX`. A
new host path outside it would be a subject the filter misses, and the job would
report green on the PR that changed it. That is the dead-guard shape
KI-DATAPATH-FILTER-EXCLUDES-THE-WORKFLOW records. So this file pins both halves:
the filter covers `$CTX` and the script, and the script reads no host path
anywhere else.
"""
from __future__ import annotations

import pathlib
import re

import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_REL = "scripts/oss-leak-proof-test.sh"
SCRIPT = REPO / SCRIPT_REL
FILTERS = REPO / ".github" / "paths-filters.yml"
CI = REPO / ".github" / "workflows" / "ci.yml"

# Paths inside the built image or a scratch copy of it, not the host checkout.
IN_IMAGE = ("/app/", "$tmp/")


def _code_lines():
    return [ln for ln in SCRIPT.read_text().splitlines() if not ln.lstrip().startswith("#")]


def _ctx():
    m = re.search(r'^CTX="([^"$]+)"$', SCRIPT.read_text(), re.M)
    assert m, f"{SCRIPT_REL} no longer sets CTX to a literal path; this guard cannot read its build context"
    return m.group(1)


def test_every_image_is_built_from_ctx():
    builds = [ln for ln in _code_lines() if "docker build" in ln]
    assert builds, f"{SCRIPT_REL} builds no image -- the checks below would pass on nothing"
    for ln in builds:
        assert re.search(r'-f "\$CTX/[^"]+"', ln) and re.search(r'"\$CTX"(\s|$)', ln), (
            f"a build outside $CTX: {ln.strip()!r}. Its context is a subject the `oss` filter does not list.")


def test_every_host_path_the_script_reads_is_under_ctx():
    # A quoted string made only of path characters -- which leaves out sed
    # expressions like "s|^$tmp/||" and messages that happen to hold a slash.
    paths = {p for ln in _code_lines() for p in re.findall(r'"([\w$./{}-]*/[\w$./{}-]*)"', ln)}
    assert paths, "found no quoted path at all -- the parse broke, not the script"
    outside = sorted(p for p in paths if not p.startswith("$CTX/") and not p.startswith(IN_IMAGE))
    assert not outside, (
        f"{SCRIPT_REL} reads {outside}, outside $CTX. Add each to the `oss` filter, or the job skips "
        f"the PRs that change it.")


def test_the_oss_filter_is_ctx_plus_the_script():
    got = yaml.safe_load(FILTERS.read_text())["oss"]
    for want in (f"{_ctx()}/**", SCRIPT_REL):
        assert want in got, f"the `oss` filter does not list {want!r}, so a PR changing only it skips the job"


def test_the_leak_job_gates_on_oss():
    steps = yaml.safe_load(CI.read_text())["jobs"]["oss-leak-proof"]["steps"]
    gate = next(st for st in steps if "RUN=1" in str(st.get("run", "")))
    wanted = str((gate.get("env") or {}).get("WANTED", ""))
    assert "steps.filter.outputs.oss == 'true'" in wanted, wanted
    assert "steps.filter.outputs.llm" not in wanted, (
        "the leak job is back on `llm`, which rebuilds both images on PRs that cannot affect them")
