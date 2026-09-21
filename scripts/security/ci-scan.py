#!/usr/bin/env python3
"""Scope the Security workflow's scans to what a pull request touched, and merge
per-module gosec SARIF into one document.

usage:
    ci-scan.py go-modules MODULE...        which Go modules to scan
    ci-scan.py semgrep-includes OUTFILE    which files Semgrep should scan
    ci-scan.py merge-sarif OUT MODULE=FILE...

`go-modules` and `semgrep-includes` write `mode=` and (for go-modules)
`modules=` to $GITHUB_OUTPUT (stdout when unset) and a short reason to
$GITHUB_STEP_SUMMARY.

    mode=full     every target is scanned: push, schedule, workflow_dispatch, a
                  PR that touched every module, or a PR whose file list could not
                  be read
    mode=partial  a PR, scanning only what it touched
    mode=none     a PR that touched nothing this scanner covers

Why scope at all: security.yml used to fan out into 14 jobs per push -- six
govulncheck and four gosec matrix legs, each of which spent ~3 s scanning and
~30 s on checkout, toolchain setup and uploads -- and every one of them queued
for a runner. The scan was never the cost; the job count was.

Every doubt resolves toward scanning MORE, never less:
  * anything but a `pull_request` event scans everything;
  * a PR file list that cannot be read (no token, API error, a PR at GitHub's
    3,000-file listing cap) scans everything, with a ::warning;
  * a Go module is in scope when a changed file sits under the module OR under
    any local `replace` target it reaches, followed transitively -- a change to
    shared/go/pgdriver re-scans every service that compiles it in;
  * a change to security.yml or to this script scans every Go module AND the
    whole tree with Semgrep, so the PR that edits a scan or its scoping is
    itself checked against everything that scan covers.

SCAN_SCOPE_CHANGED_FILES=<file> replaces the API call with a newline-separated
file list. It exists for the tests and for a local dry run; CI never sets it.
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

# A change to either file scans everything: every Go module and, for Semgrep,
# the whole tree (see the docstring).
SCAN_EVERYTHING_WHEN_CHANGED = (
    ".github/workflows/security.yml",
    "scripts/security/ci-scan.py",
)

# GET /pulls/{n}/files lists at most 3,000 files. A PR at the cap may be longer
# than the list, so the list cannot prove what it left out.
PR_FILES_API_CAP = 3000

REPLACE_LINE = re.compile(r"^\s*(?:replace\s+)?\S+(?:\s+\S+)?\s+=>\s+(\S+)(?:\s+\S+)?\s*$")


def warn(msg: str) -> None:
    print(f"::warning title=Security scan scope::{msg}")


def emit(**outputs: str) -> None:
    lines = "".join(f"{k}={v}\n" for k, v in outputs.items())
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(lines)
    sys.stdout.write(lines)


def summarize(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except OSError:
        # Same rule as report-sarif-sinks.sh: an unwritable summary is a runner
        # condition, and must not fail a security job.
        print("could not append to $GITHUB_STEP_SUMMARY; the scope is in this log instead")


def _pr_files_from_api() -> list[str] | None:
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
    event_path = os.environ.get("GITHUB_EVENT_PATH", "")
    if not (repo and token and event_path):
        warn("no repository, token or event payload to list the PR's files with; scanning everything")
        return None
    try:
        with open(event_path, encoding="utf-8") as f:
            number = json.load(f)["pull_request"]["number"]
        listed = 0
        files: list[str] = []
        for page in range(1, PR_FILES_API_CAP // 100 + 2):
            req = urllib.request.Request(
                f"{api}/repos/{repo}/pulls/{number}/files?per_page=100&page={page}",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                batch = json.load(resp)
            for entry in batch:
                listed += 1
                files.append(entry["filename"])
                # A rename's old path matters too: moving a file OUT of a module
                # changes that module.
                if entry.get("previous_filename"):
                    files.append(entry["previous_filename"])
            if len(batch) < 100:
                break
    except (OSError, ValueError, KeyError, TypeError, urllib.error.URLError) as exc:
        warn(f"could not list the PR's changed files ({exc}); scanning everything")
        return None
    if listed >= PR_FILES_API_CAP:
        warn(f"the PR lists {listed} files, GitHub's cap for this API; scanning everything")
        return None
    return files


def changed_files() -> list[str] | None:
    """The files this PR touches, or None when every target must be scanned."""
    if os.environ.get("GITHUB_EVENT_NAME") != "pull_request":
        return None
    override = os.environ.get("SCAN_SCOPE_CHANGED_FILES")
    if override:
        text = Path(override).read_text(encoding="utf-8")
        return [line.strip() for line in text.splitlines() if line.strip()]
    return _pr_files_from_api()


def _norm(path: str) -> str:
    return os.path.normpath(path).replace(os.sep, "/")


def local_replace_targets(module: str) -> list[str]:
    """Repo-relative directories that `module`'s go.mod replaces a dependency with."""
    gomod = Path(module) / "go.mod"
    if not gomod.is_file():
        return []
    targets = []
    in_block = False
    for raw in gomod.read_text(encoding="utf-8").splitlines():
        line = raw.split("//", 1)[0].strip()
        if not line:
            continue
        if line.startswith("replace") and line.endswith("("):
            in_block = True
            continue
        if in_block and line == ")":
            in_block = False
            continue
        if not (in_block or line.startswith("replace ")):
            continue
        m = REPLACE_LINE.match(line)
        if not m:
            continue
        target = m.group(1)
        # Only a filesystem path is local; `example.com/x v1.2.3` is a module.
        if target.startswith("./") or target.startswith("../") or target in (".", ".."):
            targets.append(_norm(os.path.join(module, target)))
    return targets


def module_closure(module: str) -> list[str]:
    """`module` plus every local replace target it reaches, transitively."""
    seen = [_norm(module)]
    queue = [_norm(module)]
    while queue:
        for target in local_replace_targets(queue.pop()):
            if target not in seen:
                seen.append(target)
                queue.append(target)
    return seen


def _under(path: str, directory: str) -> bool:
    return path == directory or path.startswith(directory + "/")


def cmd_go_modules(modules: list[str]) -> int:
    if not modules:
        print("usage: ci-scan.py go-modules MODULE...", file=sys.stderr)
        return 2
    changed = changed_files()
    rows = []
    if changed is None:
        selected = list(modules)
        reason = "not a pull request, or its file list could not be read: every module"
        rows = [(m, "full scan") for m in modules]
    elif any(f in SCAN_EVERYTHING_WHEN_CHANGED for f in changed):
        selected = list(modules)
        reason = "the PR changes the Security workflow or its scoping script: every module"
        rows = [(m, "scoping changed") for m in modules]
    else:
        selected = []
        for m in modules:
            hit = next(
                ((f, d) for d in module_closure(m) for f in changed if _under(_norm(f), d)),
                None,
            )
            if hit:
                selected.append(m)
                via = "" if hit[1] == _norm(m) else f" (via replace `{hit[1]}`)"
                rows.append((m, f"`{hit[0]}`{via}"))
            else:
                rows.append((m, "not touched -- skipped"))
        reason = f"the PR touches {len(selected)} of {len(modules)} modules"
    if len(selected) == len(modules):
        mode = "full"
    elif selected:
        mode = "partial"
    else:
        mode = "none"
    emit(mode=mode, modules=" ".join(selected))
    table = "\n".join(f"| `{m}` | {why} |" for m, why in rows)
    summarize(f"#### Scan scope: {mode}\n\n{reason}.\n\n| module | why |\n|---|---|\n{table}\n")
    return 0


# gitignore-syntax metacharacters. Next.js route directories are named `[id]`,
# which unescaped is a one-character class and matches nothing.
GLOB_META = re.compile(r"([\\*?\[\]])")


def cmd_semgrep_includes(outfile: str) -> int:
    """One `--include` pattern per changed file that still exists.

    Patterns, not file arguments: Semgrep scans a file named on the command line
    even where its default ignore list (tests/, build/, node_modules/, the
    .gitignore) would skip it in a whole-tree scan -- measured on semgrep 1.177,
    four findings from explicit paths against one from `.` on the same tree. An
    `--include` against `.` keeps the ignore list, so a PR scan reports a subset
    of what the full scan would, never a superset. Each pattern is anchored with
    a leading `/` so `app/x.py` cannot also match `web/app/x.py`.
    """
    changed = changed_files()
    if changed is None:
        Path(outfile).write_text("", encoding="utf-8")
        emit(mode="full")
        summarize("#### Scan scope: full\n\nNot a pull request, or its file list could not be read: the whole tree.\n")
        return 0
    if any(f in SCAN_EVERYTHING_WHEN_CHANGED for f in changed):
        Path(outfile).write_text("", encoding="utf-8")
        emit(mode="full")
        summarize("#### Scan scope: full\n\nThe PR changes the Security workflow or its scoping script: the whole tree.\n")
        return 0
    # Deleted files and a rename's old path no longer exist to scan.
    present = sorted({f for f in changed if Path(f).is_file()})
    patterns = ["/" + GLOB_META.sub(r"\\\1", f) for f in present]
    Path(outfile).write_text("".join(f"{p}\n" for p in patterns), encoding="utf-8")
    mode = "partial" if present else "none"
    emit(mode=mode)
    summarize(f"#### Scan scope: {mode}\n\nThe PR's {len(present)} changed file(s) that still exist.\n")
    return 0


def cmd_merge_sarif(out: str, pairs: list[str]) -> int:
    """One SARIF run from several gosec runs, with module-relative URIs made
    repo-relative. One run, not several: code scanning rejects a document that
    carries more than one run for the same tool and category."""
    if not pairs:
        print("usage: ci-scan.py merge-sarif OUT MODULE=FILE...", file=sys.stderr)
        return 2
    merged = None
    rules: dict[str, dict] = {}
    # gosec lists only the CWEs its own findings cite, so each module's taxonomy
    # is a different subset; keeping just the first would orphan the rest.
    taxonomies: dict[str, dict] = {}
    results: list[dict] = []
    for pair in pairs:
        module, sep, path = pair.partition("=")
        if not sep:
            print(f"merge-sarif: expected MODULE=FILE, got {pair!r}", file=sys.stderr)
            return 2
        if not Path(path).is_file():
            # The scanner for this module produced nothing. Leave the gap: if
            # every input is missing the merged file is never written, and
            # summarize-sarif.sh fails the job on its absence.
            print(f"merge-sarif: {path} is missing; {module} contributes nothing")
            continue
        doc = json.loads(Path(path).read_text(encoding="utf-8"))
        for run in doc.get("runs", []):
            if merged is None:
                merged = {k: v for k, v in doc.items() if k != "runs"}
                merged["runs"] = [{k: v for k, v in run.items() if k != "results"}]
            for rule in run.get("tool", {}).get("driver", {}).get("rules", []) or []:
                rules.setdefault(rule.get("id"), rule)
            for tax in run.get("taxonomies", []) or []:
                kept = taxonomies.setdefault(tax.get("guid") or tax.get("name"), {**tax, "taxa": []})
                known = {t.get("id") for t in kept["taxa"]}
                kept["taxa"] += [t for t in tax.get("taxa", []) or [] if t.get("id") not in known]
            prefix = _norm(module) + "/"
            for result in run.get("results", []) or []:
                for loc in result.get("locations", []) or []:
                    art = loc.get("physicalLocation", {}).get("artifactLocation", {})
                    uri = art.get("uri")
                    if uri and "://" not in uri and not uri.startswith("/") and not uri.startswith(prefix):
                        art["uri"] = prefix + uri
                results.append(result)
    if merged is None:
        print("merge-sarif: no input SARIF existed; writing nothing")
        return 0
    run = merged["runs"][0]
    run.setdefault("tool", {}).setdefault("driver", {})["rules"] = list(rules.values())
    if taxonomies:
        run["taxonomies"] = list(taxonomies.values())
    # A ruleIndex points into ONE input's rule array; re-point it into the union.
    # (gosec 2.x writes ruleId only, so today this changes nothing.)
    index = {rule_id: i for i, rule_id in enumerate(rules)}
    for result in results:
        if "ruleIndex" in result and result.get("ruleId") in index:
            result["ruleIndex"] = index[result["ruleId"]]
    run["results"] = results
    Path(out).write_text(json.dumps(merged, indent=2), encoding="utf-8")
    print(f"merge-sarif: {len(results)} result(s), {len(rules)} rule(s) -> {out}")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "go-modules":
        return cmd_go_modules(argv[2:])
    if len(argv) == 3 and argv[1] == "semgrep-includes":
        return cmd_semgrep_includes(argv[2])
    if len(argv) >= 3 and argv[1] == "merge-sarif":
        return cmd_merge_sarif(argv[2], argv[3:])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
