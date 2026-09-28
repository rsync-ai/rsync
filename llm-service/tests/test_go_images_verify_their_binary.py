"""Every image that ships a Go binary proves the binary is whole before it ships.

On 2026-09-24 a `backend-temporal-adapter` image was tagged holding a binary cut
off at exactly 20 MiB; the container crash-looped with exit 139 and every backend
PR's batch smoke stalled at `intent`. The build that produced it (run 36026825865,
paid-layer boundary) lost its runner mid-job, and the shared buildkit cache handed
the result to the next `rsync-ci` build. Nothing in the Dockerfile noticed, because
nothing looked: `go build` exiting 0 was the only check, and a torn file after that
point -- in the copy, the snapshot, a cache hit -- passes straight through.

So each Go Dockerfile does two things, and this file holds every one to both:

  * the RUN that builds the binary also runs `go version -m <out>` on it -- which
    exits 1 on a file missing even its last byte (the build info and section table
    sit at the end) -- and records `sha256sum <name> > <name>.sha256`;
  * the stage that COPYs the binary also COPYs that hash and checks it with
    `sha256sum -c`, from the directory the binary landed in, in a RUN of its own.
    A separate RUN matters: a failed check is never cached, so a torn layer that
    comes back from the cache still fails here.

Proved on real builds of all five images (2026-09-27): each prints `<name>: OK`; a
20 MiB truncate after the hash fails the runtime stage (`FAILED`), and one before
`go version -m` fails the builder (`unrecognized file format`).
"""

import pathlib
import posixpath
import re
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]

GO_BUILD = re.compile(r"\bgo build\b.*?\s-o\s+(\S+)")
LEADING_CD = re.compile(r"^cd\s+(\S+)\s*&&")


def _go_dockerfiles():
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z"], capture_output=True, text=True, check=True
    ).stdout
    names = [n for n in out.split("\0") if n and posixpath.basename(n).startswith("Dockerfile")]
    return sorted(n for n in names if (REPO / n).is_file() and GO_BUILD.search(_joined(REPO / n)))


def _instructions(text):
    """(KEYWORD, args) per instruction: continuations joined, comment lines dropped."""
    out, buf = [], ""
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.endswith("\\"):
            buf += s[:-1] + " "
            continue
        kw, _, rest = (buf + s).partition(" ")
        out.append((kw.upper(), " ".join(rest.split())))
        buf = ""
    return out


def _joined(path):
    return "\n".join(f"{k} {a}" for k, a in _instructions(path.read_text()))


def _stages(path):
    stages = []
    for kw, args in _instructions(path.read_text()):
        if kw == "FROM":
            m = re.search(r"\bAS\s+(\S+)$", args, re.I)
            stages.append({"name": m.group(1) if m else str(len(stages)), "instrs": []})
        elif stages:
            stages[-1]["instrs"].append((kw, args))
    return stages


def _copy(args):
    toks = args.split()
    frm = next((t.split("=", 1)[1] for t in toks if t.startswith("--from=")), None)
    paths = [t for t in toks if not t.startswith("--")]
    return frm, paths[:-1], paths[-1]


def _built_binaries(path):
    """(stage index, stage name, -o output) for every `go build` in the file."""
    for i, st in enumerate(_stages(path)):
        for kw, args in st["instrs"]:
            if kw == "RUN":
                for m in GO_BUILD.finditer(args):
                    yield i, st["name"], m.group(1), args


def test_the_census_finds_the_incident_image():
    """An empty enumeration would pass every check below on nothing."""
    found = _go_dockerfiles()
    assert "backend-temporal-adapter/Dockerfile" in found, (
        f"the Go-Dockerfile census no longer sees the image this file exists for; "
        f"found {found}. The enumeration broke."
    )


@pytest.mark.parametrize("dockerfile", _go_dockerfiles())
def test_the_build_rejects_an_incomplete_binary_and_hashes_it(dockerfile):
    path = REPO / dockerfile
    built = list(_built_binaries(path))
    assert built
    for _, _, out, run in built:
        name = posixpath.basename(out)
        after = run[GO_BUILD.search(run).end():]
        assert f"go version -m {out} " in after + " ", (
            f"{dockerfile}: the RUN that builds {out} never runs `go version -m {out}` "
            f"after it. `go build` exiting 0 does not mean the file on disk is whole."
        )
        assert f"sha256sum {name} > {name}.sha256" in after, (
            f"{dockerfile}: the RUN that builds {out} records no `{name}.sha256`, so "
            f"the stage that copies it has nothing to check the copy against."
        )


@pytest.mark.parametrize("dockerfile", _go_dockerfiles())
def test_the_stage_that_receives_the_binary_checks_it(dockerfile):
    path = REPO / dockerfile
    stages = _stages(path)
    for src_idx, src_name, out, _ in _built_binaries(path):
        name = posixpath.basename(out)
        receivers = 0
        for st in stages[src_idx + 1:]:
            workdir, landed, hash_copied = "/", None, False
            for kw, args in st["instrs"]:
                if kw == "WORKDIR":
                    workdir = posixpath.join(workdir, args)
                elif kw == "COPY" and args.startswith("--") and f"--from={src_name}" in args:
                    frm, srcs, dest = _copy(args)
                    if any(posixpath.basename(s) == name for s in srcs):
                        dest = posixpath.normpath(posixpath.join(workdir, dest))
                        landed = dest if args.split()[-1].endswith(("/", ".")) else posixpath.dirname(dest)
                        assert args.split()[-1].endswith(("/", ".")) or posixpath.basename(dest) == name, (
                            f"{dockerfile}: {name} is copied in as {dest}; `sha256sum -c` "
                            f"checks by the built name, so it would look for a file that is not there"
                        )
                        receivers += 1
                    if any(posixpath.basename(s) == f"{name}.sha256" for s in srcs):
                        hash_copied = True
                elif kw == "RUN" and landed and "sha256sum -c" in args and f"{name}.sha256" in args:
                    m = LEADING_CD.match(args)
                    cwd = posixpath.normpath(posixpath.join(workdir, m.group(1))) if m else workdir
                    assert hash_copied, f"{dockerfile}: checks {name}.sha256 before copying it in"
                    assert cwd == landed, (
                        f"{dockerfile}: checks {name} from {cwd}, but it landed in {landed}; "
                        f"`sha256sum -c` would not find it"
                    )
                    landed = "verified"
            assert landed in (None, "verified"), (
                f"{dockerfile}: stage `{st['name']}` copies {name} from `{src_name}` and "
                f"never checks it against {name}.sha256. A binary torn after `go build` -- "
                f"in the copy, a snapshot, a cache hit -- would ship as it is."
            )
        assert receivers, f"{dockerfile}: no later stage copies the {out} that `{src_name}` builds"
