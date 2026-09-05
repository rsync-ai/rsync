"""No routable IP addresses in anything shipped to a reader.

This repository is going public. Every `.md`, `.example` and `.env*` file in it is
read by strangers, and several carried the real addresses of machines: the prod VM,
a throwaway Azure test box, and -- inside a quoted `pg_hba.conf` error -- the public
egress address of the network the tests were run from.

None of them were secrets, and that is exactly why they survived. `gitleaks` looks
for credentials and finds nothing to flag in an IP address; a reviewer reads
`DOMAIN=YOUR_SERVER_IP (e.g. 203.0.113.10)` as documentation, because it IS
documentation -- it just happens to document a real host. The addresses were only
removed once someone went looking for them specifically, which is not a process
that repeats.

So the check runs every build. It covers documents and templates, not code and not
test fixtures: a compose file pinning a service address is configuration, whereas a
README naming a host is a disclosure.

RFC 5737 (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24) reserves address space for
exactly this purpose, and RFC 3849 does the same for IPv6. Documentation should use
those, and any private, loopback or link-local address is fine as well.
"""

from __future__ import annotations

import ipaddress
import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# Suffixes a reader consumes directly.
SUFFIXES = (".md", ".example", ".txt", ".rst")

# A dotted quad NOT embedded in a longer dotted number. The negative lookarounds
# are the whole reason this test is maintainable: Oracle reports its version as
# `23.5.0.24.7`, and a bare four-octet regex clips that to `23.5.0.24` and calls it
# a public address. Three such false hits exist in this repo today. Excluding them
# structurally beats an allowlist of version strings, which would need a new entry
# every time Oracle ships a release.
IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")

# Documentation ranges, per RFC 5737.
DOCUMENTATION = tuple(
    ipaddress.IPv4Network(n)
    for n in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")
)


def _is_publishable(ip: ipaddress.IPv4Address) -> bool:
    if any(ip in net for net in DOCUMENTATION):
        return True
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_unspecified
        or ip.is_reserved
    )


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z"],
        capture_output=True, text=True, timeout=120, check=True,
    ).stdout
    return [rel for rel in out.split("\0") if rel]


def _candidate_files() -> list[str]:
    return [
        rel for rel in _tracked_files()
        if rel.endswith(SUFFIXES) or ".env" in Path(rel).name
    ]


def _offences() -> list[str]:
    found = []
    for rel in _candidate_files():
        try:
            text = (REPO / rel).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for number, line in enumerate(text.splitlines(), 1):
            for match in IPV4.finditer(line):
                try:
                    ip = ipaddress.IPv4Address(match.group())
                except ValueError:
                    continue  # 999.1.1.1 and friends
                if not _is_publishable(ip):
                    found.append(f"{rel}:{number}  {ip}  | {line.strip()[:100]}")
    return found


def test_no_routable_address_is_published():
    offences = _offences()
    assert not offences, (
        "documents or templates name real, routable IP addresses:\n  "
        + "\n  ".join(offences)
        + "\n\nUse an RFC 5737 documentation address (203.0.113.10) for an example, or "
        "redact it. These files are read by strangers once the repository is public, "
        "and an address here documents a real host.\n\nIf the flagged text is a "
        "FOUR-component version rather than an address (`msodbcsql18 18.6.2.1-1`), "
        "shorten it in the document -- do not loosen the regex. Five components are "
        "excluded structurally because they cannot be an address; four are genuinely "
        "ambiguous, and 18.6.2.1 really does route. A doc almost never needs the "
        "fourth component to make its point."
    )


# The floor is a PROPORTION of the tracked tree, not a count.
#
# It read `> 150` until 2026-09-04, calibrated by hand against the private repo's
# ~200 candidates. The public cut removes about a third of this repo's documentation
# (scripts/flip/excludes.txt), leaving 142 -- so the guard failed on the tree it
# matters most on, having never once flagged a real address. That is the same defect
# the runbook's own `len(jobs) >= 15` fixed by deriving instead of pinning: a literal
# calibrated against one tree is an assertion about that tree, not about the census.
#
# 1-in-40 is far below any plausible documentation share (this repo runs ~8%, the cut
# tree ~6.5%) and far above what a broken census returns. The absolute floor catches
# a tree so small the ratio stops discriminating.
_MIN_CANDIDATE_SHARE = 40  # i.e. >= 1 candidate per 40 tracked files
_MIN_CANDIDATES = 25


def test_the_scan_actually_reads_a_meaningful_corpus():
    """Anti-vacuity: the assertion above passes on an empty file list."""
    tracked = _tracked_files()
    candidates = _candidate_files()
    floor = max(_MIN_CANDIDATES, len(tracked) // _MIN_CANDIDATE_SHARE)
    assert len(candidates) >= floor, (
        f"only {len(candidates)} candidate files found out of {len(tracked)} tracked; "
        f"expected at least {floor}. The census is broken and the check above is "
        f"passing on nothing."
    )
    assert any(c.endswith(".example") for c in candidates), "no .example template scanned"
    assert any(c.endswith(".md") for c in candidates), "no markdown scanned"


def test_the_matcher_finds_a_routable_address_when_one_is_present():
    """And that it discriminates -- three separate ways it could fail open."""
    # It must catch a real one.
    assert [m.group() for m in IPV4.finditer("host 8.8.8.8 replied")] == ["8.8.8.8"]
    assert not _is_publishable(ipaddress.IPv4Address("8.8.8.8"))

    # It must not catch the documentation range, or this test would fail on its own
    # remediation advice.
    assert _is_publishable(ipaddress.IPv4Address("203.0.113.10"))
    assert _is_publishable(ipaddress.IPv4Address("10.0.0.1"))
    assert _is_publishable(ipaddress.IPv4Address("127.0.0.1"))

    # And it must not clip a longer dotted version into a plausible address. This is
    # the real-world false positive: `Oracle 23.5.0.24.7` in CAPABILITIES-ARCHIVE.md.
    assert IPV4.findall("real Oracle 23.5.0.24.7 (gvenzl/") == []
    assert IPV4.findall('"Oracle Database 23ai Free | 23.6.0.24.10"') == []
    assert IPV4.findall("version 1.2.3.4.5") == []
    # ...while still matching the same digits when they genuinely stand alone.
    assert IPV4.findall("connect to 23.5.0.24 now") == ["23.5.0.24"]

    # A FOUR-component version is deliberately NOT exempt, and this case pins that.
    # `msodbcsql18 18.6.2.1-1` tripped the check in #911; 18.6.2.1 is real AWS space,
    # and no property of the digits separates a Debian revision from an address. The
    # remedy is to shorten the version in the document -- exempting a trailing `-N`
    # would silently publish any address someone wrote as part of a range.
    assert IPV4.findall("msodbcsql18 18.6.2.1-1 installed") == ["18.6.2.1"]
    assert not _is_publishable(ipaddress.IPv4Address("18.6.2.1"))
