"""A Go service that builds an OTLP exporter must collapse its export errors.

The defect this guards, measured on a self-host install: 500 of the
orchestrator's last 500 log lines were ``traces export: ... connection refused``
and ten minutes of its log held nothing else. The OTel SDK reports every failed
batch export through ``otel.Handle``, and the SDK's default handler writes each
one to the standard library logger -- one unstructured line per attempt, no
backoff, no cap, for as long as the collector is unreachable.

``OTEL_ENABLED=false`` in docker-compose.quickstart.yml stops the exporter being
built at all, which covers the self-host bundle. It cannot cover the other half:
cloud runs a collector deliberately, so a collector that restarts or goes away
produces the identical spew with telemetry correctly configured. The fix is a
rate-limiting ``otel.SetErrorHandler`` installed before the exporter exists.

The three Go services that build an exporter are separate modules with no shared
telemetry package, so the handler is a byte-identical copy in each -- the same
patch-both-or-neither shape as the Go/Python log scrubbers. That is only safe if
drift is a CI failure, which is what this file makes it. The behavioural half
(first error always logged, repeats suppressed, condition restated with a count)
lives in backend-orchestrator/internal/telemetry/otel_error_handler_test.go; one
copy is enough to prove the logic precisely because this file proves the copies
are equal.

Discovery is by scan, not by list, so a fourth Go service that starts exporting
spans is covered the day it is written. The scan is armed against finding
nothing: a glob that silently matches zero files would pass every assertion
below, which is the failure mode this repo has hit before.
"""

import os
import re

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HANDLER_FILENAME = "otel_error_handler.go"
INSTALL_CALL = "installCollapsingErrorHandler()"
EXPORTER_CTOR = "otlptracegrpc.New("

# The Go modules in this repo. Scanned rather than assumed so that a new module
# is not silently skipped.
SKIP_DIRS = {".git", "node_modules", "vendor", ".claude", "venv", ".venv"}

# The packages that build an exporter on the day this guard was written, named
# as `os.path.join(REPO_ROOT, ...)` globs for two independent reasons.
#
# They arm the scan: a walk that matched nothing would satisfy every assertion
# below, which is the vacuous-pass shape this repo has been bitten by before.
#
# And they are the shape the CI-filter census reads. That census
# (test_ci_filter_covers_every_guard_subject.py) learns a guard's subjects by
# AST-walking its source for `os.path.join(REPO_ROOT, ...)` calls and bare path
# literals. A guard that finds its files with os.walk names none of them, so its
# enrolment there would check nothing while looking exactly like a guard whose
# subjects are all covered -- the same "a skipped job looks like a passing one"
# failure the census exists to catch. Writing the three packages here makes the
# enrolment real. All three were added to the `llm` paths filter in the same
# change as this file.
KNOWN_EXPORTER_GLOBS = [
    os.path.join(REPO_ROOT, "backend-orchestrator", "internal", "telemetry", "*.go"),
    os.path.join(REPO_ROOT, "api-gateway", "internal", "telemetry", "*.go"),
    os.path.join(REPO_ROOT, "backend-temporal-adapter", "internal", "telemetry", "*.go"),
]

KNOWN_EXPORTERS = {os.path.relpath(os.path.dirname(g), REPO_ROOT) for g in KNOWN_EXPORTER_GLOBS}


def _walk_go_files():
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(".go") and not name.endswith("_test.go"):
                yield os.path.join(dirpath, name)


def _read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def exporter_files():
    """Every non-test Go file that constructs an OTLP trace exporter."""
    found = []
    for path in _walk_go_files():
        if EXPORTER_CTOR in _read(path):
            found.append(os.path.relpath(path, REPO_ROOT))
    return sorted(found)


def handler_files():
    """Every copy of the collapsing error handler."""
    found = []
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        if HANDLER_FILENAME in filenames:
            path = os.path.join(dirpath, HANDLER_FILENAME)
            found.append(os.path.relpath(path, REPO_ROOT))
    return sorted(found)


def test_the_scan_finds_the_services_that_export_spans():
    """Arm the census. Zero matches would satisfy every other test in this file."""
    found = exporter_files()
    assert found, (
        f"no Go file in the tree constructs {EXPORTER_CTOR!r} -- either every service "
        "stopped exporting spans, or this scan is broken and the rest of this file "
        "is passing vacuously"
    )
    packages = {os.path.dirname(rel) for rel in found}
    missing = KNOWN_EXPORTERS - packages
    assert not missing, (
        f"the scan no longer sees {sorted(missing)}, which built OTLP exporters when "
        "this guard was written -- if a service genuinely stopped exporting, drop it "
        "from KNOWN_EXPORTERS deliberately rather than letting the census shrink"
    )


@pytest.mark.parametrize("rel", exporter_files())
def test_every_exporter_package_ships_the_handler(rel):
    pkg_dir = os.path.join(REPO_ROOT, os.path.dirname(rel))
    handler = os.path.join(pkg_dir, HANDLER_FILENAME)
    assert os.path.exists(handler), (
        f"{rel} builds an OTLP exporter but its package has no {HANDLER_FILENAME}. "
        "Without one, the SDK's default error handler is on duty and a collector "
        "that is merely absent turns this service's log into a single repeated line."
    )


@pytest.mark.parametrize("rel", exporter_files())
def test_the_handler_is_installed_before_the_exporter_exists(rel):
    text = _read(os.path.join(REPO_ROOT, rel))
    install_at = text.find(INSTALL_CALL)
    assert install_at != -1, f"{rel} never calls {INSTALL_CALL}"

    ctor_at = text.find(EXPORTER_CTOR)
    assert install_at < ctor_at, (
        f"{rel} calls {INSTALL_CALL} at offset {install_at}, after it builds the "
        f"exporter at {ctor_at}. Registering the handler afterwards leaves a window "
        "in which the SDK's default handler is the one on duty."
    )


def test_the_handler_copies_are_byte_identical():
    copies = handler_files()
    assert len(copies) >= len(KNOWN_EXPORTERS), (
        f"found {len(copies)} copies of {HANDLER_FILENAME}, expected at least "
        f"{len(KNOWN_EXPORTERS)}: {copies}"
    )

    first_rel = copies[0]
    first_text = _read(os.path.join(REPO_ROOT, first_rel))
    for rel in copies[1:]:
        assert _read(os.path.join(REPO_ROOT, rel)) == first_text, (
            f"{rel} has drifted from {first_rel}. These are separate Go modules with "
            "no shared telemetry package, so the copies stay in lockstep by this "
            "guard and nothing else: patch all of them or none."
        )


@pytest.mark.parametrize("rel", handler_files())
def test_the_handler_actually_registers_with_the_sdk(rel):
    text = _read(os.path.join(REPO_ROOT, rel))
    assert "otel.SetErrorHandler(" in text, (
        f"{rel} defines a handler but never calls otel.SetErrorHandler, so the SDK "
        "keeps writing every export failure to the standard logger"
    )


@pytest.mark.parametrize("rel", handler_files())
def test_the_handler_does_not_silence_the_first_failure(rel):
    """Two-sided on purpose.

    A handler that dropped every error would pass every structural assertion
    above while trading a flooded log for no log at all -- which is the one
    outcome worse than the defect. The first failure has to reach logrus
    unconditionally, and the suppressed ones have to be counted.
    """
    text = _read(os.path.join(REPO_ROOT, rel))
    assert re.search(r"if\s+!h\.started\s*{", text), (
        f"{rel} has no always-log-the-first-failure branch"
    )
    assert "h.suppressed++" in text, f"{rel} never counts what it holds back"
    assert re.search(r'WithField\("suppressed"', text), (
        f"{rel} never reports how many failures it suppressed, so a continuing "
        "outage is invisible rather than merely quiet"
    )
