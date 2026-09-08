"""The OTLP exporter must not be attached when no collector exists.

The defect this guards: ``init_tracer`` built an ``OTLPSpanExporter`` and attached
a ``BatchSpanProcessor`` unconditionally, with a try/except around it that reads
like a safety net and is not one -- constructing a gRPC exporter opens no
connection, so nothing raises, the service logs the green
"OpenTelemetry initialized" line, and the batch processor then retries every span
batch at ``localhost:4317`` for the life of the process, blocking its worker for
the full export timeout each time. ``init_metrics`` in the telemetry agent had the
identical shape with a reader that wakes every 10 seconds.

Both are now gated on ``otel_enabled()``. The default is ENABLED, matching cloud,
which runs a collector; only ``docker-compose.quickstart.yml`` turns it off,
because that bundle ships none and its observability story is ``docker logs``.

Every assertion here is two-sided on purpose: a gate that disables export
unconditionally would fix the retry loop and silently break cloud tracing, and a
one-sided test could not tell those apart.
"""

import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import telemetry as tel  # noqa: E402


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, True),        # unset -> cloud default
        ("", True),          # compose renders an unset ${VAR:-} as empty: still unset
        ("   ", True),
        ("true", True),
        ("TRUE", True),
        ("1", True),
        ("yes", True),
        ("on", True),
        ("false", False),
        ("FALSE", False),
        ("0", False),
        ("off", False),
        ("no", False),
        ("nonsense", False),  # env_bool's own semantics: anything unrecognised is not true
    ],
)
def test_otel_enabled_reads_the_flag_and_defaults_to_cloud(monkeypatch, raw, expected):
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    if raw is not None:
        monkeypatch.setenv("OTEL_ENABLED", raw)
    assert tel.otel_enabled() is expected


class _Recorder:
    """Stands in for the exporter/processor classes and records construction."""

    def __init__(self):
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        return _NullProcessor()


class _NullProcessor:
    """Enough of a SpanProcessor for TracerProvider.add_span_processor."""

    def on_start(self, span, parent_context=None):
        pass

    def on_end(self, span):
        pass

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis=30000):
        return True


def _init_with_flag(monkeypatch, value):
    """Run init_tracer with OTEL_ENABLED=value, counting exporter construction."""
    exporter = _Recorder()
    processor = _Recorder()
    monkeypatch.setattr(tel, "OTLPSpanExporter", exporter)
    monkeypatch.setattr(tel, "BatchSpanProcessor", processor)
    if value is None:
        monkeypatch.delenv("OTEL_ENABLED", raising=False)
    else:
        monkeypatch.setenv("OTEL_ENABLED", value)
    tracer = tel.init_tracer("guard-service", "0.0.1")
    return tracer, exporter.calls, processor.calls


def test_disabled_attaches_no_exporter_and_no_batch_processor(monkeypatch):
    _, exporter_calls, processor_calls = _init_with_flag(monkeypatch, "false")
    assert exporter_calls == 0, (
        "an OTLPSpanExporter was built with OTEL_ENABLED=false; "
        "it retries the endpoint for the life of the process"
    )
    assert processor_calls == 0, "a BatchSpanProcessor was attached with OTEL_ENABLED=false"


def test_enabled_still_attaches_the_exporter(monkeypatch):
    """The other side: the gate must not disable export for cloud."""
    _, exporter_calls, processor_calls = _init_with_flag(monkeypatch, "true")
    assert exporter_calls == 1
    assert processor_calls == 1


def test_unset_still_attaches_the_exporter(monkeypatch):
    """Cloud sets nothing. Unset must keep exporting, per the OSS/cloud split rule."""
    _, exporter_calls, _ = _init_with_flag(monkeypatch, None)
    assert exporter_calls == 1, "OTEL_ENABLED unset must keep the cloud behaviour"


def test_empty_string_still_attaches_the_exporter(monkeypatch):
    """Compose renders an unset ${OTEL_ENABLED:-} as "", which must not disable it."""
    _, exporter_calls, _ = _init_with_flag(monkeypatch, "")
    assert exporter_calls == 1, "an empty value must read as 'operator said nothing'"


def test_disabled_still_returns_a_usable_tracer(monkeypatch):
    """Five call sites hold the return value; spans must keep working, untraced."""
    tracer, _, _ = _init_with_flag(monkeypatch, "false")
    with tracer.start_as_current_span("probe") as span:
        span.set_attribute("k", "v")  # must not raise


def test_telemetry_agent_metrics_use_the_same_gate(monkeypatch):
    """init_metrics had the identical unguarded shape, with a 10s retry loop."""
    svc = pytest.importorskip("src.agents.telemetry.service")

    exporter = _Recorder()
    monkeypatch.setattr(svc, "OTLPMetricExporter", exporter)
    monkeypatch.setattr(svc, "PeriodicExportingMetricReader", lambda *a, **k: None)
    monkeypatch.setattr(svc, "MeterProvider", lambda *a, **k: None)
    monkeypatch.setattr(svc.metrics, "set_meter_provider", lambda *a, **k: None)

    monkeypatch.setenv("OTEL_ENABLED", "false")
    svc.init_metrics()
    assert exporter.calls == 0, "an OTLPMetricExporter was built with OTEL_ENABLED=false"

    monkeypatch.setenv("OTEL_ENABLED", "true")
    svc.init_metrics()
    assert exporter.calls == 1, "the gate must not disable metrics for cloud"
