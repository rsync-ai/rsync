"""Emit Java client properties from the Debezium connector's OWN schema-history
builder, for RoundTrip.java to use as-is.

Loads connector.py from /connector (the connector's current version dir, mounted
read-only) and calls _schema_history_security() -- the function whose output the
connector puts into every CDC connector config it submits -- with nothing but the
container's environment. The producer prefix is stripped; producer and consumer
entries are identical by construction (test_debezium_schema_history_parity.py and
the connector's own tests pin that).

Output goes to stdout only: it carries SASL secrets, and run.py pipes it straight
into the JVM probe's stdin rather than onto disk.
"""
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("debezium_connector", "/connector/connector.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

try:
    props = mod._schema_history_security()
except Exception as e:  # noqa: BLE001 -- any refusal is a result, not a crash
    print(f"RESULT FAIL jvm CONFIG_REJECTED: {type(e).__name__}: {e}", file=sys.stderr)
    sys.exit(3)

PREFIX = "schema.history.internal.producer."


def esc(value: str) -> str:
    # java.util.Properties escaping for values: a backslash is an escape
    # character and a raw newline ends the entry.
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")


print("bootstrap.servers=" + esc(mod.DebeziumConnector()._bootstrap_from_args({})))
for key, value in sorted(props.items()):
    if key.startswith(PREFIX):
        print(f"{key[len(PREFIX):]}={esc(value)}")
