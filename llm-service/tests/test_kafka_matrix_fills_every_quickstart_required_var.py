"""The Kafka security matrix renders docker-compose.quickstart.yml with a clean
env, so every `${VAR:?}` in that file needs a placeholder or `docker compose
config` aborts and the whole matrix fails before a single cell runs.

It did, on #1253: the file gained BLOB_STAGING_ACCESS_KEY/SECRET_KEY, the
harness's hand-written placeholder list did not, and the render died with
"required variable BLOB_STAGING_ACCESS_KEY is missing a value". The class is any
new required var in the quickstart, so this reads the file, not a list.
"""

import importlib.util
import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
HARNESS = os.path.join(ROOT, "deploy/helm/rsync-ai/test/kind/kafka-matrix/run.py")
QUICKSTART = os.path.join(ROOT, "docker-compose.quickstart.yml")


def _required_vars():
    with open(QUICKSTART) as f:
        return set(re.findall(r"\$\{([A-Z0-9_]+):\?", f.read()))


def _harness():
    spec = importlib.util.spec_from_file_location("kafka_matrix_run", HARNESS)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_quickstart_has_required_vars_to_fill():
    # Non-zero control: an empty set would make the next test pass vacuously.
    assert len(_required_vars()) >= 5


def test_the_matrix_fills_every_required_var_of_the_quickstart():
    missing = sorted(_required_vars() - set(_harness().QUICKSTART_DUMMIES))
    assert not missing, (
        f"docker-compose.quickstart.yml requires {missing}, which the Kafka matrix "
        f"does not fill; its render aborts and every cell fails (run.py QUICKSTART_DUMMIES)"
    )
