"""Pin llm-service's log-masking key list to shared/sensitive_keys_golden.json.

api-gateway and backend-orchestrator each keep a Go ``SensitiveKeys`` list and pin it
to the same file (``internal/security/masking_golden_test.go``). This copy is the
credential group alone, on purpose: ``mask_dict``/``config_summary`` mask connector
configs by substring, where a PII key such as ``address`` would also hide
``host_address``. A credential key added in Go and not here would leak that field
from this service's logs.
"""

import json
import os

from src.utils.masking import SENSITIVE_KEYS

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_GOLDEN_PATH = os.path.join(REPO_ROOT, "shared", "sensitive_keys_golden.json")


def test_sensitive_keys_are_the_shared_credential_group():
    with open(_GOLDEN_PATH) as f:
        golden = json.load(f)
    credential_keys = golden["credential_keys"]
    assert credential_keys, "golden credential_keys is empty"
    assert golden["pii_keys"], "golden pii_keys is empty"
    assert SENSITIVE_KEYS == set(credential_keys), (
        f"SENSITIVE_KEYS drifted from shared/sensitive_keys_golden.json: "
        f"missing {sorted(set(credential_keys) - SENSITIVE_KEYS)}, "
        f"extra {sorted(SENSITIVE_KEYS - set(credential_keys))}"
    )
