"""
An example key copied from a template is not an LLM.

.env.example and llm-service/.env.example used to ship
OPENAI_API_KEY=sk-proj-your-openai-key-here under a "LLM (required)" header. A
non-empty key resolved the provider to "openai", so an operator who copied the
template got an install that claimed an LLM was set up: every LLM feature sent
its prompt to api.openai.com with a fake key and failed with an authentication
error, instead of answering "Set up an LLM first".

Two layers, tested separately:
  * the templates ship empty credential slots, so copying one sets no LLM;
  * the resolver ignores the example values this repo has shipped in its
    templates and docs, for the copies of those files that already exist.

Every recognition case has a control: a key in a real format must still count.
Real-format keys are built at runtime so no key-shaped literal sits in the repo.
"""

import logging
import os
import re
import string
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[2]
LLM_SERVICE = Path(__file__).resolve().parents[1]
if str(LLM_SERVICE) not in sys.path:
    sys.path.insert(0, str(LLM_SERVICE))

from src.utils import openai_client  # noqa: E402
from src.utils.openai_client import (  # noqa: E402
    client_egress_host,
    explorer_llm_configured,
    llm_configured,
    make_async_client,
    make_sync_client,
    resolve_explorer_provider,
    resolve_provider,
)

_LLM_ENV = (
    "LLM_PROVIDER",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_API_KEY_SOURCE",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "GROQ_API_KEY",
    "EXPLORER_LLM_PROVIDER",
    "EXPLORER_SQL_PROVIDER",
    "EXPLORER_OFFLINE_ONLY",
    "EXPLORER_SQL_ALLOW_ONLINE",
    "RANK_TABLES_LLM_PROVIDER",
    "USE_MOCK_LLM",
)


@pytest.fixture(autouse=True)
def clean_llm_env(monkeypatch):
    for var in _LLM_ENV:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("OLLAMA_URL", "http://ollama:11434")
    # The placeholder warning is once per process; each test starts fresh.
    monkeypatch.setattr(openai_client, "_PLACEHOLDER_WARNED", set(), raising=False)


def _real_format(prefix: str, length: int, alphabet: str = string.ascii_letters + string.digits) -> str:
    """A key in a provider's real format, generated so no key literal is committed."""
    body = "".join(alphabet[(i * 7 + 3) % len(alphabet)] for i in range(length))
    return prefix + body


REAL_OPENAI_PROJECT_KEY = _real_format("sk-proj-", 156, string.ascii_letters + string.digits + "-_")
REAL_OPENAI_LEGACY_KEY = _real_format("sk-", 48)
REAL_AZURE_KEY = _real_format("", 32, "0123456789abcdef")
REAL_GROQ_KEY = _real_format("gsk_", 52)
REAL_AZURE_ENDPOINT = "https://contoso-ai.openai.azure.com"


# ---------------------------------------------------------------------------
# The templates themselves
# ---------------------------------------------------------------------------

_CREDENTIAL_SLOTS = (
    "OPENAI_API_KEY",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "GROQ_API_KEY",
    "ANTHROPIC_API_KEY",
)
_SKIP_DIRS = {".git", "node_modules", ".next", "__pycache__", ".venv", "venv", "worktrees"}
_ASSIGNMENT = re.compile(r"^\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)\s*=(.*)$")


def _env_templates():
    found = []
    for root, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS]
        for name in files:
            if name.endswith(".example") and ".env" in name:
                found.append(Path(root) / name)
    return sorted(found)


def _assignments(path: Path) -> dict:
    """Uncommented KEY=VALUE lines, read the way an env file is: a copy of the
    template is exactly these values."""
    values = {}
    for line in path.read_text().splitlines():
        match = _ASSIGNMENT.match(line)
        if not match:
            continue
        value = match.group(2)
        value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[match.group(1)] = value
    return values


def _templates_with_llm_settings():
    return [
        (path, values)
        for path in _env_templates()
        for values in [_assignments(path)]
        if any(slot in values for slot in _CREDENTIAL_SLOTS)
    ]


def _llm_section_comments(path: Path) -> str:
    """The comment lines of the block (consecutive non-blank lines) that holds the
    template's first uncommented LLM credential slot: what an operator reads right
    where they would paste a key."""
    lines = path.read_text().splitlines()
    for i, line in enumerate(lines):
        match = _ASSIGNMENT.match(line)
        if not (match and match.group(1) in _CREDENTIAL_SLOTS):
            continue
        start = i
        while start > 0 and lines[start - 1].strip():
            start -= 1
        end = i
        while end + 1 < len(lines) and lines[end + 1].strip():
            end += 1
        return "\n".join(l.strip() for l in lines[start : end + 1] if l.strip().startswith("#"))
    return ""


_DOC_LINK = re.compile(r"\b(docs/[\w./-]+\.md)#([\w-]+)")


def _heading_anchors(path: Path) -> set:
    """The anchors GitHub gives the headings of a Markdown file."""
    anchors = set()
    in_fence = False
    for line in path.read_text().splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        match = None if in_fence else re.match(r"^#{1,6}\s+(.+?)\s*#*\s*$", line)
        if match:
            text = re.sub(r"[^\w\- ]", "", match.group(1).strip().lower())
            anchors.add(text.replace(" ", "-"))
    return anchors


class TestTemplates:
    def test_the_templates_that_name_an_llm_key_are_found(self):
        rel = {str(path.relative_to(REPO_ROOT)) for path, _ in _templates_with_llm_settings()}
        # The two files the docs tell an operator to copy for local use.
        assert {".env.example", "llm-service/.env.example"} <= rel, rel
        assert len(rel) >= 4, rel

    def test_no_template_ships_a_value_in_a_credential_slot(self):
        checked = 0
        filled = []
        for path, values in _templates_with_llm_settings():
            for slot in _CREDENTIAL_SLOTS:
                if slot in values:
                    checked += 1
                    if values[slot]:
                        filled.append(f"{path.relative_to(REPO_ROOT)}: {slot}")
        assert checked >= 6, f"only {checked} credential slots found; the parser is not reading the templates"
        assert filled == [], (
            "A copied template must set no LLM credential. Leave these empty and point "
            f"at docs/deployment/self-hosting.md#which-llm-is-used instead: {filled}"
        )

    def test_a_copy_of_each_template_has_no_llm_set_up(self, monkeypatch):
        templates = _templates_with_llm_settings()
        assert templates
        for path, values in templates:
            for var in _LLM_ENV:
                monkeypatch.delenv(var, raising=False)
                if var in values:
                    monkeypatch.setenv(var, values[var])
            assert llm_configured() is False, path.relative_to(REPO_ROOT)

    def test_the_llm_section_says_optional_and_where_to_read_more(self):
        # These headers used to say "LLM (required)" and "fill in at least one",
        # which is what sent operators to paste an example key.
        checked = set()
        for path, _ in _templates_with_llm_settings():
            rel = str(path.relative_to(REPO_ROOT))
            comments = _llm_section_comments(path)
            assert comments, f"{rel}: no comment above the LLM credential slot"
            assert re.search(r"\boptional\b", comments, re.IGNORECASE), f"{rel}: {comments!r}"
            assert not re.search(r"\brequired\b|fill in at least one", comments, re.IGNORECASE), (
                f"{rel}: {comments!r}"
            )
            links = _DOC_LINK.findall(comments)
            assert len(links) >= 1, f"{rel}: no docs link in {comments!r}"
            for doc, anchor in links:
                target = REPO_ROOT / doc
                assert target.is_file(), f"{rel}: {doc} does not exist"
                assert anchor in _heading_anchors(target), f"{rel}: {doc} has no heading #{anchor}"
            assert ("docs/deployment/self-hosting.md", "which-llm-is-used") in links, f"{rel}: {links}"
            checked.add(rel)
        assert {
            ".env.example",
            ".env.staging.example",
            ".env.prod.example",
            "llm-service/.env.example",
        } <= checked, checked

    def test_the_anchor_reader_tells_a_real_heading_from_a_missing_one(self):
        # Control for the check above: a count of zero headings would pass anything.
        anchors = _heading_anchors(REPO_ROOT / "docs/deployment/self-hosting.md")
        assert "which-llm-is-used" in anchors
        assert "which-llm-is-required" not in anchors


# ---------------------------------------------------------------------------
# Example values this repo has shipped are not credentials
# ---------------------------------------------------------------------------

# Each value appears, or appeared, in a template or doc in this repo.
_SHIPPED_OPENAI_PLACEHOLDERS = [
    "sk-proj-your-openai-key-here",  # .env.example, llm-service/.env.example
    "sk-...",  # .env.staging.example, self-hosting.md, env-vars.md, aws.md
    "sk-proj-...",  # self-hosting.md
    "sk-xxx",  # docs/services/LLM_SERVICE_AI_AGENTS.md
    "your-key-here",  # agents/connector_resolver/README.md
    "<key from Step 2a>",  # DEPLOY_AZURE.md
]
# The same kinds of value, spelled differently.
_SHAPED_PLACEHOLDERS = [
    "your-openai-key-here",
    "YOUR_OPENAI_API_KEY_HERE",
    "sk-your_groq_key_here",
    "  sk-proj-your-openai-key-here  ",
    "SK-XXX",
    "<openai-api-key>",
]


class TestPlaceholdersAreNotAnLLM:
    @pytest.mark.parametrize("value", _SHIPPED_OPENAI_PLACEHOLDERS + _SHAPED_PLACEHOLDERS)
    def test_auto_detect(self, monkeypatch, value):
        monkeypatch.setenv("OPENAI_API_KEY", value)
        assert resolve_provider() == "ollama"
        assert llm_configured() is False

    @pytest.mark.parametrize("value", _SHIPPED_OPENAI_PLACEHOLDERS + _SHAPED_PLACEHOLDERS)
    def test_openai_chosen(self, monkeypatch, value):
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        monkeypatch.setenv("OPENAI_API_KEY", value)
        assert llm_configured() is False

    @pytest.mark.parametrize("value", ["<azure-key>", "<azure-openai-api-key>", "<key>"])
    def test_azure_key(self, monkeypatch, value):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", value)
        assert llm_configured() is False

    @pytest.mark.parametrize(
        "value", ["https://<resource>.openai.azure.com", "https://<your-resource>.openai.azure.com"]
    )
    def test_azure_endpoint_chosen(self, monkeypatch, value):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", value)
        assert llm_configured() is False

    def test_azure_endpoint_auto_detect(self, monkeypatch):
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://<resource>.openai.azure.com")
        assert resolve_provider() == "ollama"
        assert llm_configured() is False

    def test_groq_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "groq")
        monkeypatch.setenv("GROQ_API_KEY", "gsk_...")
        assert llm_configured() is False

    def test_a_placeholder_azure_key_is_what_the_client_would_send(self, monkeypatch):
        # The Azure client reads AZURE_OPENAI_API_KEY first and only falls back to
        # OPENAI_API_KEY when it is empty, so a placeholder there is the key it
        # sends; a real OpenAI key behind it must not make Azure look set up.
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "<azure-key>")
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_LEGACY_KEY)
        assert llm_configured() is False

    def test_explorer_follows_the_same_rule(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-proj-your-openai-key-here")
        monkeypatch.setenv("EXPLORER_LLM_PROVIDER", "openai")
        assert resolve_explorer_provider() == "ollama"
        assert explorer_llm_configured() is False

    def test_no_client_is_pointed_at_openai_with_a_placeholder(self, monkeypatch):
        # A placeholder key used to build a client aimed at api.openai.com, so the
        # prompt left the deployment before the fake key was rejected.
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-proj-your-openai-key-here")
        assert client_egress_host(make_sync_client()) != "api.openai.com"
        assert client_egress_host(make_async_client()) != "api.openai.com"


class TestAzureIsAllOrNothing:
    """The Azure client sends AZURE_OPENAI_ENDPOINT together with a key. If
    either one is still an example value no call can succeed, whatever the
    other one holds."""

    PLACEHOLDER_ENDPOINT = "https://<resource>.openai.azure.com"

    def test_a_placeholder_endpoint_with_a_real_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", self.PLACEHOLDER_ENDPOINT)
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", REAL_AZURE_KEY)
        assert resolve_provider() == "ollama"
        assert llm_configured() is False

    @pytest.mark.parametrize("provider", ["azure", ""])
    def test_a_real_endpoint_with_a_placeholder_key(self, monkeypatch, provider):
        if provider:
            monkeypatch.setenv("LLM_PROVIDER", provider)
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", REAL_AZURE_ENDPOINT)
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "<azure-key>")
        assert resolve_provider() == "ollama"
        assert llm_configured() is False

    def test_a_real_endpoint_with_a_placeholder_openai_key_behind_an_empty_azure_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", REAL_AZURE_ENDPOINT)
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "")
        monkeypatch.setenv("OPENAI_API_KEY", "sk-proj-your-openai-key-here")
        assert llm_configured() is False

    def test_control_a_real_endpoint_and_a_real_key(self, monkeypatch):
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", REAL_AZURE_ENDPOINT)
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", REAL_AZURE_KEY)
        assert resolve_provider() == "azure"
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        assert llm_configured() is True
        client = make_sync_client()
        assert client.api_key == REAL_AZURE_KEY
        assert client_egress_host(client) == "<azure-endpoint>"

    def test_auto_detect_does_not_move_an_unfinished_azure_setup_to_openai(self, monkeypatch):
        # A placeholder endpoint means Azure was meant. Falling through to
        # OPENAI_API_KEY would send prompts to a different vendor than the one
        # the operator was setting up.
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", self.PLACEHOLDER_ENDPOINT)
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_PROJECT_KEY)
        assert resolve_provider() == "ollama"
        assert llm_configured() is False
        assert client_egress_host(make_sync_client()) != "api.openai.com"

    def test_control_no_endpoint_at_all_still_auto_detects_openai(self, monkeypatch):
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "")
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_PROJECT_KEY)
        assert resolve_provider() == "openai"
        assert llm_configured() is True


class TestBlankValues:
    """A variable holding only spaces is set but empty. The clients strip it and
    send a blank key, so it cannot be an LLM, and it must not let the resolver
    look past it to the next variable the client would never reach."""

    @pytest.mark.parametrize("blank", ["   ", "\t", " \t "])
    def test_openai_key(self, monkeypatch, blank):
        monkeypatch.setenv("OPENAI_API_KEY", blank)
        assert resolve_provider() == "ollama"
        assert llm_configured() is False
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        assert llm_configured() is False

    def test_groq_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "groq")
        monkeypatch.setenv("GROQ_API_KEY", "   ")
        assert llm_configured() is False

    def test_azure_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "   ")
        assert llm_configured() is False

    def test_a_blank_azure_key_hides_the_openai_key_behind_it(self, monkeypatch):
        # (os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")) takes
        # "   " and strips it: the client sends a blank key, not the OpenAI one.
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "   ")
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_LEGACY_KEY)
        assert llm_configured() is False

    def test_control_a_blank_openai_key_behind_a_real_azure_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("OPENAI_API_KEY", "   ")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", REAL_AZURE_KEY)
        assert llm_configured() is True
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", REAL_AZURE_ENDPOINT)
        assert make_sync_client().api_key == REAL_AZURE_KEY
        assert make_async_client().api_key == REAL_AZURE_KEY

    def test_a_blank_azure_endpoint_is_no_endpoint(self, monkeypatch):
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "   ")
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_PROJECT_KEY)
        assert resolve_provider() == "openai"
        assert llm_configured() is True


def _near_miss(head: str, tail: str) -> str:
    """A real-format OpenAI key with ``head`` and ``tail`` around its body."""
    body = _real_format("", 40)
    return "sk-proj-" + head + body + tail + body


class TestRealKeysStillCount:
    @pytest.mark.parametrize(
        "value",
        [
            REAL_OPENAI_PROJECT_KEY,
            REAL_OPENAI_LEGACY_KEY,
            "sk-test",  # the fixture style the rest of the suite uses
            # Near misses of the placeholder shapes.
            _real_format("sk-proj-yourteam0Key9here", 40),
            _real_format("sk-proj-your", 40) + "key",
            _near_miss("your_", "keyhere"),  # no separator between key and here
            _near_miss("yours", "key_here"),  # no separator after your
            _near_miss("", "key-here"),  # no "your" at all
            _near_miss("", "<>"),  # angle brackets with nothing to fill in
        ],
    )
    def test_openai(self, monkeypatch, value):
        monkeypatch.setenv("OPENAI_API_KEY", value)
        assert resolve_provider() == "openai"
        assert llm_configured() is True
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        assert llm_configured() is True

    def test_openai_client_still_goes_to_openai(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_PROJECT_KEY)
        assert client_egress_host(make_sync_client()) == "api.openai.com"
        assert client_egress_host(make_async_client()) == "api.openai.com"

    def test_azure_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", REAL_AZURE_KEY)
        assert llm_configured() is True

    def test_azure_endpoint(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", REAL_AZURE_ENDPOINT)
        assert llm_configured() is True
        monkeypatch.delenv("LLM_PROVIDER")
        assert resolve_provider() == "azure"

    def test_azure_falls_back_to_the_openai_key_when_its_own_is_empty(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", "")
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_LEGACY_KEY)
        assert llm_configured() is True

    def test_groq_key(self, monkeypatch):
        monkeypatch.setenv("LLM_PROVIDER", "groq")
        monkeypatch.setenv("GROQ_API_KEY", REAL_GROQ_KEY)
        assert llm_configured() is True


class TestAnEndpointTheOperatorNamed:
    # A local OpenAI-compatible server that ignores keys is commonly given a dummy
    # one. Only that server can judge the key, so it is not second-guessed here.
    @pytest.mark.parametrize("value", ["sk-xxx", "sk-...", "your-key-here"])
    def test_any_key_counts_for_a_custom_base_url(self, monkeypatch, value):
        monkeypatch.setenv("OPENAI_BASE_URL", "http://vllm.internal:8000/v1")
        monkeypatch.setenv("OPENAI_API_KEY", value)
        assert resolve_provider() == "openai"
        assert llm_configured() is True
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        assert llm_configured() is True
        assert client_egress_host(make_sync_client()) == "vllm.internal"

    @pytest.mark.parametrize(
        "base_url",
        [
            "https://api.openai.com/v1",
            "https://eu.api.openai.com/v1",  # a regional OpenAI host is still OpenAI
            "api.openai.com/v1",  # no scheme: no host can be read from it
        ],
    )
    def test_naming_openai_itself_is_not_a_custom_endpoint(self, monkeypatch, base_url):
        monkeypatch.setenv("OPENAI_BASE_URL", base_url)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-xxx")
        assert resolve_provider() == "ollama"
        assert llm_configured() is False
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        assert llm_configured() is False

    @pytest.mark.parametrize("value", ["<key from Step 2a>", "<azure-key>"])
    def test_a_slot_to_fill_in_is_never_a_key(self, monkeypatch, value):
        # DEPLOY_AZURE.md pairs a custom OPENAI_BASE_URL with OPENAI_API_KEY=<key
        # from Step 2a>. A server that ignores keys is given a dummy key, never an
        # unfilled slot.
        monkeypatch.setenv(
            "OPENAI_BASE_URL", "https://contoso-ai.openai.azure.com/openai/deployments/gpt-4o-mini"
        )
        monkeypatch.setenv("OPENAI_API_KEY", value)
        assert resolve_provider() == "ollama"
        assert llm_configured() is False
        monkeypatch.setenv("LLM_PROVIDER", "openai")
        assert llm_configured() is False

    def test_control_a_real_key_for_that_endpoint(self, monkeypatch):
        monkeypatch.setenv(
            "OPENAI_BASE_URL", "https://contoso-ai.openai.azure.com/openai/deployments/gpt-4o-mini"
        )
        monkeypatch.setenv("OPENAI_API_KEY", REAL_AZURE_KEY)
        assert resolve_provider() == "openai"
        assert llm_configured() is True

    @pytest.mark.parametrize("value", ["your-azure-key-here", "<azure-key>"])
    def test_the_azure_key_is_not_covered_by_it(self, monkeypatch, value):
        # The Azure client never reads OPENAI_BASE_URL.
        monkeypatch.setenv("OPENAI_BASE_URL", "http://vllm.internal:8000/v1")
        monkeypatch.setenv("LLM_PROVIDER", "azure")
        monkeypatch.setenv("AZURE_OPENAI_API_KEY", value)
        assert llm_configured() is False


# ---------------------------------------------------------------------------
# What the operator sees
# ---------------------------------------------------------------------------

class TestOperatorSees:
    def test_the_log_names_the_variable_once_and_never_its_value(self, monkeypatch, caplog):
        value = "sk-proj-your-openai-key-here"
        monkeypatch.setenv("OPENAI_API_KEY", value)
        with caplog.at_level(logging.WARNING):
            llm_configured()
            llm_configured()
            resolve_provider("openai")
        records = [r for r in caplog.records if "OPENAI_API_KEY" in r.getMessage()]
        assert len(records) == 1, [r.getMessage() for r in caplog.records]
        assert value not in caplog.text

    def test_each_variable_is_reported_once_under_its_own_name(self, monkeypatch, caplog):
        values = {
            "OPENAI_API_KEY": "sk-proj-your-openai-key-here",
            "GROQ_API_KEY": "gsk_...",
            "AZURE_OPENAI_API_KEY": "<azure-key>",
        }
        for name, value in values.items():
            monkeypatch.setenv(name, value)
        with caplog.at_level(logging.WARNING):
            for _ in range(2):
                monkeypatch.delenv("LLM_PROVIDER", raising=False)
                assert llm_configured() is False  # reads OPENAI_API_KEY
                for provider in ("groq", "azure"):
                    monkeypatch.setenv("LLM_PROVIDER", provider)
                    assert llm_configured() is False
        # AZURE_OPENAI_API_KEY contains OPENAI_API_KEY, so match the leading name.
        by_name = {name: [] for name in values}
        for record in caplog.records:
            first = record.getMessage().split()[0]
            if first in by_name:
                by_name[first].append(record)
        assert {name: len(found) for name, found in by_name.items()} == {name: 1 for name in values}, (
            [r.getMessage() for r in caplog.records]
        )
        assert all(r.levelno == logging.WARNING for found in by_name.values() for r in found)
        for value in values.values():
            assert value not in caplog.text

    def test_the_warning_says_what_to_do(self, monkeypatch, caplog):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-...")
        with caplog.at_level(logging.WARNING):
            assert llm_configured() is False
        messages = [r.getMessage() for r in caplog.records if r.getMessage().startswith("OPENAI_API_KEY ")]
        assert len(messages) == 1, [r.getMessage() for r in caplog.records]
        assert "example value" in messages[0]
        assert "Put the real value in .env" in messages[0]
        assert "leave it empty to run without an LLM" in messages[0]

    def test_no_warning_for_a_real_key(self, monkeypatch, caplog):
        monkeypatch.setenv("OPENAI_API_KEY", REAL_OPENAI_PROJECT_KEY)
        with caplog.at_level(logging.WARNING):
            assert llm_configured() is True
        assert "OPENAI_API_KEY" not in caplog.text

    def test_health_and_a_gated_route_say_set_up_an_llm(self, monkeypatch):
        from src.gateway import main as gateway_main

        calls = []

        class _Completions:
            async def create(self, **_kwargs):
                calls.append(1)
                raise AssertionError("the model was called with a placeholder key")

        recorder = type("_Client", (), {})()
        recorder.chat = type("_Chat", (), {"completions": _Completions()})()
        recorder.completions = _Completions()
        monkeypatch.setattr(gateway_main, "default_client", recorder)
        monkeypatch.setattr(gateway_main, "sql_client", recorder)

        monkeypatch.setenv("OPENAI_API_KEY", "sk-proj-your-openai-key-here")
        client = TestClient(gateway_main.app)
        assert client.get("/health").json()["llm_configured"] is False

        response = client.post(
            "/v1/diagnose/pipeline",
            json={"pipeline_id": "p1", "evidence": {"status": "failed"}},
        )
        assert response.status_code == 503, response.text
        body = response.json()
        assert body["error"] == "llm_not_configured"
        assert body["message"].startswith("Set up an LLM first")
        assert calls == []
