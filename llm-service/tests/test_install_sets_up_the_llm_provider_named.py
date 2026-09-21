"""
install.sh sets up the LLM provider the operator named, with the settings it needs.

Three ways it did not:

* ``LLM_PROVIDER=groq`` and ``LLM_PROVIDER=azure`` went through a menu that
  offers OpenAI, Ollama and none. The provider was replaced with ``openai`` and
  the install asked for an OpenAI key, or, with no terminal and no
  OPENAI_API_KEY, it installed with no LLM at all. GROQ_API_KEY and the
  AZURE_OPENAI_* settings were never written to the .env either.
* ``OPENAI_API_KEY_SOURCE=gcp-metadata`` (Vertex AI with the VM's service account
  token, renewed by llm-service) was not known, so an install without a key
  either stopped or ran with no LLM.
* With OPENAI_BASE_URL naming another endpoint, LLM_MODEL defaulted to gpt-4o,
  a name Vertex AI and OpenRouter do not serve, so every LLM call failed.

Each test runs prompt_env and write_env for real, in a clean environment, and
reads the .env they write.
"""

import os
import pathlib
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
INSTALL_SH = REPO / "install.sh"

VERTEX_URL = "https://us-central1-aiplatform.googleapis.com/v1/projects/p/locations/us-central1/endpoints/openapi"
REPORTED = (
    "LLM_PROVIDER",
    "LLM_MODEL",
    "OPENAI_API_KEY_SOURCE",
    "GROQ_API_KEY",
    "AZURE_OPENAI_ENDPOINT",
    "AZURE_OPENAI_API_KEY",
    "AZURE_OPENAI_DEPLOYMENT",
)


def _install(tmp_path, env=None, answers=None):
    """Run prompt_env then write_env. Returns (exit code, output, variables, .env text)."""
    lib = tmp_path / "install-lib.sh"
    lib.write_text("\n".join(INSTALL_SH.read_text().splitlines()[:-1]) + "\n")
    install_dir = tmp_path / "rsync-ai"
    install_dir.mkdir()
    if answers is None:
        tty = "TTY_OK=0"
    else:
        answers_file = tmp_path / "answers.txt"
        answers_file.write_text("\n".join(answers) + "\n")
        tty = f'exec 3< "{answers_file}"\nTTY_OK=1'
    reported = tmp_path / "vars.txt"
    driver = tmp_path / "drive.sh"
    driver.write_text(
        "set -euo pipefail\n"
        f'source "{lib}"\n'
        f'INSTALL_DIR="{install_dir}"\n'
        f"{tty}\n"
        "report() {\n"
        "  local rc=$?\n"
        f'  {{ printf "RC=%s\\n" "$rc"; for v in {" ".join(REPORTED)}; do printf "%s=%s\\n" "$v" "${{!v:-}}"; done; }} >"{reported}"\n'
        "}\n"
        "trap report EXIT\n"
        "prompt_env\n"
        "write_env\n"
    )
    run_env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tmp_path),
        "LC_ALL": "C",
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
        "PUBLIC_HOST": "localhost",
    }
    run_env.update(env or {})
    proc = subprocess.run(["bash", str(driver)], capture_output=True, text=True, env=run_env, timeout=60)
    output = proc.stdout + proc.stderr
    assert reported.exists(), f"the driver never reached its exit trap:\n{output[-2000:]}"
    got = dict(line.partition("=")[::2] for line in reported.read_text().splitlines())
    envf = install_dir / ".env"
    return int(got.pop("RC")), output, got, envf.read_text() if envf.exists() else ""


def _env_lines(text):
    return {k: v for k, _, v in (line.partition("=") for line in text.splitlines() if "=" in line and not line.startswith("#"))}


# ---------------------------------------------------------------------------
# groq
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spelling", ["groq", "GROQ"])
def test_an_unattended_groq_install_is_a_groq_install(tmp_path, spelling):
    rc, out, got, env_text = _install(tmp_path, env={"LLM_PROVIDER": spelling, "GROQ_API_KEY": "gsk_FAKEPLACEHOLDER"})
    assert rc == 0, out[-2000:]
    written = _env_lines(env_text)
    assert written["LLM_PROVIDER"] == "groq"
    assert written["GROQ_API_KEY"] == "gsk_FAKEPLACEHOLDER"
    # Empty, so llm-service uses Groq's own default rather than asking Groq for gpt-4o.
    assert written["LLM_MODEL"] == ""
    assert "installed without an LLM" not in out
    assert "gsk_FAKEPLACEHOLDER" not in out


def test_an_unattended_groq_install_without_its_key_stops_and_says_which(tmp_path):
    rc, out, got, env_text = _install(tmp_path, env={"LLM_PROVIDER": "groq", "OPENAI_API_KEY": "sk-FAKEPLACEHOLDER"})
    assert rc != 0
    assert "GROQ_API_KEY" in out
    assert "LLM_PROVIDER=none" in out
    # Not quietly turned into an OpenAI install because an OpenAI key was there.
    assert got["LLM_PROVIDER"] == "groq"
    assert env_text == ""


def test_a_terminal_groq_install_asks_for_the_groq_key_not_an_openai_one(tmp_path):
    rc, out, got, env_text = _install(
        tmp_path, env={"LLM_PROVIDER": "groq"}, answers=["gsk_FAKEPLACEHOLDER", "localhost"]
    )
    assert rc == 0, out[-2000:]
    assert "Groq API key" in out
    assert "3) None" not in out and "OpenAI API Key" not in out
    assert _env_lines(env_text)["GROQ_API_KEY"] == "gsk_FAKEPLACEHOLDER"


def test_a_terminal_that_never_gives_the_key_stops_after_three_tries(tmp_path):
    rc, out, got, env_text = _install(tmp_path, env={"LLM_PROVIDER": "groq"}, answers=[])
    assert rc != 0
    assert out.count("GROQ_API_KEY is required") == 3


# ---------------------------------------------------------------------------
# azure
# ---------------------------------------------------------------------------

AZURE = {
    "LLM_PROVIDER": "azure",
    "AZURE_OPENAI_ENDPOINT": "https://example-resource.openai.azure.com",
    "AZURE_OPENAI_API_KEY": "azure-FAKEPLACEHOLDER",
    "AZURE_OPENAI_DEPLOYMENT": "prod-chat",
}


def test_an_unattended_azure_install_is_an_azure_install(tmp_path):
    rc, out, got, env_text = _install(tmp_path, env={**AZURE, "AZURE_OPENAI_API_VERSION": "2024-10-21"})
    assert rc == 0, out[-2000:]
    written = _env_lines(env_text)
    assert written["LLM_PROVIDER"] == "azure"
    assert written["LLM_MODEL"] == ""
    for key in ("AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_DEPLOYMENT"):
        assert written[key] == AZURE[key]
    assert written["AZURE_OPENAI_API_VERSION"] == "2024-10-21"
    assert "azure-FAKEPLACEHOLDER" not in out


def test_the_azure_key_may_come_from_openai_api_key(tmp_path):
    # llm-service reads the Azure key from OPENAI_API_KEY when AZURE_OPENAI_API_KEY is unset.
    env = {k: v for k, v in AZURE.items() if k != "AZURE_OPENAI_API_KEY"}
    rc, out, got, env_text = _install(tmp_path, env={**env, "OPENAI_API_KEY": "azure-FAKEPLACEHOLDER"})
    assert rc == 0, out[-2000:]
    assert _env_lines(env_text)["LLM_PROVIDER"] == "azure"


@pytest.mark.parametrize("missing", ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_DEPLOYMENT"])
def test_an_unattended_azure_install_missing_a_setting_stops_and_names_it(tmp_path, missing):
    env = {k: v for k, v in AZURE.items() if k != missing}
    rc, out, got, env_text = _install(tmp_path, env=env)
    assert rc != 0
    assert f"needs {missing}" in out
    assert env_text == ""


def test_llm_model_names_the_azure_deployment(tmp_path):
    env = {k: v for k, v in AZURE.items() if k != "AZURE_OPENAI_DEPLOYMENT"}
    rc, out, got, env_text = _install(tmp_path, env={**env, "LLM_MODEL": "prod-chat"})
    assert rc == 0, out[-2000:]
    assert _env_lines(env_text)["LLM_MODEL"] == "prod-chat"


# ---------------------------------------------------------------------------
# Vertex AI: OPENAI_API_KEY_SOURCE=gcp-metadata
# ---------------------------------------------------------------------------

VERTEX = {
    "OPENAI_API_KEY_SOURCE": "gcp-metadata",
    "OPENAI_BASE_URL": VERTEX_URL,
    "LLM_MODEL": "google/gemini-2.5-flash",
}


@pytest.mark.parametrize("provider", [None, "openai"])
def test_an_unattended_vertex_install_needs_no_key(tmp_path, provider):
    env = dict(VERTEX)
    if provider:
        env["LLM_PROVIDER"] = provider
    rc, out, got, env_text = _install(tmp_path, env=env)
    assert rc == 0, out[-2000:]
    written = _env_lines(env_text)
    assert written["LLM_PROVIDER"] == "openai"
    assert written["OPENAI_API_KEY_SOURCE"] == "gcp-metadata"
    assert written["OPENAI_BASE_URL"] == VERTEX_URL
    assert written["LLM_MODEL"] == "google/gemini-2.5-flash"
    assert written["OPENAI_API_KEY"] == ""
    assert "installed without an LLM" not in out


@pytest.mark.parametrize(
    "url",
    [
        "",
        "http://us-central1-aiplatform.googleapis.com/v1",
        "https://api.openai.com/v1",
        "https://evil.example/#.googleapis.com",
        "https://evil.example?.googleapis.com",
        "https://googleapis.com.evil.example/v1",
    ],
)
def test_the_service_account_token_is_only_set_up_for_googleapis(tmp_path, url):
    rc, out, got, env_text = _install(tmp_path, env={**VERTEX, "OPENAI_BASE_URL": url})
    assert rc != 0
    assert "googleapis.com" in out
    assert env_text == ""


URLS = [
    VERTEX_URL,
    "HTTPS://US-CENTRAL1-AIPLATFORM.GOOGLEAPIS.COM/v1",
    "https://googleapis.com/v1",
    "https://user@europe-west4-aiplatform.googleapis.com:443/v1",
    "https://aiplatform.googleapis.com?x=1",
    "https://svc@googleapis.com/v1",
    "https://aiplatform.googleapis.com:443/v1",
    "",
    "http://aiplatform.googleapis.com/v1",
    "https://api.openai.com/v1",
    "https://evil.example/#.googleapis.com",
    "https://evil.example?.googleapis.com",
    "https://evil.example/.googleapis.com",
    "https://googleapis.com.evil.example/v1",
    "https://x.googleapis.com@evil.example/v1",
    "https://notgoogleapis.com/v1",
]


def test_the_installer_and_llm_service_agree_on_which_hosts_get_the_token(tmp_path, monkeypatch):
    from src.utils import openai_client

    lib = tmp_path / "install-lib.sh"
    lib.write_text("\n".join(INSTALL_SH.read_text().splitlines()[:-1]) + "\n")
    script = f'source "{lib}"\nfor u in "$@"; do if base_url_is_googleapis "$u"; then echo yes; else echo no; fi; done\n'
    proc = subprocess.run(
        ["bash", "-c", script, "drive", *URLS], capture_output=True, text=True, timeout=30,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LC_ALL": "C"},
    )
    shell = proc.stdout.split()
    assert len(shell) == len(URLS), proc.stderr[-2000:]
    python = []
    for url in URLS:
        monkeypatch.setenv("OPENAI_BASE_URL", url)
        python.append("yes" if openai_client._base_url_is_google() else "no")
    assert shell == python, list(zip(URLS, shell, python))
    # Not vacuous: both answers occur.
    assert {"yes", "no"} <= set(python)


# ---------------------------------------------------------------------------
# another OpenAI-compatible endpoint names its own models
# ---------------------------------------------------------------------------


def test_an_unattended_install_on_another_endpoint_needs_its_model_name(tmp_path):
    rc, out, got, env_text = _install(
        tmp_path, env={"OPENAI_BASE_URL": "https://openrouter.ai/api/v1", "OPENAI_API_KEY": "sk-or-FAKEPLACEHOLDER"}
    )
    assert rc != 0
    assert "needs LLM_MODEL" in out
    assert env_text == ""


def test_a_terminal_install_on_another_endpoint_asks_for_the_model(tmp_path):
    rc, out, got, env_text = _install(
        tmp_path,
        env={"OPENAI_BASE_URL": "https://openrouter.ai/api/v1"},
        answers=["1", "sk-or-FAKEPLACEHOLDER", "openai/gpt-4o", "localhost"],
    )
    assert rc == 0, out[-2000:]
    assert _env_lines(env_text)["LLM_MODEL"] == "openai/gpt-4o"


def test_openai_itself_still_defaults_to_gpt_4o(tmp_path):
    rc, out, got, env_text = _install(tmp_path, env={"OPENAI_API_KEY": "sk-FAKEPLACEHOLDER"})
    assert rc == 0, out[-2000:]
    written = _env_lines(env_text)
    assert written["LLM_PROVIDER"] == "openai"
    assert written["LLM_MODEL"] == "gpt-4o"
    assert written["OPENAI_API_KEY_SOURCE"] == ""


# ---------------------------------------------------------------------------
# names this installer does not know, and installs with no hosted provider
# ---------------------------------------------------------------------------


def test_an_unknown_provider_is_reported_without_its_value(tmp_path):
    rc, out, got, env_text = _install(
        tmp_path, env={"LLM_PROVIDER": "zz-unknown-provider", "OPENAI_API_KEY": "sk-FAKEPLACEHOLDER"}
    )
    assert rc == 0, out[-2000:]
    assert "does not know" in out and "OPENAI_BASE_URL" in out
    assert "zz-unknown-provider" not in out
    assert _env_lines(env_text)["LLM_PROVIDER"] == "openai"


def test_an_unknown_key_source_is_reported_without_its_value(tmp_path):
    rc, out, got, env_text = _install(
        tmp_path, env={"OPENAI_API_KEY_SOURCE": "sk-PASTEDINTOTHEWRONGPLACE", "OPENAI_API_KEY": "sk-FAKEPLACEHOLDER"}
    )
    assert rc == 0, out[-2000:]
    assert "OPENAI_API_KEY_SOURCE" in out
    assert "sk-PASTEDINTOTHEWRONGPLACE" not in out and "sk-PASTEDINTOTHEWRONGPLACE" not in env_text
    assert _env_lines(env_text)["OPENAI_API_KEY_SOURCE"] == ""


@pytest.mark.parametrize("provider", ["none", "ollama"])
def test_no_hosted_provider_settings_reach_an_install_that_uses_none(tmp_path, provider):
    leftovers = {
        "GROQ_API_KEY": "gsk_FAKEPLACEHOLDER",
        "AZURE_OPENAI_ENDPOINT": "https://example-resource.openai.azure.com",
        "AZURE_OPENAI_API_KEY": "azure-FAKEPLACEHOLDER",
        "OPENAI_API_KEY_SOURCE": "gcp-metadata",
    }
    rc, out, got, env_text = _install(tmp_path, env={"LLM_PROVIDER": provider, **leftovers})
    assert rc == 0, out[-2000:]
    written = _env_lines(env_text)
    assert written["LLM_PROVIDER"] == provider
    for key in leftovers:
        assert written[key] == "", key
