"""rsync's log4j logger levels reach the Kafka Connect worker even on a host whose
/kafka/config volume outlived the image that seeded it.

/kafka/config is a volume, and Docker seeds a volume from the image only when it
creates it; the base entrypoint's `cp -n config.orig/* config` skips a file that
exists. So the PgOutputMessageDecoder=ERROR level #1198 baked into the image never
reached prod's kept volume. sync-log4j-loggers.sh now appends every logger in
log4j-loggers.properties that the live file lacks, at every start. These tests run
it against a temp KAFKA_HOME.
"""

import pathlib
import re
import shutil
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
CONNECT = REPO / "shared" / "internal" / "infra" / "kafka-connect"
SYNC = CONNECT / "sync-log4j-loggers.sh"
LOGGERS = CONNECT / "log4j-loggers.properties"

DECODER = "log4j.logger.io.debezium.connector.postgresql.connection.pgoutput.PgOutputMessageDecoder"

# The debezium/connect 3.1.3 worker file a kept volume holds (abridged).
OLD_LIVE = """kafka.logs.dir=logs

log4j.rootLogger=INFO, stdout, appender

# Disable excessive reflection warnings - KAFKA-5229
log4j.logger.org.reflections=ERROR

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
"""

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")


def _home(tmp_path, live):
    (tmp_path / "config").mkdir()
    if live is not None:
        (tmp_path / "config" / "log4j.properties").write_text(live)
    return tmp_path


def _run(home, script=SYNC):
    return subprocess.run(
        ["bash", str(script)], env={"KAFKA_HOME": str(home), "PATH": "/usr/bin:/bin"},
        capture_output=True, text=True, timeout=30,
    )


def _live(home):
    return (home / "config" / "log4j.properties").read_text()


def _with_loggers(tmp_path, text):
    """A copy of the script next to a log4j-loggers.properties of our own."""
    d = tmp_path / "opt"
    d.mkdir()
    shutil.copy(SYNC, d / SYNC.name)
    (d / LOGGERS.name).write_text(text)
    return d / SYNC.name


def test_rsync_sets_the_decoder_level():
    lines = [l.strip() for l in LOGGERS.read_text().splitlines()]
    assert f"{DECODER}=ERROR" in lines


def test_a_kept_volume_gets_the_level_it_lacks(tmp_path):
    home = _home(tmp_path, OLD_LIVE)
    r = _run(home)
    assert r.returncode == 0, r.stderr
    live = _live(home)
    assert live.startswith(OLD_LIVE), "the sync rewrote the live file instead of appending"
    assert f"\n{DECODER}=ERROR\n" in live
    assert DECODER in r.stderr, "the sync set a level without saying so"


def test_the_sync_is_idempotent(tmp_path):
    home = _home(tmp_path, OLD_LIVE)
    _run(home)
    once = _live(home)
    r = _run(home)
    assert r.returncode == 0 and _live(home) == once
    assert r.stderr == "", "a second start reported setting a level it already set"


def test_a_level_the_live_file_sets_wins(tmp_path):
    # An operator's level, indented and with the other separator, is still that logger.
    live = OLD_LIVE + f"  {DECODER} : WARN\n"
    home = _home(tmp_path, live)
    assert _run(home).returncode == 0
    assert _live(home) == live


def test_a_commented_out_logger_does_not_count_as_set(tmp_path):
    home = _home(tmp_path, OLD_LIVE + f"#{DECODER}=WARN\n")
    _run(home)
    assert _live(home).endswith(f"#{DECODER}=WARN\n{DECODER}=ERROR\n")


def test_a_live_file_without_a_final_newline_is_not_joined(tmp_path):
    home = _home(tmp_path, OLD_LIVE.rstrip("\n"))
    _run(home)
    lines = _live(home).splitlines()
    assert "log4j.appender.stdout=org.apache.log4j.ConsoleAppender" in lines
    assert f"{DECODER}=ERROR" in lines


def test_only_logger_lines_move(tmp_path):
    script = _with_loggers(tmp_path, (
        "# a comment=with a separator\n"
        "log4j.rootLogger=DEBUG, stdout\n"
        "log4j.appender.extra=org.apache.log4j.ConsoleAppender\n"
        "log4j.logger.a.B=WARN\n"
        "log4j.logger.c.D=ERROR"  # no final newline: still read
    ))
    (tmp_path / "h").mkdir()
    home = _home(tmp_path / "h", OLD_LIVE)
    r = _run(home, script)
    assert r.returncode == 0, r.stderr
    assert _live(home) == OLD_LIVE + "log4j.logger.a.B=WARN\nlog4j.logger.c.D=ERROR\n"


def test_a_missing_live_file_is_left_to_the_base_entrypoint(tmp_path):
    # Creating it would stop the base entrypoint's seed: a worker with no root logger.
    home = _home(tmp_path, None)
    r = _run(home)
    assert r.returncode == 0
    assert not (home / "config" / "log4j.properties").exists()


def test_a_read_only_live_file_never_fails_the_start(tmp_path):
    home = _home(tmp_path, OLD_LIVE)
    f = home / "config" / "log4j.properties"
    f.chmod(0o444)
    try:
        r = _run(home)
    finally:
        f.chmod(0o644)
    if _live(home) != OLD_LIVE:
        pytest.skip("running as root: a 0444 file is still writable")
    assert r.returncode == 0
    assert "not writable" in r.stderr


def test_the_image_ships_both_files_and_runs_the_sync_before_the_base_entrypoint():
    dockerfile = (CONNECT / "Dockerfile").read_text()
    shipped = set()
    for m in re.finditer(r"^COPY --chmod=\d+ (.+) /opt/rsync/$", dockerfile, re.M):
        shipped.update(m.group(1).split())
    assert {SYNC.name, LOGGERS.name} <= shipped, (
        "the script finds log4j-loggers.properties next to itself: both must land in /opt/rsync/"
    )
    entry = (CONNECT / "connect-entrypoint.sh").read_text()
    call = entry.find(f"/opt/rsync/{SYNC.name} ||")
    handover = entry.find('exec /docker-entrypoint.sh "$@"')
    assert call != -1, "connect-entrypoint.sh no longer runs the sync, or lets it fail the start"
    assert handover != -1 and call < handover, "the sync must run before the base entrypoint reads the file"


def test_no_logger_level_is_baked_into_the_config_volume():
    """A level written into /kafka/config at build time reaches only a fresh volume --
    the defect this file exists for. rsync.ai's levels live in log4j-loggers.properties."""
    dockerfile = (CONNECT / "Dockerfile").read_text()
    assert not re.search(r"log4j\.logger\.\S+=\S+", dockerfile), (
        "a logger level is set in the Dockerfile; put it in log4j-loggers.properties"
    )
