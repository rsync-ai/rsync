"""A Kafka SASL password containing a backslash must survive the trip to the broker.

`sasl.jaas.config` is not a string field. It is a miniature grammar that the JVM
parses with a StreamTokenizer, and inside a quoted value a backslash is an escape
introducer that gets *consumed*. An unescaped one is therefore eaten rather than
rejected -- so the failure mode is a silently wrong password, and the broker's
reply is an ordinary authentication failure. Nothing anywhere says "your password
was mangled in transit".

Worse, the value often crosses TWO grammars. A Java .properties file eats a level
of backslash of its own, so a JAAS line written into one and loaded back needs to
have been escaped twice. Escaping it once there produces corruption byte-identical
to not escaping it at all, which is how a site can look guarded and not be.

    site                                        grammars crossed
    ------------------------------------------  ------------------------
    kafka_security._jaas_config -> config JSON  JAAS
    debezium connector, externalized secrets    JAAS + .properties
    chart kafka-init --command-config           JAAS + .properties
    chart Connect CONNECT_* env                 JAAS + .properties
                                                (docker-entrypoint.sh writes
                                                 them to connect-distributed
                                                 .properties, then loads it)

The decoders below model those two grammars in Python so this can be a fast unit
test. They are not taken on faith: `test_the_decoders_reproduce_what_the_jvm_did`
pins them against values measured from Kafka's own parser. If a decoder drifts,
that control fails first and says so, rather than letting the encoder assertions
pass against a fiction.

Provenance of the pinned values -- reproduce with
deploy/helm/rsync-ai/test/kind/jaas-probe/run.sh, which runs Kafka's real
JaasContext (kafka-clients 3.7.0) and Utils.loadProps under a JDK 21 container.
"""

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.kafka_security import _jaas_config  # noqa: E402

# --- the two grammars, as the JVM implements them ---------------------------------

# java.io.StreamTokenizer, which is what org.apache.kafka.common.security.JaasConfig
# tokenizes with. Inside a quoted string it recognises these escapes and, crucially,
# drops the backslash of any escape it does NOT recognise.
_ST_ESCAPES = {
    "\\": "\\", '"': '"', "'": "'",
    "n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f",
}


def _jaas_unescape(quoted: str) -> str:
    out, i = [], 0
    while i < len(quoted):
        c = quoted[i]
        if c != "\\" or i + 1 >= len(quoted):
            out.append(c)
            i += 1
            continue
        nxt = quoted[i + 1]
        if nxt in "01234567":  # octal escape, up to 3 digits
            m = re.match(r"[0-7]{1,3}", quoted[i + 1:])
            out.append(chr(int(m.group(0), 8)))
            i += 1 + len(m.group(0))
            continue
        out.append(_ST_ESCAPES.get(nxt, nxt))  # unknown escape: backslash dropped
        i += 2
    return out


def _jaas_parse_password(line: str) -> str:
    """Extract and unescape the password from a JAAS line, the way the JVM would.

    Raises ValueError where the JVM raises IllegalArgumentException -- an unbalanced
    quote makes the rest of the line unparseable, which the JVM reports as a missing
    value or an unterminated entry, i.e. as a syntax problem rather than a password
    problem.
    """
    m = re.search(r'password="((?:[^"\\]|\\.)*)"', line)
    if not m:
        raise ValueError("no parseable password= entry (unbalanced quoting)")
    # Everything after the value must be further key="value" options and then the
    # terminating semicolon. Merely ending in ';' is too lenient: when a stray quote
    # closes the value early the remainder is a bare word, and the JVM reports that
    # as `Value not specified for key '...'` rather than as a quoting problem.
    tail = line[m.end():]
    if not re.fullmatch(r'(?:\s+[\w.$-]+="(?:[^"\\]|\\.)*")*\s*;\s*', tail):
        raise ValueError(f"unparseable JAAS remainder {tail!r}")
    return "".join(_jaas_unescape(m.group(1)))


def _properties_unescape(raw: str) -> str:
    """java.util.Properties.load: also eats one level of backslash."""
    out, i = [], 0
    simple = {"n": "\n", "t": "\t", "r": "\r", "f": "\f"}
    while i < len(raw):
        c = raw[i]
        if c != "\\" or i + 1 >= len(raw):
            out.append(c)
            i += 1
            continue
        nxt = raw[i + 1]
        if nxt == "u":
            out.append(chr(int(raw[i + 2:i + 6], 16)))
            i += 6
            continue
        out.append(simple.get(nxt, nxt))
        i += 2
    return "".join(out)


# Passwords that actually exercise the grammar. A password of only alphanumerics
# passes under every encoding, correct or not, so a suite built from those would
# have shipped this bug too.
NASTY = [
    "plain-ok",
    "pa\\ss",
    'pa"ss',
    'pa"ss\\x',
    "C:\\Users\\svc",
    "a\\nb",
    "tail\\",
    "sp ace=and&sym",
]

# What each encoding produced when fed to Kafka's own JaasContext. `RAISES` means
# the JVM threw IllegalArgumentException. These are measurements, not predictions.
JVM_OBSERVED_QUOTE_ONLY = {
    "pa\\ss": "pass",
    'pa"ss': "OK",
    'pa"ss\\x': 'pa"ssx',
    "C:\\Users\\svc": "C:Userssvc",
    "a\\nb": "a\nb",
    "tail\\": "RAISES",
}
# jaas-escaped once, then read back out of a .properties file.
JVM_OBSERVED_SINGLE_ESCAPE_VIA_PROPERTIES = {
    "pa\\ss": "pass",
    'pa"ss': "RAISES",
    "C:\\Users\\svc": "C:Userssvc",
    "a\\nb": "a\nb",
    "tail\\": "RAISES",
}


def _quote_only(v: str) -> str:
    """The encoding this module shipped before -- kept as the negative control."""
    return v.replace('"', '\\"')


def _properties_escape(v: str) -> str:
    return v.replace("\\", "\\\\")


def _roundtrip(line: str) -> str:
    return _jaas_parse_password(line)


def _roundtrip_via_properties(line: str) -> str:
    return _jaas_parse_password(_properties_unescape(line))


# --- control: is the model of the JVM actually right? -----------------------------


@pytest.mark.parametrize("pw", sorted(JVM_OBSERVED_QUOTE_ONLY))
def test_the_decoders_reproduce_what_the_jvm_did(pw):
    """If this fails, the decoders are fiction and every assertion below is vacuous."""
    expected = JVM_OBSERVED_QUOTE_ONLY[pw]
    line = f'M required username="u" password="{_quote_only(pw)}";'
    if expected == "RAISES":
        with pytest.raises(ValueError):
            _roundtrip(line)
    elif expected == "OK":
        assert _roundtrip(line) == pw
    else:
        assert _roundtrip(line) == expected


@pytest.mark.parametrize("pw", sorted(JVM_OBSERVED_SINGLE_ESCAPE_VIA_PROPERTIES))
def test_the_properties_decoder_reproduces_what_the_jvm_did(pw):
    expected = JVM_OBSERVED_SINGLE_ESCAPE_VIA_PROPERTIES[pw]
    line = _jaas_config("PLAIN", "u", pw)  # correctly JAAS-escaped, NOT props-escaped
    if expected == "RAISES":
        with pytest.raises(ValueError):
            _roundtrip_via_properties(line)
    else:
        assert _roundtrip_via_properties(line) == expected


# --- the actual guard -------------------------------------------------------------


@pytest.mark.parametrize("pw", NASTY)
def test_the_password_survives_the_jaas_grammar(pw):
    assert _roundtrip(_jaas_config("PLAIN", "u", pw)) == pw, (
        f"{pw!r} does not survive sasl.jaas.config -- the broker will be handed a "
        "different password and reject it as a bad credential"
    )


@pytest.mark.parametrize("pw", NASTY)
def test_the_username_survives_too(pw):
    line = _jaas_config("SCRAM-SHA-512", pw, "irrelevant")
    m = re.search(r'username="((?:[^"\\]|\\.)*)"', line)
    assert m, f"username={pw!r} broke the quoting"
    assert "".join(_jaas_unescape(m.group(1))) == pw


@pytest.mark.parametrize("pw", NASTY)
def test_control_the_old_quote_only_encoding_really_was_broken(pw):
    """At least one realistic password must be provably corrupted by the old code.

    Asserted per-value so this cannot pass by averaging: for the backslash cases the
    old encoding must fail, and for the rest it must agree -- which is exactly why
    the bug survived review.
    """
    line = f'M required username="u" password="{_quote_only(pw)}";'
    try:
        got = _roundtrip(line)
    except ValueError:
        assert "\\" in pw, f"{pw!r} has no backslash but the old encoding could not parse"
        return
    if "\\" in pw:
        assert got != pw, (
            f"{pw!r} round-trips under quote-only escaping too; it does not "
            "discriminate, so remove it from NASTY or this control is noise"
        )
    else:
        assert got == pw


@pytest.mark.parametrize("pw", NASTY)
def test_a_value_crossing_a_properties_file_needs_the_second_pass(pw):
    """The two-grammar case: escape once and the corruption is identical to none."""
    once = _jaas_config("PLAIN", "u", pw)
    twice = _properties_escape(once)
    assert _roundtrip_via_properties(twice) == pw, (
        f"{pw!r} does not survive .properties + JAAS with both escaping passes"
    )
    if "\\" in pw:
        try:
            got = _roundtrip_via_properties(once)
        except ValueError:
            return
        assert got != pw, (
            f"{pw!r} survives a single escaping pass through a .properties file, so "
            "the second pass is not load-bearing for it -- check this test, not the code"
        )
