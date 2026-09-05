"""Regression guard for the logo-SVG XXE / entity-expansion fix (KI-LOGO-SVG-XXE).

`_is_valid_svg` receives attacker-controlled bytes: an uploaded OpenAPI spec's
`info.x-logo.url` is fetched by `_download_vendor_logo` and, once it passes the
SSRF + `image/svg+xml` content-type gate, handed to `_is_valid_svg` which used to
call `xml.etree.ElementTree.fromstring` with no DOCTYPE/ENTITY guard — a
billion-laughs / quadratic-blowup DoS vector (stdlib ElementTree does not resolve
external entities, so no file exfil, but internal entity expansion is unbounded;
the 5 MiB size cap does not stop it). The fix mirrors csdl_converter's
`_DANGEROUS_XML_RE`: reject any SVG declaring a DOCTYPE or ENTITY *before* parsing,
outright — never falling through to the lenient `len(content) > 50` size fallback.

Pure, offline assertions — no network.
"""

from src.utils.logo_downloader import _is_valid_svg

_PLAIN_SVG = (
    b'<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10">'
    b'<rect width="10" height="10"/></svg>'
)
_XXE_ENTITY = (
    b'<?xml version="1.0"?>\n'
    b'<!DOCTYPE svg [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>\n'
    b'<svg>&xxe;</svg>'
)
_BILLION_LAUGHS = (
    b'<!DOCTYPE lolz [<!ENTITY lol "lol">'
    b'<!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;">]>'
    b'<svg>&lol2;</svg>'
)
# A DOCTYPE pushed past a large leading comment — the guard must scan the whole
# document, not just a leading window, or this slips through.
_COMMENT_PADDED_DOCTYPE = (
    b'<svg>' + b'<!-- ' + b'x' * 4000 + b' -->'
    b'<!DOCTYPE svg [<!ENTITY x "y">]></svg>'
)


def test_plain_svg_accepted():
    assert _is_valid_svg(_PLAIN_SVG) is True


def test_doctype_entity_rejected():
    assert _is_valid_svg(_XXE_ENTITY) is False


def test_billion_laughs_rejected():
    assert _is_valid_svg(_BILLION_LAUGHS) is False


def test_comment_padded_doctype_rejected():
    # Must NOT fall through to the lenient size fallback: this payload is > 50 bytes.
    assert len(_COMMENT_PADDED_DOCTYPE) > 50
    assert _is_valid_svg(_COMMENT_PADDED_DOCTYPE) is False


def test_empty_rejected():
    assert _is_valid_svg(b"") is False
