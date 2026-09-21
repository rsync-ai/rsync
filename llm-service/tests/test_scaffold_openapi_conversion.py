"""The deterministic scaffolder's OpenAPI -> ConnectorSpec conversion.

`scaffold/openapi_to_spec.py` is the half of the scaffolder that has no
dependencies at all: stdlib in, a plain dict out. Everything it decides is a
RULE applied to the document, so every rule is checkable here without rendering
anything, without pydantic, and without a model.

WHY THIS FILE ASSERTS ON FIELD NAMES AND NOT JUST "it produced a spec". Three of
the rules exist to satisfy a validator or a template branch that lives somewhere
else, and each of them fails SILENTLY when it regresses:

  * `header_prefix: ""` on api_key auth. `AuthConfig.header_prefix` defaults to
    the literal "Bearer" for every auth type, so dropping the explicit empty
    string does not raise -- it emits a spec whose own JSON claims the connector
    sends `X-Api-Key: Bearer <key>`.
  * both `username_env` and `password_env` on basic auth. `AuthConfig`'s
    validator rejects basic auth without them, so this one is loud today, and
    that is exactly why it must stay asserted: the raise happens in a file this
    package does not own.
  * the oauth2 -> bearer downgrade. `AuthConfig` requires an `oauth_provider`
    that resolves against a curated registry no document can supply. A future
    "improvement" that passes oauth2 straight through would fail at
    ConnectorSpec construction, which is a render-time error and not a
    conversion-time one.

THIS SUITE IS COLLECTED IN THE PUBLIC REPO, because `scaffold/` ships there. That
verdict is not written down anywhere: the import below stays spelled
`agents.tool_generator.*` so `tests/_cut_collection.py` can see what the file
reaches into, and `test_the_scaffold_package_is_where_the_cut_detector_can_see_it`
reads the expected answer out of `oss-strip-list.txt`. Put `scaffold/` back on
that list and the suite stops being collected publicly with nothing here to edit.
"""

from __future__ import annotations

import ast
import os

import pytest

# Must run before the first `agents.*` import below: the sibling `test_gen_*.py`
# suites are collected first and leave `agents` bound to the generation
# package's own inner `agents/`, under which `agents.tool_generator` does not
# exist. `_real_agents_package` explains the whole mechanism.
from _real_agents_package import make_agents_tool_generator_importable

make_agents_tool_generator_importable()

from agents.tool_generator.scaffold.openapi_to_spec import (  # noqa: E402
    ConversionReport,
    OpenAPIConversionError,
    openapi_to_connector_spec,
    slugify,
)


# --------------------------------------------------------------------------- #
# Documents. Each is the smallest thing that exercises the rule under test.
# --------------------------------------------------------------------------- #

def _oas3(**overrides):
    """A minimal but complete OAS 3.0 document with one readable collection."""
    doc = {
        "openapi": "3.0.3",
        "info": {"title": "Widget Co API", "description": "Widgets, on demand."},
        "servers": [{"url": "https://api.widgetco.test/v2"}],
        "paths": {
            "/widgets": {
                "get": {
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {"data": {"type": "array", "items": {}}},
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
    }
    doc.update(overrides)
    return doc


def _with_schemes(schemes):
    doc = _oas3()
    doc["components"] = {"securitySchemes": schemes}
    return doc


def _convert(doc, **kwargs) -> ConversionReport:
    return openapi_to_connector_spec(doc, **kwargs)


def _auth(doc, **kwargs):
    return _convert(doc, **kwargs).spec["auth"]


def _only_resource(report: ConversionReport):
    resources = report.spec["resources"]
    assert len(resources) == 1, [r["name"] for r in resources]
    return resources[0]


# --------------------------------------------------------------------------- #
# Vacuity guard
# --------------------------------------------------------------------------- #

def test_the_base_document_converts():
    """Every other test edits this document. If it stops converting they all lie."""
    report = _convert(_oas3())
    assert report.spec["name"] == "widget_co_api"
    assert report.spec["display_name"] == "Widget Co API"
    assert report.resource_count == 1
    assert _only_resource(report)["name"] == "widgets"


# --------------------------------------------------------------------------- #
# Document shape
# --------------------------------------------------------------------------- #

def test_swagger_2_documents_are_accepted():
    doc = {
        "swagger": "2.0",
        "info": {"title": "Legacy Ledger"},
        "host": "legacy.example.com",
        "basePath": "/api/v1",
        "schemes": ["http", "https"],
        "paths": {"/accounts": {"get": {"responses": {"200": {"schema": {"type": "array"}}}}}},
    }
    spec = _convert(doc).spec
    # https is preferred whenever it is offered, whatever order schemes lists it in.
    assert spec["base_url"] == "https://legacy.example.com/api/v1"
    assert spec["resources"][0]["name"] == "accounts"


def test_a_document_that_is_neither_openapi_nor_swagger_is_refused():
    with pytest.raises(OpenAPIConversionError) as excinfo:
        _convert({"info": {"title": "Not A Spec"}, "paths": {}})
    assert "openapi" in str(excinfo.value).lower()


def test_a_non_mapping_document_is_refused():
    with pytest.raises(OpenAPIConversionError):
        _convert(["not", "a", "document"])


def test_a_document_with_no_readable_collection_is_refused():
    """A source connector with nothing to GET would render and then do nothing."""
    doc = _oas3(paths={"/webhooks": {"post": {"responses": {"201": {}}}}})
    with pytest.raises(OpenAPIConversionError) as excinfo:
        _convert(doc)
    assert "GET-able" in str(excinfo.value)


def test_a_document_with_no_title_and_no_name_override_is_refused():
    doc = _oas3(info={})
    with pytest.raises(OpenAPIConversionError) as excinfo:
        _convert(doc)
    assert "--name" in str(excinfo.value)


def test_an_explicit_name_substitutes_for_a_missing_title():
    report = _convert(_oas3(info={}), name="widgetco")
    assert report.spec["name"] == "widgetco"
    # display_name must never be left empty: the product UI renders it as the label.
    assert report.spec["display_name"] == "widgetco"


# --------------------------------------------------------------------------- #
# Base URL
# --------------------------------------------------------------------------- #

def test_server_variables_resolve_from_their_own_defaults():
    doc = _oas3(servers=[{
        "url": "https://{region}.api.widgetco.test/v2/",
        "variables": {"region": {"default": "eu", "enum": ["eu", "us"]}},
    }])
    assert _convert(doc).spec["base_url"] == "https://eu.api.widgetco.test/v2"


def test_an_explicit_base_url_overrides_the_declared_server():
    spec = _convert(_oas3(), base_url="https://self-hosted.example.com/api/").spec
    assert spec["base_url"] == "https://self-hosted.example.com/api"
    # The connection form's default follows the override, not the document.
    base_field = next(f for f in spec["config_fields"] if f["name"] == "base_url")
    assert base_field["default"] == "https://self-hosted.example.com/api"


def test_a_document_with_no_server_says_so_instead_of_guessing():
    doc = _oas3()
    doc.pop("servers")
    report = _convert(doc)
    assert report.spec["base_url"] == ""
    assert any("no server URL" in note for note in report.notes)


# --------------------------------------------------------------------------- #
# Authentication
# --------------------------------------------------------------------------- #

def test_an_api_key_header_pins_the_prefix_to_empty():
    """The regression this guards ships a spec that documents a wrong header.

    `AuthConfig.header_prefix` defaults to "Bearer" for every auth type, so
    omitting the key here is not an error -- it silently claims the connector
    sends `X-Api-Key: Bearer <key>`.
    """
    auth = _auth(_with_schemes({"key": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}}))
    assert auth == {"type": "api_key", "header_name": "X-Api-Key", "header_prefix": ""}


def test_an_api_key_query_parameter_becomes_api_key_query():
    auth = _auth(_with_schemes({"key": {"type": "apiKey", "in": "query", "name": "api_token"}}))
    assert auth["type"] == "api_key_query"
    assert auth["query_param"] == "api_token"


def test_http_bearer_becomes_bearer():
    auth = _auth(_with_schemes({"jwt": {"type": "http", "scheme": "bearer"}}))
    assert auth == {"type": "bearer", "header_name": "Authorization", "header_prefix": "Bearer"}


@pytest.mark.parametrize("scheme", [
    {"type": "http", "scheme": "basic"},   # OpenAPI 3 spelling
    {"type": "basic"},                     # Swagger 2.0 spelling
])
def test_basic_auth_always_carries_both_credential_env_vars(scheme):
    """`AuthConfig.validate_auth_config` rejects basic auth that omits either."""
    auth = _auth(_with_schemes({"login": scheme}))
    assert auth["type"] == "basic"
    assert auth["username_env"] and auth["password_env"]


def test_oauth2_is_downgraded_to_a_pre_issued_bearer_token():
    """No document can supply the curated `oauth_provider` key oauth2 requires."""
    doc = _with_schemes({"oauth": {
        "type": "oauth2",
        "flows": {"clientCredentials": {"tokenUrl": "https://api.widgetco.test/oauth/token", "scopes": {}}},
    }})
    report = _convert(doc)
    assert report.spec["auth"]["type"] == "bearer"
    assert "oauth_provider" not in report.spec["auth"]
    # The marker the sorter uses must never reach the emitted spec.
    assert "_was_oauth2" not in report.spec["auth"]
    assert any("OAuth2" in note and "access token" in note for note in report.notes)


def test_a_static_credential_wins_over_oauth_and_the_note_names_the_declared_scheme():
    """The reader goes looking in their own document, which says 'oauth2'."""
    doc = _with_schemes({
        "oauth": {"type": "oauth2", "flows": {}},
        "key": {"type": "apiKey", "in": "header", "name": "X-Api-Key"},
    })
    report = _convert(doc)
    assert report.spec["auth"]["type"] == "api_key"
    multi = [n for n in report.notes if "multiple auth methods" in n]
    assert len(multi) == 1
    assert "oauth2" in multi[0]
    assert "bearer" not in multi[0]


def test_a_scheme_with_no_connector_equivalent_is_reported_not_silently_dropped():
    report = _convert(_with_schemes({"session": {"type": "apiKey", "in": "cookie", "name": "sid"}}))
    assert report.spec["auth"]["type"] == "none"
    assert any("'session'" in note for note in report.notes)


def test_a_document_that_declares_no_security_marks_auth_undetermined():
    """`auth_undetermined` is the difference between 'public API' and 'unstated'.

    ConnectorSpec exposes the flag so the template renders an OPTIONAL X-API-Key
    header. Without it a spec-silent-but-authenticated API produces a connector
    that can never send a credential.
    """
    report = _convert(_oas3())
    assert report.spec["auth"] == {"type": "none"}
    assert report.spec["auth_undetermined"] is True
    # The private marker is consumed, never emitted.
    assert "_undetermined" not in report.spec["auth"]
    assert any("X-API-Key" in note for note in report.notes)


def test_a_document_that_declares_security_does_not_mark_it_undetermined():
    report = _convert(_with_schemes({"jwt": {"type": "http", "scheme": "bearer"}}))
    assert report.spec["auth_undetermined"] is False


# --------------------------------------------------------------------------- #
# Resources: verbs
# --------------------------------------------------------------------------- #

def _crud_doc(item_path, item_verbs, collection_verbs=()):
    paths = {"/widgets": {"get": {"responses": {"200": {}}}}}
    for verb in collection_verbs:
        paths["/widgets"][verb] = {"responses": {"200": {}}}
    paths[item_path] = {verb: {"responses": {"200": {}}} for verb in item_verbs}
    return _oas3(paths=paths)


def test_the_item_route_is_found_whatever_its_placeholder_is_called():
    """`/widgets/{widgetId}` is the common spelling; `{id}` is not guaranteed."""
    report = _convert(_crud_doc("/widgets/{widgetId}", ["get"]))
    assert _only_resource(report)["supports_get"] is True


def test_a_collection_with_no_item_route_does_not_claim_single_record_reads():
    """`supports_get` drives a per-record fetch the API would answer with a 404."""
    assert _only_resource(_convert(_oas3()))["supports_get"] is False


def test_a_sub_collection_is_a_resource_in_its_own_right():
    """`/widgets/{widgetId}/audit` reads audit records, not one widget.

    It is named for its own last literal segment and inherits the parent's
    placeholder as a path parameter the operator has to supply.
    """
    report = _convert(_crud_doc("/widgets/{widgetId}/audit", ["get"]))
    audit = next(r for r in report.spec["resources"] if r["name"] == "audit")
    assert audit["endpoint"] == "/widgets/{widgetId}/audit"
    assert audit["path_params"] == ["widgetId"]
    # And it did not steal the parent's item route.
    assert next(r for r in report.spec["resources"] if r["name"] == "widgets")["supports_get"] is False


def test_update_and_delete_are_read_off_the_item_route():
    """PUT/PATCH/DELETE address one record, so they are declared on `/x/{id}`."""
    resource = _only_resource(_convert(_crud_doc("/widgets/{widgetId}", ["get", "put", "delete"])))
    assert resource["supports_update"] is True
    assert resource["supports_delete"] is True
    # PUT is the default verb, so no override is emitted.
    assert "update_method" not in resource


def test_a_patch_only_api_pins_the_update_verb():
    resource = _only_resource(_convert(_crud_doc("/widgets/{widgetId}", ["get", "patch"])))
    assert resource["supports_update"] is True
    assert resource["update_method"] == "PATCH"


def test_verbs_declared_on_the_collection_still_count():
    """Some APIs accept PUT/DELETE on the collection with the id in the body."""
    resource = _only_resource(_convert(_crud_doc("/widgets/{widgetId}", ["get"], ["put", "delete"])))
    assert resource["supports_update"] is True
    assert resource["supports_delete"] is True


def test_a_post_on_the_collection_is_create():
    resource = _only_resource(_convert(_crud_doc("/widgets/{widgetId}", ["get"], ["post"])))
    assert resource["supports_create"] is True


def test_a_read_only_api_advertises_no_write_verbs():
    resource = _only_resource(_convert(_oas3()))
    assert resource["supports_list"] is True
    assert (resource["supports_create"], resource["supports_update"], resource["supports_delete"]) == (
        False, False, False,
    )


# --------------------------------------------------------------------------- #
# Resources: selection
# --------------------------------------------------------------------------- #

def test_a_path_with_no_get_is_skipped_with_its_reason():
    doc = _oas3(paths={
        "/widgets": _oas3()["paths"]["/widgets"],
        "/ping": {"post": {"responses": {"204": {}}}},
    })
    report = _convert(doc)
    assert report.resource_count == 1
    assert any(s.startswith("/ping ") and "no GET" in s for s in report.skipped_paths)


def test_a_deprecated_collection_is_skipped_with_its_reason():
    doc = _oas3(paths={
        "/widgets": _oas3()["paths"]["/widgets"],
        "/invoices": {"get": {"deprecated": True, "responses": {"200": {}}}},
    })
    report = _convert(doc)
    assert report.resource_count == 1
    assert any(s.startswith("/invoices ") and "deprecated" in s for s in report.skipped_paths)


def test_the_least_parameterised_path_wins_a_name_collision():
    """`/orders` is callable out of the box; `/orgs/{orgId}/orders` needs config."""
    doc = _oas3(paths={
        "/orders": {"get": {"responses": {"200": {}}}},
        "/orgs/{orgId}/orders": {"get": {"responses": {"200": {}}}},
    })
    report = _convert(doc)
    resource = _only_resource(report)
    assert resource["endpoint"] == "/orders"
    assert resource["path_params"] == []
    assert any("duplicate resource 'orders'" in s for s in report.skipped_paths)


def test_the_winner_supersedes_a_parameterised_path_seen_first():
    """Paths are walked in sorted order, so the loser can arrive either side."""
    doc = _oas3(paths={
        "/accounts/{accountId}/orders": {"get": {"responses": {"200": {}}}},
        "/orders": {"get": {"responses": {"200": {}}}},
    })
    report = _convert(doc)
    assert _only_resource(report)["endpoint"] == "/orders"
    assert any("superseded by /orders" in s for s in report.skipped_paths)


def test_path_parameters_are_carried_onto_the_resource():
    doc = _oas3(paths={"/orgs/{orgId}/orders": {"get": {"responses": {"200": {}}}}})
    assert _only_resource(_convert(doc))["path_params"] == ["orgId"]


def test_max_resources_truncates_and_says_so():
    doc = _oas3(paths={
        "/widgets": {"get": {"responses": {"200": {}}}},
        "/orders": {"get": {"responses": {"200": {}}}},
        "/invoices": {"get": {"responses": {"200": {}}}},
    })
    report = _convert(doc, max_resources=2)
    assert report.resource_count == 2
    # Alphabetical, so the kept pair is deterministic.
    assert [r["name"] for r in report.spec["resources"]] == ["invoices", "orders"]
    assert any("Kept 2 of 3" in note for note in report.notes)


# --------------------------------------------------------------------------- #
# Resources: pagination, incremental sync, response envelope
# --------------------------------------------------------------------------- #

def _paged(query_names):
    return _oas3(paths={"/widgets": {"get": {
        "parameters": [{"name": n, "in": "query", "schema": {"type": "string"}} for n in query_names],
        "responses": {"200": {}},
    }}})


@pytest.mark.parametrize("param,style", [
    ("cursor", "cursor"),
    ("starting_after", "cursor"),
    ("page_token", "cursor"),
    ("offset", "offset"),
    ("skip", "offset"),
    ("page", "page"),
])
def test_pagination_style_is_read_off_the_declared_query_parameters(param, style):
    resource = _only_resource(_convert(_paged([param, "limit"])))
    assert resource["pagination_type"] == style
    assert resource["pagination_param"] == param


def test_a_cursor_style_also_names_the_cursor_parameter():
    resource = _only_resource(_convert(_paged(["starting_after"])))
    assert resource["cursor_param"] == "starting_after"


def test_an_offset_style_names_no_cursor_parameter():
    resource = _only_resource(_convert(_paged(["offset"])))
    assert "cursor_param" not in resource


def test_an_undeclared_pagination_scheme_is_left_to_the_field_default():
    """Emitting `pagination_type: None` would override the schema default."""
    resource = _only_resource(_convert(_paged(["fields"])))
    assert "pagination_type" not in resource
    assert "pagination_param" not in resource


def test_the_page_size_parameter_is_taken_from_the_document():
    assert _only_resource(_convert(_paged(["offset", "per_page"])))["limit_param"] == "per_page"


def test_the_page_size_parameter_falls_back_to_limit():
    assert _only_resource(_convert(_paged(["offset"])))["limit_param"] == "limit"


def test_query_parameter_matching_ignores_case_but_keeps_the_declared_spelling():
    """The comparison is case-insensitive; the value sent on the wire is not."""
    resource = _only_resource(_convert(_paged(["Offset", "PageSize"])))
    assert resource["pagination_param"] == "Offset"
    assert resource["limit_param"] == "PageSize"


@pytest.mark.parametrize("param", ["updated_since", "modified_since", "if_modified_since"])
def test_a_modification_filter_enables_incremental_sync(param):
    resource = _only_resource(_convert(_paged([param])))
    assert resource["supports_incremental"] is True
    assert resource["incremental_param"] == param


def test_no_modification_filter_means_no_incremental_claim():
    resource = _only_resource(_convert(_paged(["limit"])))
    assert "supports_incremental" not in resource


def _enveloped(schema, components=None):
    doc = _oas3(paths={"/widgets": {"get": {"responses": {"200": {
        "content": {"application/json": {"schema": schema}}
    }}}}})
    if components:
        doc["components"] = components
    return doc


def test_the_record_array_key_is_found_in_the_response_envelope():
    schema = {"type": "object", "properties": {
        "has_more": {"type": "boolean"},
        "data": {"type": "array", "items": {}},
    }}
    assert _only_resource(_convert(_enveloped(schema)))["response_data_key"] == "data"


def test_an_unconventional_envelope_key_is_still_found():
    schema = {"type": "object", "properties": {"payload": {"type": "array", "items": {}}}}
    assert _only_resource(_convert(_enveloped(schema)))["response_data_key"] == "payload"


def test_a_bare_array_response_declares_no_envelope_key():
    """The runtime auto-detects a top-level array; naming a key would break it."""
    assert "response_data_key" not in _only_resource(_convert(_enveloped({"type": "array", "items": {}})))


def test_a_referenced_response_schema_is_resolved():
    schema = {"$ref": "#/components/schemas/WidgetList"}
    components = {"schemas": {"WidgetList": {
        "type": "object", "properties": {"results": {"type": "array", "items": {}}},
    }}}
    assert _only_resource(_convert(_enveloped(schema, components)))["response_data_key"] == "results"


def test_a_reference_that_does_not_resolve_is_not_fatal():
    """An unresolvable $ref means one unknown field, not a failed conversion."""
    report = _convert(_enveloped({"$ref": "#/components/schemas/Missing"}))
    assert "response_data_key" not in _only_resource(report)


def test_a_swagger_2_response_schema_is_read_from_its_own_slot():
    doc = {
        "swagger": "2.0",
        "info": {"title": "Legacy Ledger"},
        "host": "legacy.example.com",
        "paths": {"/accounts": {"get": {"responses": {"200": {"schema": {
            "type": "object", "properties": {"items": {"type": "array", "items": {}}},
        }}}}}},
    }
    assert _convert(doc).spec["resources"][0]["response_data_key"] == "items"


# --------------------------------------------------------------------------- #
# Config fields: the connection form the operator fills in
# --------------------------------------------------------------------------- #

def _fields(doc):
    return {f["name"]: f for f in _convert(doc).spec["config_fields"]}


def test_every_connector_offers_a_base_url_override():
    field = _fields(_oas3())["base_url"]
    assert field["required"] is False
    assert field["default"] == "https://api.widgetco.test/v2"
    assert field["env_var"] == "MCP_BASE_URL"


@pytest.mark.parametrize("scheme,expected", [
    ({"type": "apiKey", "in": "header", "name": "X-Api-Key"}, ["api_key"]),
    ({"type": "apiKey", "in": "query", "name": "api_token"}, ["api_key"]),
    ({"type": "http", "scheme": "bearer"}, ["access_token"]),
    ({"type": "http", "scheme": "basic"}, ["username", "password"]),
])
def test_the_credential_fields_match_the_auth_type(scheme, expected):
    """A field the template's auth dispatcher never reads is a form that does nothing."""
    fields = _fields(_with_schemes({"s": scheme}))
    assert [n for n in fields if n != "base_url"] == expected
    for name in expected:
        assert fields[name]["required"] is True


def test_the_api_key_field_names_the_header_it_is_sent_in():
    field = _fields(_with_schemes({"s": {"type": "apiKey", "in": "header", "name": "X-Api-Key"}}))["api_key"]
    assert "X-Api-Key" in field["description"]


def test_the_query_key_field_names_the_url_parameter():
    field = _fields(_with_schemes({"s": {"type": "apiKey", "in": "query", "name": "api_token"}}))["api_key"]
    assert "api_token" in field["description"]


def test_a_username_is_not_marked_secret_but_a_password_is():
    fields = _fields(_with_schemes({"s": {"type": "http", "scheme": "basic"}}))
    assert fields["username"]["secret"] is False
    assert fields["password"]["secret"] is True


def test_an_undetermined_auth_offers_an_optional_key_rather_than_no_field_at_all():
    fields = _fields(_oas3())
    assert fields["api_key"]["required"] is False
    assert fields["api_key"]["secret"] is True


# --------------------------------------------------------------------------- #
# Operations
# --------------------------------------------------------------------------- #

def test_a_source_connector_declares_the_core_operations_and_a_read():
    spec = _convert(_oas3()).spec
    names = [op["name"] for op in spec["operations"]]
    assert names == ["test_connection", "validate_config", "discover_schema", "get_capabilities", "export"]
    assert spec["supports_source"] is True
    assert spec["supports_destination"] is False


def test_cloud_storage_reads_rather_than_exports():
    spec = _convert(_oas3(), category="cloud_storage").spec
    assert [op["name"] for op in spec["operations"]][-1] == "read"


def test_destination_support_is_opt_in():
    spec = _convert(_oas3(), supports_destination=True).spec
    assert spec["supports_destination"] is True
    assert {"name": "import_data", "type": "destination"} in spec["operations"]


# --------------------------------------------------------------------------- #
# Determinism and hygiene
# --------------------------------------------------------------------------- #

def test_the_same_document_always_produces_the_same_spec():
    """Determinism is the product claim; a set or a dict order would break it."""
    doc = _oas3(paths={
        "/widgets": {"get": {"responses": {"200": {}}}},
        "/orders": {"get": {"responses": {"200": {}}}},
        "/invoices": {"get": {"responses": {"200": {}}}},
    })
    doc["components"] = {"securitySchemes": {
        "oauth": {"type": "oauth2", "flows": {}},
        "key": {"type": "apiKey", "in": "header", "name": "X-Api-Key"},
    }}
    first, second = _convert(doc), _convert(doc)
    assert first.spec == second.spec
    assert first.notes == second.notes
    assert first.skipped_paths == second.skipped_paths


def test_notes_are_recorded_once_however_often_they_are_added():
    report = ConversionReport(spec={})
    report.add_note("same")
    report.add_note("same")
    assert report.notes == ["same"]


def test_a_long_description_is_truncated_rather_than_carried_whole():
    report = _convert(_oas3(info={"title": "Widget Co API", "description": "x" * 900}))
    assert len(report.spec["description"]) == 500


@pytest.mark.parametrize("raw,expected", [
    ("Widget Co API", "widget_co_api"),
    ("  Stripe  ", "stripe"),
    ("Foo//Bar", "foo_bar"),
    ("", ""),
])
def test_slugify_produces_a_python_safe_identifier(raw, expected):
    assert slugify(raw) == expected


def test_the_converter_imports_only_the_standard_library():
    """The conversion half must stay dependency-free.

    Read statically rather than by inspecting `sys.modules`: by the time this
    runs, pytest has imported most of the world, so an in-process census could
    not tell what THIS module pulled in. The render half's import closure is
    checked in `test_scaffold_renders_offline.py`, in a subprocess.
    """
    import agents.tool_generator.scaffold.openapi_to_spec as module

    source = open(module.__file__, encoding="utf-8").read()
    imported = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            # A relative import would reach back into the generation package.
            assert node.level == 0 or node.module == "__future__", (
                f"relative import of {'.' * node.level}{node.module or ''}"
            )
            if node.module:
                imported.add(node.module.split(".")[0])

    allowed = {"__future__", "logging", "re", "dataclasses", "typing"}
    assert imported <= allowed, f"non-stdlib imports appeared: {sorted(imported - allowed)}"


def test_the_scaffold_package_is_where_the_cut_detector_can_see_it():
    """`tests/conftest.py` decides this suite's public fate by reading its imports.

    Two separate things, and only the first is fixed. The import above must stay
    spelled `agents.tool_generator.*`: a bare `from scaffold...` is invisible to
    the detector, so the suite would be collected in the public repo with nothing
    to import and turn the whole `llm-service/tests` run into a collection error.

    The verdict itself is NOT fixed, and this assertion used to hard-code it as
    "ignored" -- true when the scaffolder was written, false one commit later when
    `scaffold/` was promoted out of `oss-strip-list.txt` so that self-hosted users
    would actually get it. A test that states today's answer instead of today's
    rule goes red at the promotion it exists to allow, so the expectation is read
    from the strip list and inverts on its own if `scaffold/` ever goes back.
    """
    import _cut_collection

    here = os.path.abspath(__file__)
    reaches = _cut_collection._imported_modules(here) or []
    assert any(m.startswith("agents.tool_generator.scaffold") for m in reaches), (
        "no absolute `agents.tool_generator.scaffold` import left in this file; the "
        f"detector cannot see {sorted(set(reaches))} as reaching into the package"
    )

    stripped = "scaffold" in _cut_collection.STRIPPED_MODULES
    ignored = os.path.basename(here) in _cut_collection.ignored_modules(
        os.path.dirname(here)
    )
    assert ignored is stripped, (
        f"oss-strip-list.txt {'strips' if stripped else 'ships'} "
        f"src/agents/tool_generator/scaffold, so tests/conftest.py should "
        f"{'drop' if stripped else 'collect'} this suite publicly, but the detector "
        f"says {'drop' if ignored else 'collect'}"
    )
