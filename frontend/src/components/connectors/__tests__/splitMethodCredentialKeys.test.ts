import { describe, it, expect } from "vitest"
import { splitMethodCredentialKeys, type SupportedAuthMethod } from "@/lib/types/mcp-connector"

// ---------------------------------------------------------------------------
// splitMethodCredentialKeys is the single source of truth shared by the auth
// picker (which fields to render) AND the form's Test/Save completeness gate
// (which fields must be filled). The bug it fixes: aws-s3's api_key method has
// two DISTINCT required secrets (access_key_id + secret_access_key) but the old
// "render config_keys[0], alias the rest" logic dropped the second — leaving no
// field for it and letting Save/Test pass with it empty (a silently-broken
// connection). Distinctness is derived from configuration_schema membership.
// (Lives under connectors/__tests__ because that's the vitest `include` scope.)
// ---------------------------------------------------------------------------

const method = (over: Partial<SupportedAuthMethod>): SupportedAuthMethod => ({
  method: "api_key",
  header_name: "",
  header_prefix: "",
  config_keys: ["api_key"],
  description: "",
  ...over,
})

describe("splitMethodCredentialKeys", () => {
  it("aws-s3: two schema-backed keys are BOTH distinct fields, no aliases", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "api_key", config_keys: ["access_key_id", "secret_access_key"] }),
      new Set(["access_key_id", "secret_access_key"]),
    )
    expect(out.fields).toEqual(["access_key_id", "secret_access_key"])
    expect(out.aliases).toEqual([])
  })

  it("shopify custom_header: schema-backed key is the field, the non-schema key is an alias", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "custom_header", config_keys: ["access_token", "token"] }),
      new Set(["access_token"]),
    )
    expect(out.fields).toEqual(["access_token"])
    expect(out.aliases).toEqual(["token"])
  })

  it("single-key api_key: one field, no aliases (regardless of schema)", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "api_key", config_keys: ["api_key"] }),
      new Set(["api_key"]),
    )
    expect(out.fields).toEqual(["api_key"])
    expect(out.aliases).toEqual([])
  })

  it("bearer with a single key: field is the key", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "bearer", config_keys: ["access_token"] }),
      new Set(["access_token"]),
    )
    expect(out.fields).toEqual(["access_token"])
    expect(out.aliases).toEqual([])
  })

  it("basic: all keys are distinct fields (username + password), schema irrelevant", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "basic", config_keys: ["user", "password"] }),
      new Set(),
    )
    expect(out.fields).toEqual(["user", "password"])
    expect(out.aliases).toEqual([])
  })

  it("oauth2 / oauth: no manual fields — the Connect flow owns the credential", () => {
    expect(splitMethodCredentialKeys(method({ method: "oauth2", config_keys: ["oauth_token_id", "access_token"] }), new Set(["access_token"]))).toEqual({ fields: [], aliases: [] })
    expect(splitMethodCredentialKeys(method({ method: "oauth", config_keys: ["access_token"] }), new Set(["access_token"]))).toEqual({ fields: [], aliases: [] })
  })

  it("no schema-backed keys → falls back to first-field + aliases (never zero fields)", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "custom_header", config_keys: ["primary", "alt"] }),
      new Set(),
    )
    expect(out.fields).toEqual(["primary"])
    expect(out.aliases).toEqual(["alt"])
  })

  it("is case-insensitive on schema membership", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "api_key", config_keys: ["Access_Key_Id", "Secret_Access_Key"] }),
      new Set(["access_key_id", "secret_access_key"]),
    )
    expect(out.fields).toEqual(["Access_Key_Id", "Secret_Access_Key"])
    expect(out.aliases).toEqual([])
  })

  // The single schema-backed key is NOT config_keys[0]. This is the post-#484
  // dedup shape: the generator forces the generic canonical (access_token / api_key)
  // to the FRONT of config_keys, but keeps the vendor-named field (integration_token,
  // bot_token, api_token) as the sole configuration_schema property — which is also
  // what required_config references and what the orchestrator's pre-start gate
  // (missingRequiredConfig) validates. Rendering config_keys[0] here submits the
  // credential under a key the backend gate rejects ("missing required config:
  // integration_token"). The rendered field MUST be the schema-backed (required) key,
  // with the generic config_keys aliased onto it.
  it("notion bearer: schema-backed vendor key (not config_keys[0]) is the field", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "bearer", config_keys: ["access_token", "token", "integration_token"] }),
      new Set(["integration_token"]),
    )
    expect(out.fields).toEqual(["integration_token"])
    expect(out.aliases).toEqual(["access_token", "token"])
  })

  it("slack bearer: schema-backed bot_token is the field, generic canonical aliased", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "bearer", config_keys: ["access_token", "token", "bot_token"] }),
      new Set(["bot_token"]),
    )
    expect(out.fields).toEqual(["bot_token"])
    expect(out.aliases).toEqual(["access_token", "token"])
  })

  it("pipedrive api_key: schema-backed api_token is the field, api_key aliased", () => {
    const out = splitMethodCredentialKeys(
      method({ method: "api_key_query", config_keys: ["api_key", "api_token"] }),
      new Set(["api_token"]),
    )
    expect(out.fields).toEqual(["api_token"])
    expect(out.aliases).toEqual(["api_key"])
  })
})
