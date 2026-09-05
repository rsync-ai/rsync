import { describe, it, expect } from "vitest"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { computeAuthUI } from "../GenericConnectorForm"

// ---------------------------------------------------------------------------
// The §6 auth-shape matrix, asserted against the ONE pure decision function
// (plan §3). One case per auth SHAPE, not per connector. computeAuthUI is pure
// over (connector, authMethod), so these need no DOM — they pin the precedence
// and the regression nets (#290 aws-s3, #291 github-rest, the oauth2 footgun,
// the oauth_provider+non-oauth bypass) at the source of truth.
// ---------------------------------------------------------------------------

const mk = (over: Partial<MCPConnector>): MCPConnector =>
  ({
    configuration_schema: { type: "object", required: [], properties: {} },
    ...over,
  }) as unknown as MCPConnector

describe("computeAuthUI — the single auth decision tree (§3)", () => {
  it("none / public API → no auth UI", () => {
    const d = computeAuthUI(mk({ auth_type: "none" }), "")
    expect(d.kind).toBe("none")
    expect(d.showOAuthConnect).toBe(false)
    expect(d.requiresOAuthConnected).toBe(false)
  })

  it("empty auth_type → no auth UI", () => {
    expect(computeAuthUI(mk({}), "").kind).toBe("none")
  })

  it("api_key with multi-field non-standard schema (aws-s3, #290 net) → schema, NO synthesis", () => {
    const d = computeAuthUI(
      mk({
        auth_type: "api_key",
        configuration_schema: {
          type: "object",
          required: ["access_key_id", "secret_access_key"],
          properties: { access_key_id: { type: "string", description: "" }, secret_access_key: { type: "string", description: "" } },
        },
      }),
      "",
    )
    expect(d.kind).toBe("schema") // not "fallback" — the substring predicate sees the creds
  })

  it("bearer WITH a schema credential (github-rest #291 net) → schema", () => {
    const d = computeAuthUI(
      mk({
        auth_type: "bearer",
        configuration_schema: {
          type: "object",
          required: ["access_token"],
          properties: { access_token: { type: "string", description: "" } },
        },
      }),
      "",
    )
    expect(d.kind).toBe("schema")
  })

  it("bearer with an EMPTY schema (github-rest as shipped) → synthesized fallback", () => {
    expect(computeAuthUI(mk({ auth_type: "bearer" }), "").kind).toBe("fallback")
  })

  it("api_key with empty schema → fallback", () => {
    expect(computeAuthUI(mk({ auth_type: "api_key" }), "").kind).toBe("fallback")
  })

  it("custom_header with empty schema → fallback", () => {
    expect(computeAuthUI(mk({ auth_type: "custom_header" }), "").kind).toBe("fallback")
  })

  it("single-scheme oauth2 (oauth_provider, no methods) → OAuth Connect, gated", () => {
    const d = computeAuthUI(mk({ auth_type: "oauth2", oauth_provider: "shopify" }), "")
    expect(d.kind).toBe("oauth")
    expect(d.showOAuthConnect).toBe(true)
    expect(d.oauthProvider).toBe("shopify")
    expect(d.requiresOAuthConnected).toBe(true)
  })

  it("legacy auth_type 'oauth' normalizes to oauth2", () => {
    expect(computeAuthUI(mk({ auth_type: "oauth", oauth_provider: "x" }), "").kind).toBe("oauth")
    // 'oauth' without a provider needs no credential → no auth UI (not fallback).
    expect(computeAuthUI(mk({ auth_type: "oauth" }), "").kind).toBe("none")
  })

  it("oauth_provider set but auth_type=api_key, no methods → still OAuth-gated (the bypass fix)", () => {
    // The old Test/Save gate only fired for auth_type oauth/oauth2, so this shape
    // let Save through without a completed authorization. Row 2 now classifies it
    // as the OAuth path, so requiresOAuthConnected is true.
    const d = computeAuthUI(mk({ auth_type: "api_key", oauth_provider: "acme" }), "")
    expect(d.kind).toBe("oauth")
    expect(d.requiresOAuthConnected).toBe(true)
  })

  describe("multi-auth picker (methods authoritative)", () => {
    const pipedrive = (authMethod: string, opts: { methodProvider?: string; connectorProvider?: string } = {}) =>
      computeAuthUI(
        mk({
          auth_type: "api_key",
          oauth_provider: opts.connectorProvider,
          supported_auth_methods: [
            { method: "api_key", header_name: "", header_prefix: "", config_keys: ["api_key"], description: "" },
            {
              method: "oauth2",
              header_name: "Authorization",
              header_prefix: "Bearer ",
              config_keys: ["access_token"],
              description: "",
              oauth_provider: opts.methodProvider,
            },
          ],
        }),
        authMethod,
      )

    it("non-oauth method selected → picker, no OAuth Connect, not OAuth-gated", () => {
      const d = pipedrive("api_key", { connectorProvider: "pipedrive", methodProvider: "pipedrive" })
      expect(d.kind).toBe("picker")
      expect(d.showOAuthConnect).toBe(false)
      expect(d.requiresOAuthConnected).toBe(false)
    })

    it("oauth2 method selected WITH a provider → picker + OAuth Connect, gated", () => {
      const d = pipedrive("oauth2", { connectorProvider: "pipedrive", methodProvider: "pipedrive" })
      expect(d.kind).toBe("picker")
      expect(d.showOAuthConnect).toBe(true)
      expect(d.oauthProvider).toBe("pipedrive")
      expect(d.requiresOAuthConnected).toBe(true)
    })

    it("per-method oauth_provider wins when the connector has none", () => {
      const d = pipedrive("oauth2", { methodProvider: "pipedrive" }) // no connectorProvider
      expect(d.showOAuthConnect).toBe(true)
      expect(d.oauthProvider).toBe("pipedrive")
    })

    it("oauth2 method selected with NO provider anywhere → footgun net: unconfigured, NOT a Connect", () => {
      const d = pipedrive("oauth2", {}) // neither provider
      expect(d.kind).toBe("picker")
      expect(d.showOAuthConnect).toBe(false)
      expect(d.oauthUnconfigured).toBe(true)
      expect(d.oauthProvider).toBeNull()
    })
  })
})
