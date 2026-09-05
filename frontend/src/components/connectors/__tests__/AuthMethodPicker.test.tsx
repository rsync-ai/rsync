import { describe, it, expect } from "vitest"
import { render, screen } from "@testing-library/react"
import type { SupportedAuthMethod } from "@/lib/types/mcp-connector"
import { AuthMethodPicker } from "../AuthMethodPicker"

// ---------------------------------------------------------------------------
// The oauth2-method footgun. The picker used to suppress the manual paste-token
// field ONLY when the `oauthProvider` prop was truthy. A multi-scheme connector
// declaring an oauth2 method but no registered provider therefore rendered a
// free-text `oauth_token_id` box and NO Connect button — the user pasted a token
// the runtime never used. oauth2 must NEVER render a paste field: with a provider
// it points at Connect; without one it says "OAuth not configured".
// ---------------------------------------------------------------------------

const m = (over: Partial<SupportedAuthMethod>): SupportedAuthMethod => ({
  method: "api_key",
  header_name: "",
  header_prefix: "",
  config_keys: ["api_key"],
  description: "",
  ...over,
})

const noop = () => {}

const renderPicker = (
  methods: SupportedAuthMethod[],
  selectedMethod: string,
  oauthProvider?: string | null,
) =>
  render(
    <AuthMethodPicker
      methods={methods}
      selectedMethod={selectedMethod}
      onMethodChange={noop}
      values={{}}
      onValueChange={noop}
      oauthProvider={oauthProvider}
    />,
  )

describe("AuthMethodPicker — oauth2 method handling", () => {
  it("oauth2 method with NO provider → 'OAuth not configured', NOT a paste-token field", () => {
    renderPicker(
      [m({ method: "api_key", config_keys: ["api_key"] }), m({ method: "oauth2", config_keys: ["access_token"] })],
      "oauth2",
    )
    expect(screen.getByTestId("oauth-not-configured")).toBeInTheDocument()
    // The footgun: no free-text token input for oauth2.
    expect(screen.queryByTestId("credential-access_token")).not.toBeInTheDocument()
    expect(screen.queryByTestId("credential-oauth_token_id")).not.toBeInTheDocument()
  })

  it("oauth2 method WITH a connector-level provider → points at Connect, no paste-token", () => {
    renderPicker(
      [m({ method: "api_key", config_keys: ["api_key"] }), m({ method: "oauth2", config_keys: ["access_token"] })],
      "oauth2",
      "shopify",
    )
    expect(screen.getByText(/Connect with OAuth/i)).toBeInTheDocument()
    expect(screen.queryByTestId("oauth-not-configured")).not.toBeInTheDocument()
    expect(screen.queryByTestId("credential-access_token")).not.toBeInTheDocument()
  })

  it("oauth2 method with a PER-METHOD provider (no connector prop) → points at Connect", () => {
    // The pipedrive-gap link: the method carries its own oauth_provider.
    renderPicker(
      [m({ method: "api_key", config_keys: ["api_key"] }), m({ method: "oauth2", config_keys: ["access_token"], oauth_provider: "pipedrive" })],
      "oauth2",
    )
    expect(screen.getByText(/Connect with OAuth/i)).toBeInTheDocument()
    expect(screen.queryByTestId("oauth-not-configured")).not.toBeInTheDocument()
  })

  it("a non-oauth method still renders its credential field", () => {
    renderPicker(
      [m({ method: "api_key", config_keys: ["api_key"] }), m({ method: "oauth2", config_keys: ["access_token"] })],
      "api_key",
    )
    expect(screen.getByTestId("credential-api_key")).toBeInTheDocument()
    expect(screen.queryByTestId("oauth-not-configured")).not.toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// Distinct credential fields vs alias key-names. The picker used to render only
// config_keys[0] for non-basic methods and treat the rest as aliases — which
// silently dropped aws-s3's required secret_access_key (no field, un-connectable,
// and Save/Test stayed enabled). We now disambiguate from the connector's
// configuration_schema: schema-backed keys are distinct fields; the rest aliases.
// ---------------------------------------------------------------------------
describe("AuthMethodPicker — distinct credential fields vs alias key-names", () => {
  const renderWithSchema = (
    methods: SupportedAuthMethod[],
    selectedMethod: string,
    schemaKeys: Set<string>,
  ) =>
    render(
      <AuthMethodPicker
        methods={methods}
        selectedMethod={selectedMethod}
        onMethodChange={noop}
        values={{}}
        onValueChange={noop}
        schemaKeys={schemaKeys}
      />,
    )

  it("api_key with TWO schema-backed keys (aws-s3) renders BOTH secrets — secret_access_key is no longer dropped", () => {
    renderWithSchema(
      [m({ method: "api_key", config_keys: ["access_key_id", "secret_access_key"] })],
      "api_key",
      new Set(["access_key_id", "secret_access_key"]),
    )
    expect(screen.getByTestId("credential-access_key_id")).toBeInTheDocument()
    expect(screen.getByTestId("credential-secret_access_key")).toBeInTheDocument()
    // Both are distinct fields → no "Also accepted under" alias note.
    expect(screen.queryByText(/Also accepted under/i)).not.toBeInTheDocument()
  })

  it("custom_header with an alias key absent from the schema (shopify access_token/token) renders ONE field + an alias note", () => {
    renderWithSchema(
      [m({ method: "custom_header", config_keys: ["access_token", "token"], header_name: "X-Shopify-Access-Token" })],
      "custom_header",
      new Set(["access_token"]), // 'token' is an alias name, not its own schema property
    )
    expect(screen.getByTestId("credential-access_token")).toBeInTheDocument()
    expect(screen.queryByTestId("credential-token")).not.toBeInTheDocument()
    expect(screen.getByText(/Also accepted under: token/i)).toBeInTheDocument()
  })

  it("single-key api_key with empty schema still renders its one field (no regression)", () => {
    renderWithSchema([m({ method: "api_key", config_keys: ["api_key"] })], "api_key", new Set())
    expect(screen.getByTestId("credential-api_key")).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// Browser-autofill hardening. Chrome ignores autoComplete="off" on a password
// field and will autofill the saved localhost login into a connector credential
// field (and the email into the adjacent Description) — seen live on the Stripe
// modal. The secret input must use autoComplete="new-password" (the only value
// Chrome honors) and must NOT use a bullet-dot placeholder, which makes an empty
// field look identical to a saved/masked value.
// ---------------------------------------------------------------------------
describe("AuthMethodPicker — browser-autofill hardening", () => {
  it("secret credential field → autoComplete='new-password', no bullet placeholder", () => {
    renderPicker([m({ method: "api_key", config_keys: ["access_token"] })], "api_key")
    const input = screen.getByTestId("credential-access_token")
    expect(input).toHaveAttribute("autocomplete", "new-password")
    expect(input.getAttribute("placeholder") ?? "").not.toContain("•")
  })

  it("basic auth: username field → autoComplete='off', password field → 'new-password'", () => {
    renderPicker([m({ method: "basic", config_keys: ["username", "password"] })], "basic")
    expect(screen.getByTestId("credential-username")).toHaveAttribute("autocomplete", "off")
    expect(screen.getByTestId("credential-password")).toHaveAttribute("autocomplete", "new-password")
  })
})
