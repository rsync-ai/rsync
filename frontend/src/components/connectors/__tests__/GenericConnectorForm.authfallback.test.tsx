import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, fireEvent } from "@testing-library/react"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm } from "../GenericConnectorForm"

// ---------------------------------------------------------------------------
// Auth-fallback coverage. Some generated connectors declare an auth_type that
// needs a credential but emit NO way to enter it — no oauth_provider, no
// supported_auth_methods, and no credential key in configuration_schema
// (real example: github-rest → auth_type "bearer", empty schema). The modal
// then renders ZERO auth UI and the connector is unusable. The form must
// synthesize a credential field from auth_type so every connector is authable,
// WITHOUT duplicating an input when the metadata already provides one.
// ---------------------------------------------------------------------------

const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => jsonRes({})),
}))
vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: () => <button type="button">Connect</button>,
}))
// Stub the multi-auth picker so case 3 only needs hasMultiAuth to be true.
vi.mock("@/components/connectors/AuthMethodPicker", () => ({
  AuthMethodPicker: () => <div data-testid="auth-picker" />,
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/lib/api/mcp-connectors", () => ({ testMCPConnection: vi.fn() }))

const base = {
  supports_source: true,
  supports_destination: false,
  supports_cdc: false,
  docker_status: "running",
  configuration_schema: { properties: {}, required: [] },
}

const mk = (over: Record<string, unknown>) => ({ ...base, ...over }) as unknown as MCPConnector

beforeEach(() => vi.clearAllMocks())

describe("GenericConnectorForm — auth fallback", () => {
  it("bearer connector with no auth metadata renders a credential field and gates Save until filled", () => {
    // github-rest shape: auth_type bearer, no oauth_provider, no supported_auth_methods, empty schema.
    render(
      <GenericConnectorForm
        connector={mk({ connector_type: "github-rest", display_name: "Github-Rest", auth_type: "bearer" })}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "gh conn" }}
      />,
    )

    const tokenInput = screen.getByPlaceholderText(/access token/i)
    expect(tokenInput).toBeInTheDocument()
    expect(screen.getByText(/Provide your Github-Rest access token below/i)).toBeInTheDocument()

    // Save is gated while the synthesized credential is empty…
    const save = screen.getByRole("button", { name: /Save Connection/i })
    expect(save).toBeDisabled()
    // …and enabled once it is provided.
    fireEvent.change(tokenInput, { target: { value: "ghp_xxx" } })
    expect(save).not.toBeDisabled()
  })

  it("api_key auth_type with no schema field renders an API Key field", () => {
    render(
      <GenericConnectorForm
        connector={mk({ connector_type: "acme", display_name: "Acme", auth_type: "api_key" })}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "acme conn" }}
      />,
    )
    expect(screen.getByText(/Provide your Acme API key below/i)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/your api key/i)).toBeInTheDocument()
  })

  it("does NOT render the fallback when the connector already declares supported_auth_methods", () => {
    render(
      <GenericConnectorForm
        connector={mk({
          connector_type: "multi",
          display_name: "Multi",
          auth_type: "bearer",
          supported_auth_methods: [{ method: "bearer", label: "Bearer", config_keys: ["access_token"] }],
        })}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "multi conn" }}
      />,
    )
    // The multi-auth picker owns credential entry; no synthesized fallback.
    expect(screen.getByTestId("auth-picker")).toBeInTheDocument()
    expect(screen.queryByText(/access token below/i)).not.toBeInTheDocument()
  })

  it("oauth2 picker method with NO resolvable provider gates Save in BOTH create and edit mode", () => {
    // Broken/generated metadata: a multi-auth connector ships an oauth2 method
    // with no provider (neither method.oauth_provider nor connector.oauth_provider).
    // It can't be completed (no Connect, no paste-token), so Save must stay
    // disabled regardless of mode — the edit-mode bypass the adversarial review
    // caught. (No shipping connector hits this; guards future metadata.)
    const broken = {
      connector_type: "brokenoauth",
      display_name: "BrokenOAuth",
      supported_auth_methods: [{ method: "oauth2", config_keys: ["access_token"] }],
    }

    const { unmount } = render(
      <GenericConnectorForm
        connector={mk(broken)}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "create conn" }}
        isEditing={false}
      />,
    )
    expect(screen.getByRole("button", { name: /Save Connection/i })).toBeDisabled()
    unmount()

    render(
      <GenericConnectorForm
        connector={mk(broken)}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "edit conn", config: { auth_method: "oauth2" } }}
        isEditing
        connectionId="conn_x"
      />,
    )
    expect(screen.getByRole("button", { name: /Update Connection/i })).toBeDisabled()
  })

  it("basic auth_type renders username + password fields", () => {
    render(
      <GenericConnectorForm
        connector={mk({ connector_type: "legacy", display_name: "Legacy", auth_type: "basic" })}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "legacy conn" }}
      />,
    )
    expect(screen.getByPlaceholderText(/^username$/i)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/^password$/i)).toBeInTheDocument()
  })

  it("does NOT synthesize a fallback when the schema has non-standard credential keys (aws-s3)", () => {
    // aws-s3 shape: auth_type api_key, no oauth_provider, no supported_auth_methods,
    // but the schema DOES carry credentials under non-standard keys
    // (access_key_id / secret_access_key). The exact-match credential set missed
    // these → a bogus "API Key" field was synthesized ON TOP of the real fields
    // and gated Save (regression from #290). Substring detection must suppress it.
    render(
      <GenericConnectorForm
        connector={mk({
          connector_type: "aws-s3",
          display_name: "Aws-S3",
          auth_type: "api_key",
          configuration_schema: {
            properties: { access_key_id: {}, secret_access_key: {} },
            required: ["access_key_id", "secret_access_key"],
          },
        })}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "s3 conn" }}
      />,
    )

    // No synthesized fallback (its heading + placeholder must be absent)…
    expect(screen.queryByText(/Provide your .* API key below/i)).not.toBeInTheDocument()
    expect(screen.queryByPlaceholderText(/^your api key$/i)).not.toBeInTheDocument()
    // …and the real schema-declared credential fields render instead.
    expect(screen.getByText(/Access Key Id/i)).toBeInTheDocument()
    expect(screen.getByText(/Secret Access Key/i)).toBeInTheDocument()
  })
})
