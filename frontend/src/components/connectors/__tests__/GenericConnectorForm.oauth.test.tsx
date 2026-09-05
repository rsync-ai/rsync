import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm } from "../GenericConnectorForm"

// ---------------------------------------------------------------------------
// Regression coverage for the PR #288 mount-time OAuth token-detection effect.
//
// Bug: `detectExistingToken` flipped `oauthConnected=true` whenever ANY
// non-expired token for the provider existed in the DB — with no `isEditing`
// guard. So opening the *create* modal for an OAuth connector that had a stale
// token from a prior flow showed the green "Authentication successful!" banner
// (and enabled Save) without the user ever completing OAuth — and could even
// render alongside "OAuth Not Available / 502".
//
// Correct behaviour:
//   - create mode  → never auto-detect; banner only after a real OAuth success.
//   - edit mode    → reflect the connection's own token (config.oauth_token_id).
//   - secret change→ saving the BYO OAuth app clears connected → forces re-auth.
// ---------------------------------------------------------------------------

const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })

// vi.mock factories are hoisted above imports — share mutable server state via
// vi.hoisted so each test can stage its own /oauth/* responses.
const h = vi.hoisted(() => ({
  server: {
    providers: { providers: [{ name: "shopify", enabled: false }] } as unknown,
    app: {
      provider: "shopify",
      known: true,
      configured: false,
      operator_managed: false,
      redirect_uri: "http://localhost:5001/oauth/callback/shopify",
      default_scopes: "read_products",
    } as unknown,
    tokens: { tokens: [] as Array<Record<string, unknown>> } as unknown,
    appsPost: { redirect_uri: "http://localhost:5001/oauth/callback/shopify", client_id: "cid" } as unknown,
  },
}))

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async (url: string) => {
    const u = String(url)
    if (u.includes("/oauth/providers")) return jsonRes(h.server.providers)
    if (u.includes("/oauth/tokens")) return jsonRes(h.server.tokens)
    if (u.includes("/oauth/apps/")) return jsonRes(h.server.app) // GET /apps/:provider
    if (u.includes("/oauth/apps")) return jsonRes(h.server.appsPost) // POST /apps
    return jsonRes({})
  }),
}))

// Stub the OAuth popup button — its real implementation opens a window.
vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: (p: { disabled?: boolean; onSuccess?: (t: string) => void }) => (
    <button type="button" disabled={p.disabled} onClick={() => p.onSuccess?.("tok_live")}>
      Connect
    </button>
  ),
}))

vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/lib/api/mcp-connectors", () => ({ testMCPConnection: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"

const shopify = {
  connector_type: "shopify",
  display_name: "Shopify",
  oauth_provider: "shopify",
  auth_type: "oauth",
  supports_source: true,
  supports_destination: false,
  supports_cdc: false,
  docker_status: "running",
  configuration_schema: { properties: {}, required: [] },
} as unknown as MCPConnector

const tokensWereFetched = () =>
  (authFetch as unknown as { mock: { calls: unknown[][] } }).mock.calls.some((c) =>
    String(c[0]).includes("/oauth/tokens"),
  )

beforeEach(() => {
  vi.clearAllMocks()
  h.server.providers = { providers: [{ name: "shopify", enabled: false }] }
  h.server.app = {
    provider: "shopify",
    known: true,
    configured: false,
    operator_managed: false,
    redirect_uri: "http://localhost:5001/oauth/callback/shopify",
    default_scopes: "read_products",
  }
  h.server.tokens = { tokens: [] }
})

describe("GenericConnectorForm — OAuth connected-state", () => {
  it("CREATE mode: a stale provider token must NOT show 'Authentication successful' or enable Save", async () => {
    // A leftover, never-expiring token exists for shopify (prior session).
    h.server.tokens = {
      tokens: [
        { id: "tok_stale", provider: "shopify", expires_at: "2126-01-01T00:00:00Z", created_at: "2026-06-20T14:59:00Z" },
      ],
    }

    render(
      <GenericConnectorForm
        connector={shopify}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "new shopify conn" }}
        isEditing={false}
      />,
    )

    // Settle point: BYO section renders only after the mount effects resolve.
    await screen.findByText(/Your OAuth App/i)

    expect(screen.queryByText(/Authentication successful/i)).not.toBeInTheDocument()
    // The fix early-returns before ever querying /oauth/tokens in create mode.
    expect(tokensWereFetched()).toBe(false)
    // Save stays gated on a real OAuth completion.
    expect(screen.getByRole("button", { name: /Save Connection/i })).toBeDisabled()
  })

  it("EDIT mode: reflects the connection's own stored token (config.oauth_token_id) even with no provider token", async () => {
    h.server.tokens = { tokens: [] } // lookup would find nothing — config must drive it

    render(
      <GenericConnectorForm
        connector={shopify}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "existing", config: { oauth_token_id: "tok_existing" } }}
        isEditing
        connectionId="conn_1"
      />,
    )

    expect(await screen.findByText(/Authentication successful/i)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /Update Connection/i })).not.toBeDisabled()
  })

  it("CREDENTIAL CHANGE: saving the BYO OAuth app clears the connected banner (forces re-auth)", async () => {
    h.server.tokens = { tokens: [] }

    render(
      <GenericConnectorForm
        connector={shopify}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "existing", config: { oauth_token_id: "tok_existing" } }}
        isEditing
        connectionId="conn_1"
      />,
    )

    // Starts connected (edit mode reflects the stored token).
    expect(await screen.findByText(/Authentication successful/i)).toBeInTheDocument()

    // User re-enters credentials and saves the OAuth app. The BYO form renders
    // after loadByoApp's async fetch resolves, so wait for the input rather than
    // assuming it's present the instant the (synchronous) banner appears.
    fireEvent.change(await screen.findByPlaceholderText(/client ID/i), { target: { value: "new-client-id" } })
    fireEvent.change(screen.getByPlaceholderText(/client secret/i), { target: { value: "new-secret" } })
    fireEvent.click(screen.getByRole("button", { name: /Save OAuth App/i }))

    // After saving new credentials the stale connected state must clear.
    await waitFor(() =>
      expect(screen.queryByText(/Authentication successful/i)).not.toBeInTheDocument(),
    )
  })
})
