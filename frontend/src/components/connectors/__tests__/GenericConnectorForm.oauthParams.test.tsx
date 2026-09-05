import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm } from "../GenericConnectorForm"

// ---------------------------------------------------------------------------
// P2 — metadata-driven OAuth authorize params. The modal used to hardcode
// `formData.shop || formData.subdomain → ?shop=` for Shopify. It now forwards
// whatever the connector declares in
// `oauth_authorize_params = { authorizeParam: configField }`, so any per-tenant
// OAuth connector (Shopify, Zendesk, self-hosted GitLab, …) works with zero
// modal changes. These tests pin that contract by capturing the extraParams the
// form hands to OAuthConnectButton.
// ---------------------------------------------------------------------------

const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })

// Capture the props OAuthConnectButton receives so we can assert the derived
// extraParams without opening a real popup.
const cap = vi.hoisted(() => ({ props: null as null | Record<string, unknown> }))

const h = vi.hoisted(() => ({
  server: {
    // enabled:true → oauthUsable, so the Connect button's only gate is the
    // required non-credential field (shop) being filled.
    providers: { providers: [{ name: "shopify", enabled: true }] } as unknown,
    app: { provider: "shopify", known: true, configured: false, operator_managed: true } as unknown,
    tokens: { tokens: [] as Array<Record<string, unknown>> } as unknown,
  },
}))

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async (url: string) => {
    const u = String(url)
    if (u.includes("/oauth/providers")) return jsonRes(h.server.providers)
    if (u.includes("/oauth/tokens")) return jsonRes(h.server.tokens)
    if (u.includes("/oauth/apps/")) return jsonRes(h.server.app)
    return jsonRes({})
  }),
}))

vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: (p: Record<string, unknown>) => {
    cap.props = p
    return (
      <button type="button" disabled={p.disabled as boolean}>
        Connect
      </button>
    )
  },
}))

vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/lib/api/mcp-connectors", () => ({ testMCPConnection: vi.fn() }))

const perTenant = {
  connector_type: "shopify-admin-graphql",
  name: "shopify-admin-graphql",
  display_name: "Shopify",
  oauth_provider: "shopify",
  auth_type: "oauth",
  oauth_authorize_params: { shop: "shop" },
  supports_source: true,
  supports_destination: false,
  supports_cdc: false,
  docker_status: "running",
  configuration_schema: {
    properties: {
      shop: { type: "string", description: "Your store subdomain — e.g. acme for acme.myshopify.com" },
    },
    required: ["shop"],
  },
} as unknown as MCPConnector

const standard = {
  connector_type: "hubspot",
  name: "hubspot",
  display_name: "HubSpot",
  oauth_provider: "hubspot",
  auth_type: "oauth",
  // no oauth_authorize_params
  supports_source: true,
  supports_destination: false,
  supports_cdc: false,
  docker_status: "running",
  configuration_schema: { properties: {}, required: [] },
} as unknown as MCPConnector

beforeEach(() => {
  vi.clearAllMocks()
  cap.props = null
  h.server.providers = { providers: [{ name: "shopify", enabled: true }] }
  h.server.app = { provider: "shopify", known: true, configured: false, operator_managed: true }
  h.server.tokens = { tokens: [] }
})

describe("GenericConnectorForm — metadata-driven OAuth authorize params", () => {
  it("forwards the mapped config field as the declared authorize param (shop)", async () => {
    render(<GenericConnectorForm connector={perTenant} onSave={vi.fn()} onCancel={vi.fn()} isEditing={false} />)

    await screen.findByRole("button", { name: /^Connect$/i })

    // Before the user fills shop, there are no extra params to forward.
    expect((cap.props?.extraParams as unknown) ?? undefined).toBeUndefined()

    // The per-field guidance now comes from the field's own schema description,
    // not a hardcoded shop blurb — and the old "don't include .myshopify.com"
    // special-case text is gone.
    expect(screen.getByText(/acme for acme\.myshopify\.com/i)).toBeInTheDocument()
    expect(screen.queryByText(/don't include/i)).not.toBeInTheDocument()

    fireEvent.change(screen.getByLabelText(/shop/i), { target: { value: "acme" } })

    await waitFor(() => expect(cap.props?.extraParams).toEqual({ shop: "acme" }))
  })

  it("trims the forwarded value and omits it when blank", async () => {
    render(<GenericConnectorForm connector={perTenant} onSave={vi.fn()} onCancel={vi.fn()} isEditing={false} />)
    await screen.findByRole("button", { name: /^Connect$/i })

    fireEvent.change(screen.getByLabelText(/shop/i), { target: { value: "  acme  " } })
    await waitFor(() => expect(cap.props?.extraParams).toEqual({ shop: "acme" }))

    fireEvent.change(screen.getByLabelText(/shop/i), { target: { value: "   " } })
    await waitFor(() => expect((cap.props?.extraParams as unknown) ?? undefined).toBeUndefined())
  })

  it("passes NO extra params for a standard global-endpoint OAuth connector", async () => {
    render(<GenericConnectorForm connector={standard} onSave={vi.fn()} onCancel={vi.fn()} isEditing={false} />)
    await screen.findByRole("button", { name: /^Connect$/i })

    expect((cap.props?.extraParams as unknown) ?? undefined).toBeUndefined()
  })
})
