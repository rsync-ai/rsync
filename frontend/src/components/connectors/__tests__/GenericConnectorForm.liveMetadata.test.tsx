import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen } from "@testing-library/react"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm, computeAuthUI } from "../GenericConnectorForm"
import fixture from "./fixtures/live-connectors.staging.json"

// ---------------------------------------------------------------------------
// Data-driven render test against the REAL deployed connector metadata.
//
// The §6 computeAuthUI matrix asserts the decision tree against synthetic
// shapes. This test closes the remaining loop: it feeds the ACTUAL metadata the
// staging api-gateway serves (captured into the fixture after the Phase A2
// backfill + Phase D deploy) through the full GenericConnectorForm and asserts
// each connector renders the auth block the contract requires — proving the
// deployed metadata and the UI agree end-to-end, not just in unit isolation.
//
// EXPECT is hand-authored from the plan §3 precedence (NOT derived from
// computeAuthUI), so a regression in either the metadata or the selector fails
// the test. Regenerate the fixture if connector auth metadata changes.
// ---------------------------------------------------------------------------

const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => jsonRes({})),
}))
vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: ({ provider }: { provider: string }) => (
    <button type="button" data-testid="oauth-connect">
      Connect {provider}
    </button>
  ),
}))
vi.mock("@/components/connectors/AuthMethodPicker", () => ({
  AuthMethodPicker: ({ selectedMethod }: { selectedMethod: string }) => (
    <div data-testid="auth-picker" data-method={selectedMethod} />
  ),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/lib/api/mcp-connectors", () => ({ testMCPConnection: vi.fn() }))

type Expectation = { kind: string; oauthConnect: boolean }

// The contract every deployed connector's served metadata must satisfy.
// Post-A2 every credentialed connector declares supported_auth_methods → the
// picker is authoritative (kind "picker"); only the auth_type:none public API
// (countries-gql) renders no auth UI. The two connectors whose FIRST method is
// oauth2 with a resolvable provider also surface the standalone Connect block.
const EXPECT: Record<string, Expectation> = {
  "aws-s3": { kind: "picker", oauthConnect: false }, // api_key, schema creds (the #290 net)
  "countries-gql": { kind: "none", oauthConnect: false }, // public API, no auth
  "github-rest": { kind: "picker", oauthConnect: false }, // bearer
  "google-sheets": { kind: "picker", oauthConnect: true }, // oauth2 + provider google
  "mysql": { kind: "picker", oauthConnect: false }, // basic
  "petstore": { kind: "picker", oauthConnect: true }, // oauth2 + provider petstore
  "postgresql": { kind: "picker", oauthConnect: false }, // basic
  "shopify-admin-graphql": { kind: "picker", oauthConnect: false }, // first method custom_header (oauth2 secondary)
}

const connectors = (fixture as unknown as { connectors: MCPConnector[] }).connectors

beforeEach(() => vi.clearAllMocks())

describe("GenericConnectorForm — LIVE staging metadata renders the computeAuthUI-correct auth block", () => {
  it("fixture covers exactly the connectors the contract enumerates", () => {
    expect(connectors.map((c) => c.name).sort()).toEqual(Object.keys(EXPECT).sort())
  })

  for (const connector of connectors) {
    const name = connector.name
    const exp = EXPECT[name]

    it(`${name} (${connector.auth_type}) → kind "${exp.kind}", oauthConnect=${exp.oauthConnect}`, () => {
      const defaultMethod = connector.supported_auth_methods?.[0]?.method ?? ""

      // 1. The pure decision matches the hand-authored contract.
      const decision = computeAuthUI(connector, defaultMethod)
      expect(decision.kind).toBe(exp.kind)
      expect(decision.showOAuthConnect).toBe(exp.oauthConnect)

      // 2. The fully-rendered form matches that decision.
      render(
        <GenericConnectorForm
          connector={{ ...connector, docker_status: "running" } as MCPConnector}
          onSave={vi.fn()}
          onCancel={vi.fn()}
          initialData={{ connectionName: `${name} conn` }}
        />,
      )

      // The picker block and the synthesized fallback are mutually exclusive by
      // construction (kind "fallback" requires zero supported_auth_methods,
      // every picker connector has ≥1), so the picker's presence alone proves
      // no bogus synthesized credential field rendered — the #290/#291 net.
      if (exp.kind === "picker") {
        expect(screen.getByTestId("auth-picker")).toBeInTheDocument()
      } else {
        expect(screen.queryByTestId("auth-picker")).not.toBeInTheDocument()
      }

      if (exp.oauthConnect) {
        expect(screen.getByTestId("oauth-connect")).toBeInTheDocument()
      } else {
        expect(screen.queryByTestId("oauth-connect")).not.toBeInTheDocument()
      }
    })
  }
})
