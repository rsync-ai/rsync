import { describe, it, expect, vi, afterEach } from "vitest"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { render, screen, fireEvent, cleanup, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { APIRequestError } from "@/lib/errors/api-errors"
import { GenericConnectorForm } from "../GenericConnectorForm"

// ---------------------------------------------------------------------------
// KI-CONNECTOR-SAVE-ERROR-NOT-ANNOUNCED: a failed save drew a red box with no
// role, so a screen reader said nothing and focus stayed on Save.
// KI-CONNECTION-WIZARD-DISCARDS-SERVER-ERROR: the box showed a constant, never
// the server's reason or the connector's test error.
// KI-CONFLICT-ALWAYS-BLAMED-ON-CONNECTION-NAME: any 409 focused the name input.
//
// The rejections below are what tracedJsonFetch throws for the gateway's real
// response bodies (connections.go).
// ---------------------------------------------------------------------------

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}) })),
}))
vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: () => <button type="button">Connect</button>,
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
vi.mock("@/lib/api/mcp-connectors", () => ({ testMCPConnection: vi.fn() }))

const CONNECTORS_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../../../shared/mcp-connectors/public",
)

function loadShippedConnector(rel: string): MCPConnector {
  const dir = path.join(CONNECTORS_ROOT, rel)
  const cv = JSON.parse(fs.readFileSync(path.join(dir, "latest.json"), "utf8")).current_version
  const m = JSON.parse(fs.readFileSync(path.join(dir, "versions", cv, "metadata.json"), "utf8"))
  return {
    ...m,
    name: m.id,
    configuration_schema: m.config_schema,
    docker_status: "running",
  } as unknown as MCPConnector
}

const postgresql = loadShippedConnector("postgresql")

function gatewayError(status: number, body: Record<string, unknown>) {
  return new APIRequestError(`HTTP ${status}: ${JSON.stringify(body)}`)
}

// A complete form, so the click reaches onSave and the banner shows the
// server's answer, not the form's own required-field check.
function renderSavingInto(rejection: unknown) {
  const onSave = vi.fn(async () => {
    throw rejection
  })
  render(
    <GenericConnectorForm
      connector={postgresql}
      onSave={onSave}
      onCancel={vi.fn()}
      initialData={{ connectionName: "orders-db", config: { host: "h", port: 5432, database: "d" } }}
    />,
  )
  fireEvent.change(screen.getByTestId("credential-user"), { target: { value: "rsync" } })
  fireEvent.change(screen.getByTestId("credential-password"), { target: { value: "x" } })
  const save = screen.getByRole("button", { name: /Save Connection/i })
  expect(save).toBeEnabled()
  fireEvent.click(save)
  return onSave
}

afterEach(cleanup)

describe("GenericConnectorForm — a failed save", () => {
  it("is announced, takes focus, and shows the connector's own test error", async () => {
    const onSave = renderSavingInto(
      gatewayError(422, {
        error: "connection_test_failed",
        message:
          "Connectivity test failed before saving. Fix the credentials and try again, or set force_save=true to persist anyway.",
        test_error: 'password authentication failed for user "rsync"',
      }),
    )
    const banner = await screen.findByRole("alert")
    expect(onSave).toHaveBeenCalledTimes(1)
    expect(banner).toHaveAttribute("data-testid", "connector-form-error")
    expect(banner).toHaveTextContent(/^Connectivity test failed before saving/)
    expect(banner).toHaveTextContent('password authentication failed for user "rsync"')
    // No field to point at, so the reason is where focus goes.
    await waitFor(() => expect(banner).toHaveFocus())
  })

  it("shows a 5xx's own sentence", async () => {
    renderSavingInto(
      gatewayError(500, { error: "encrypt_credentials_failed", message: "Failed to encrypt credentials" }),
    )
    const banner = await screen.findByRole("alert")
    expect(banner).toHaveTextContent("Failed to encrypt credentials")
    expect(banner).not.toHaveTextContent("A server error occurred")
  })

  it("focuses the name input when the server says the name is taken", async () => {
    renderSavingInto(
      gatewayError(409, {
        error: 'A connection named "orders-db" already exists. Please choose a different name.',
        code: "DUPLICATE_NAME",
      }),
    )
    const banner = await screen.findByRole("alert")
    expect(banner).toHaveTextContent('A connection named "orders-db" already exists.')
    await waitFor(() => expect(document.getElementById("connection_name")).toHaveFocus())
    expect(banner).not.toHaveFocus()
  })

  it("does not blame the name for a conflict that is not about the name", async () => {
    renderSavingInto(
      gatewayError(409, {
        error: "Cannot complete operation: this connection is referenced by other resources.",
        code: "DATABASE_ERROR",
      }),
    )
    const banner = await screen.findByRole("alert")
    expect(banner).toHaveTextContent("this connection is referenced by other resources")
    expect(banner).not.toHaveTextContent(/already exists/)
    await waitFor(() => expect(banner).toHaveFocus())
    expect(document.getElementById("connection_name")).not.toHaveFocus()
  })

  it("an error that is not an APIRequestError is announced too", async () => {
    renderSavingInto(
      Object.assign(new Error("HTTP 503"), {
        error: "connector_deploying",
        message: "The connector is still being set up, so nothing was saved. Try again in a minute.",
        statusCode: 503,
      }),
    )
    const banner = await screen.findByRole("alert")
    expect(banner).toHaveTextContent("The connector is still being set up, so nothing was saved.")
    await waitFor(() => expect(banner).toHaveFocus())
  })
})
