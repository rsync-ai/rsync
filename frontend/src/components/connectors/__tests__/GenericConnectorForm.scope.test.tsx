import { describe, it, expect, vi, beforeEach, afterEach, type Mock } from "vitest"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm } from "../GenericConnectorForm"
import { primeNamespaceModels } from "@/lib/pipeline/namespaceModel"
import { repoNamespaceModels } from "@/lib/pipeline/__tests__/repoNamespaceModels"

// The Scope step inside the real connection form: shown for a source whose
// connector lists databases or schemas, previewed through the gateway, and
// refused at save when the server would refuse it.

const jsonRes = (data: unknown, status = 200) => ({ ok: status < 400, status, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("@/components/oauth/OAuthConnectButton", () => ({ OAuthConnectButton: () => null }))
vi.mock("@/components/connectors/AuthMethodPicker", () => ({ AuthMethodPicker: () => null }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("@/lib/api/mcp-connectors", () => ({ testMCPConnection: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"

const mysql = {
  name: "mysql",
  display_name: "MySQL",
  auth_type: "none",
  supports_source: true,
  supports_destination: true,
  docker_status: "running",
  configuration_schema: {
    properties: {
      host: { type: "string" },
      user: { type: "string" },
      database: { type: "string" },
    },
    required: ["host", "user"],
  },
} as unknown as MCPConnector

const gcs = { ...mysql, name: "gcs", display_name: "GCS" } as unknown as MCPConnector

function renderForm(
  connector: MCPConnector,
  config: Record<string, unknown>,
  connectionType: "source" | "destination" = "source",
) {
  return render(
    <GenericConnectorForm
      connector={connector}
      onSave={vi.fn()}
      onCancel={vi.fn()}
      initialData={{ connectionName: "c", connectionType, config }}
    />,
  )
}

beforeEach(() => {
  primeNamespaceModels(repoNamespaceModels())
  ;(authFetch as Mock).mockReset()
  ;(authFetch as Mock).mockImplementation(async (url: string) =>
    String(url).endsWith("/api/v1/connections/namespaces")
      ? jsonRes({ connector_type: "mysql", lists_namespaces: true, namespaces: ["crm", "shop", "tmp_1"], current: "" })
      : jsonRes({}),
  )
})

afterEach(() => primeNamespaceModels(undefined))

describe("GenericConnectorForm Scope step", () => {
  it("a server-level source previews its databases from the unsaved config", async () => {
    renderForm(mysql, { host: "db", user: "u" })

    fireEvent.click(screen.getByRole("radio", { name: "All except" }))
    fireEvent.change(screen.getByLabelText("Databases to leave out"), { target: { value: "tmp_*" } })
    fireEvent.click(screen.getByRole("button", { name: /Preview databases/ }))

    await waitFor(() => expect(screen.getByText("Reads 2 of 3 databases")).toBeInTheDocument())
    const call = (authFetch as Mock).mock.calls.find(([u]) => String(u).endsWith("/api/v1/connections/namespaces"))
    expect(call?.[1]?.method).toBe("POST")
    const body = JSON.parse(call?.[1]?.body)
    expect(body.connector_type).toBe("mysql")
    expect(body.config).toMatchObject({ host: "db", user: "u" })
    expect(body.config.namespace_filter_mode).toBe("exclude")
  })

  it("a saved connection lists with its stored credentials", async () => {
    render(
      <GenericConnectorForm
        connector={mysql}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "c", connectionType: "source", config: { host: "db", user: "u" } }}
        isEditing
        connectionId="11111111-2222-3333-4444-555555555555"
      />,
    )
    ;(authFetch as Mock).mockImplementation(async () => jsonRes({ namespaces: ["crm"] }))
    fireEvent.click(screen.getByRole("button", { name: /Preview databases/ }))
    await waitFor(() => expect(screen.getByText("Reads 1 of 1 databases")).toBeInTheDocument())
    const [url, init] = (authFetch as Mock).mock.calls.at(-1)!
    expect(String(url)).toMatch(/\/api\/v1\/connections\/11111111-2222-3333-4444-555555555555\/namespaces$/)
    expect(init).toBeUndefined()
  })

  it("an Only-these Scope with no pattern is not saved", async () => {
    renderForm(mysql, { host: "db", user: "u" })
    fireEvent.click(screen.getByRole("radio", { name: "Only these" }))
    ;(authFetch as Mock).mockClear()

    fireEvent.click(screen.getByRole("button", { name: "Save Connection" }))

    await waitFor(() => expect(screen.getByText(/^Scope: /)).toBeInTheDocument())
    expect(authFetch).not.toHaveBeenCalled()
  })

  it("a connection naming a database is pinned to it", () => {
    renderForm(mysql, { host: "db", user: "u", database: "shop" })
    expect(screen.getByTestId("connection-scope-pinned")).toHaveTextContent("shop")
  })

  it("is absent for a destination and for a connector with nothing to list", () => {
    const { unmount } = renderForm(mysql, { host: "db", user: "u" }, "destination")
    expect(screen.queryByTestId("connection-scope")).toBeNull()
    unmount()
    renderForm(gcs, { bucket: "b" })
    expect(screen.queryByTestId("connection-scope")).toBeNull()
  })
})
