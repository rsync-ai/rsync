import { describe, it, expect, vi, afterEach } from "vitest"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm } from "../GenericConnectorForm"

// ---------------------------------------------------------------------------
// Prod 2026-09-26, /connections/new for PostgreSQL: Test Connection and Save
// Connection were both greyed out while the password box was empty, and nothing
// said why. The gate (authIncomplete) was a bare boolean, so the names of the
// missing fields were thrown away. The form must now say what is still required,
// tie that sentence to the disabled buttons, and mark the credential fields
// required.
//
// Uses the REAL shipped postgresql metadata and the REAL AuthMethodPicker (the
// other full-form test files stub the picker out).
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

const testButton = () => screen.getByRole("button", { name: /Test Connection/i })
const saveButton = () => screen.getByRole("button", { name: /Save Connection/i })

afterEach(cleanup)

describe("fixture is the real shipped postgresql metadata", () => {
  it("basic auth over user + password, both required", () => {
    expect(postgresql.supported_auth_methods?.map((m) => m.method)).toEqual(["basic"])
    expect(postgresql.configuration_schema?.required).toEqual(
      expect.arrayContaining(["user", "password"]),
    )
  })
})

describe("GenericConnectorForm — why Test / Save are disabled", () => {
  it("names the missing credentials beside the disabled buttons", () => {
    render(
      <GenericConnectorForm
        connector={postgresql}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "pg" }}
      />,
    )

    expect(testButton()).toBeDisabled()
    expect(saveButton()).toBeDisabled()
    expect(screen.getByTestId("connector-form-blocked-reason")).toHaveTextContent(
      "Still required: User, Password.",
    )
    expect(testButton()).toHaveAccessibleDescription("Still required: User, Password.")
    expect(saveButton()).toHaveAccessibleDescription("Still required: User, Password.")

    fireEvent.change(screen.getByTestId("credential-user"), { target: { value: "rsync" } })
    expect(saveButton()).toHaveAccessibleDescription("Still required: Password.")

    fireEvent.change(screen.getByTestId("credential-password"), { target: { value: "x" } })
    expect(screen.queryByTestId("connector-form-blocked-reason")).not.toBeInTheDocument()
    expect(testButton()).toBeEnabled()
    expect(saveButton()).toBeEnabled()
    expect(saveButton()).not.toHaveAttribute("aria-describedby")
  })

  it("an empty connection name is named too, and blocks only Save", () => {
    render(
      <GenericConnectorForm
        connector={postgresql}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{}}
      />,
    )
    fireEvent.change(screen.getByTestId("credential-user"), { target: { value: "rsync" } })
    fireEvent.change(screen.getByTestId("credential-password"), { target: { value: "x" } })

    expect(saveButton()).toBeDisabled()
    expect(saveButton()).toHaveAccessibleDescription("Still required: Connection Name.")
    expect(testButton()).toBeEnabled()
    expect(testButton()).not.toHaveAttribute("aria-describedby")
  })

  it("marks the required credential fields, and flags one left empty", () => {
    render(
      <GenericConnectorForm
        connector={postgresql}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        initialData={{ connectionName: "pg" }}
      />,
    )
    const password = screen.getByTestId("credential-password")
    expect(password).toHaveAttribute("aria-required", "true")
    expect(password).not.toHaveAttribute("aria-invalid")

    fireEvent.blur(password)
    expect(password).toHaveAttribute("aria-invalid", "true")
    expect(password).toHaveAccessibleDescription("Password is required.")

    fireEvent.change(password, { target: { value: "x" } })
    expect(password).not.toHaveAttribute("aria-invalid")
  })

  // Control: edit mode keeps a blank credential (the stored one is used), so the
  // picker gate and the required markers stay off there.
  it("edit mode does not mark stored credentials required", () => {
    render(
      <GenericConnectorForm
        connector={postgresql}
        onSave={vi.fn()}
        onCancel={vi.fn()}
        isEditing
        initialData={{ connectionName: "pg", config: { host: "h", port: 5432, database: "d" } }}
      />,
    )
    const password = screen.getByTestId("credential-password")
    expect(password).not.toHaveAttribute("aria-required")
    fireEvent.blur(password)
    expect(password).not.toHaveAttribute("aria-invalid")
    expect(screen.queryByTestId("connector-form-blocked-reason")).not.toBeInTheDocument()
  })
})
