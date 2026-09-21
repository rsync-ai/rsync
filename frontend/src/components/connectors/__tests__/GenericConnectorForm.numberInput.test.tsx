import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import type { Mock } from "vitest"
import type { MCPConnector } from "@/lib/types/mcp-connector"
import { GenericConnectorForm, parseNumberInput } from "../GenericConnectorForm"

// ---------------------------------------------------------------------------
// Issue #11: MongoDB connections showed "Port 0".
//
// A number box stored `parseInt(text) || 0`, so clearing the MongoDB port saved
// `port: 0` — a value no MongoDB listens on, which the connection page then
// printed. The connector itself never needed the key (connector.py _build_uri:
// `config.get("port") or 27017`), so "unset" must be stored as unset: the key is
// left out of the saved config. A typed 0 is still 0, because some fields use 0
// as a real value (gcs `max_file_rows: 0` = no cap).
//
// The connectors below are the REAL shipped metadata read from disk, shaped the
// way the gateway serves it (wire `configuration_schema` = disk `config_schema`).
// ---------------------------------------------------------------------------

const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => jsonRes({})),
}))
vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: () => <button type="button">Connect</button>,
}))
vi.mock("@/components/connectors/AuthMethodPicker", () => ({
  AuthMethodPicker: () => <div data-testid="auth-picker" />,
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

const mongodb = loadShippedConnector("database/mongodb")
const gcs = loadShippedConnector("storage/gcs")

type SavedPayload = { config: Record<string, unknown> }
type SaveMock = Mock<(payload: Record<string, unknown>) => void>
const saveMock = (): SaveMock => vi.fn<(payload: Record<string, unknown>) => void>()

function openAdvanced() {
  fireEvent.click(screen.getByRole("button", { name: /Advanced settings/i }))
}

async function saveAndCapture(onSave: SaveMock, label: RegExp) {
  fireEvent.click(screen.getByRole("button", { name: label }))
  await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1))
  return (onSave.mock.calls[0][0] as unknown as SavedPayload).config
}

// React logs "changing a controlled input to be uncontrolled" when an input's
// value goes from a number to undefined. A cleared number box must stay a
// controlled, empty input. React logs this once per test file, so every test
// checks it: the first test that clears a box is the one that fails.
let controlledWarnings: string[] = []
beforeEach(() => {
  vi.clearAllMocks()
  controlledWarnings = []
  const original = console.error
  vi.spyOn(console, "error").mockImplementation((...args: unknown[]) => {
    const message = args.map(String).join(" ")
    if (/controlled input to be uncontrolled/i.test(message)) controlledWarnings.push(message)
    else original(...args)
  })
})
afterEach(() => {
  vi.mocked(console.error).mockRestore()
  expect(controlledWarnings).toEqual([])
})

describe("fixtures are the real shipped schema", () => {
  it("mongodb declares an optional integer port with no default; gcs max_file_rows is an integer defaulting to 0", () => {
    const mProps = mongodb.configuration_schema?.properties ?? {}
    expect(Object.keys(mProps).length).toBeGreaterThan(0)
    expect(mProps.port?.type).toBe("integer")
    expect(mProps.port?.default).toBeUndefined()
    expect(mongodb.configuration_schema?.required ?? []).not.toContain("port")

    const gProps = gcs.configuration_schema?.properties ?? {}
    expect(gProps.max_file_rows?.type).toBe("integer")
    expect(gProps.max_file_rows?.default).toBe(0)
  })
})

describe("parseNumberInput", () => {
  it("empty or non-numeric text is unset, a typed 0 is 0", () => {
    expect(parseNumberInput("", "integer")).toBeUndefined()
    expect(parseNumberInput("   ", "integer")).toBeUndefined()
    expect(parseNumberInput("-", "integer")).toBeUndefined()
    expect(parseNumberInput("abc", "number")).toBeUndefined()
    // Too big to be a real number: unset, never Infinity (which JSON saves as null).
    expect(parseNumberInput("1e999", "integer")).toBeUndefined()
    expect(parseNumberInput("-1e999", "number")).toBeUndefined()
    expect(parseNumberInput("0", "integer")).toBe(0)
    expect(parseNumberInput("27017", "integer")).toBe(27017)
    expect(parseNumberInput("12.7", "integer")).toBe(12)
    expect(parseNumberInput("1.5", "number")).toBe(1.5)
  })
})

describe("GenericConnectorForm number inputs (MongoDB port)", () => {
  function renderMongo(onSave: SaveMock) {
    render(
      <GenericConnectorForm
        connector={mongodb}
        onSave={onSave}
        onCancel={vi.fn()}
        initialData={{ connectionName: "mongo conn" }}
      />,
    )
    fireEvent.change(screen.getByLabelText(/^Host/), { target: { value: "mongo.internal" } })
    fireEvent.change(screen.getByLabelText(/^Database/), { target: { value: "app" } })
    openAdvanced()
    return screen.getByLabelText(/^Port/) as HTMLInputElement
  }

  it("typing 27017 saves port 27017", async () => {
    const onSave = saveMock()
    const port = renderMongo(onSave)
    fireEvent.change(port, { target: { value: "27017" } })

    const config = await saveAndCapture(onSave, /Save Connection/i)
    expect(config.host).toBe("mongo.internal")
    expect(config.port).toBe(27017)
  })

  it("a decimal typed into the integer Port box saves a whole number", async () => {
    const onSave = saveMock()
    const port = renderMongo(onSave)
    fireEvent.change(port, { target: { value: "27017.9" } })

    const config = await saveAndCapture(onSave, /Save Connection/i)
    expect(config.port).toBe(27017)
  })

  it("clearing the port leaves the key out of the saved config (not port: 0)", async () => {
    const onSave = saveMock()
    const port = renderMongo(onSave)
    fireEvent.change(port, { target: { value: "27017" } })
    fireEvent.change(port, { target: { value: "" } })
    expect(port.value).toBe("")

    const config = await saveAndCapture(onSave, /Save Connection/i)
    // Control: the save really went through with the rest of the form.
    expect(config.host).toBe("mongo.internal")
    expect(config.database).toBe("app")
    expect("port" in config).toBe(false)
  })

  it("editing a stored port: 0 connection and clearing the box removes the port", async () => {
    const onSave = saveMock()
    render(
      <GenericConnectorForm
        connector={mongodb}
        onSave={onSave}
        onCancel={vi.fn()}
        isEditing
        connectionId="c1"
        initialData={{
          connectionName: "old mongo",
          connectionType: "source",
          config: { host: "mongo.internal", database: "app", port: 0 },
        }}
      />,
    )
    openAdvanced()
    const port = screen.getByLabelText(/^Port/) as HTMLInputElement
    fireEvent.change(port, { target: { value: "" } })

    const config = await saveAndCapture(onSave, /Update Connection/i)
    expect(config.host).toBe("mongo.internal")
    expect("port" in config).toBe(false)
  })
})

describe("GenericConnectorForm number inputs (a field where 0 is legal)", () => {
  it("gcs destination: typing 0 into max_file_rows saves 0, not unset", async () => {
    const onSave = saveMock()
    render(
      <GenericConnectorForm
        connector={gcs}
        onSave={onSave}
        onCancel={vi.fn()}
        initialData={{ connectionName: "gcs dest", connectionType: "destination" }}
      />,
    )
    fireEvent.change(screen.getByLabelText(/^Bucket/), { target: { value: "my-bucket" } })
    openAdvanced()
    const rows = screen.getByLabelText(/^Max File Rows/) as HTMLInputElement
    fireEvent.change(rows, { target: { value: "500" } })
    expect(rows.value).toBe("500")
    fireEvent.change(rows, { target: { value: "0" } })

    const config = await saveAndCapture(onSave, /Save Connection/i)
    expect(config.bucket).toBe("my-bucket")
    expect(config).toHaveProperty("max_file_rows", 0)
  })

  it("gcs destination: clearing max_file_rows leaves the key out, like the port", async () => {
    const onSave = saveMock()
    render(
      <GenericConnectorForm
        connector={gcs}
        onSave={onSave}
        onCancel={vi.fn()}
        initialData={{ connectionName: "gcs dest", connectionType: "destination" }}
      />,
    )
    fireEvent.change(screen.getByLabelText(/^Bucket/), { target: { value: "my-bucket" } })
    openAdvanced()
    const rows = screen.getByLabelText(/^Max File Rows/) as HTMLInputElement
    fireEvent.change(rows, { target: { value: "500" } })
    fireEvent.change(rows, { target: { value: "" } })
    expect(rows.value).toBe("")

    const config = await saveAndCapture(onSave, /Save Connection/i)
    // Control: the other schema defaults are still saved.
    expect(config.bucket).toBe("my-bucket")
    expect(config).toHaveProperty("sample_files", 5)
    expect("max_file_rows" in config).toBe(false)
  })
})
