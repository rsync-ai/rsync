import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { fireEvent, render, screen, waitFor } from "@testing-library/react"

// The community image strips the discovery service, and the generate page
// used to offer only the discovery wizard -- so a self-hoster had no way to
// generate a connector at all. The page now probes the service and, on a 404
// only, offers SpecUploadFlow: an OpenAPI document sent inline to the same
// generate endpoint.

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(""),
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
}))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({ role: "admin", isLoading: false }),
}))
vi.mock("@/components/connectors/discovery/DiscoveryFlow", () => ({
  DiscoveryFlow: () => <div data-testid="discovery-wizard" />,
}))

import GenerateConnectorPage from "@/app/(dashboard)/connectors/generate/page"
import { SpecUploadFlow } from "@/components/connectors/discovery/SpecUploadFlow"

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  })

const PETSTORE = JSON.stringify({
  openapi: "3.0.0",
  info: { title: "Pet Store API", version: "1.0" },
  paths: { "/pets": { get: { operationId: "listPets" } } },
})

const PETSTORE_YAML = `openapi: 3.0.0
info:
  version: "1.0"
  title: 'Swagger Petstore'
paths: {}
`

const generateCalls = () =>
  authFetch.mock.calls.filter(([url]) => String(url).endsWith("/api/v1/connectors/generate"))

const generateBody = (i = 0) => JSON.parse(String(generateCalls()[i][1].body))

async function uploadPetstore() {
  const input = screen.getByTestId("spec-file-input") as HTMLInputElement
  const file = new File([PETSTORE], "petstore.json", { type: "application/json" })
  fireEvent.change(input, { target: { files: [file] } })
  await screen.findByTestId("spec-loaded")
}

beforeEach(() => {
  authFetch.mockReset()
})
afterEach(() => {
  vi.unstubAllGlobals()
})

describe("generate page picks the flow by probing the discovery service", () => {
  it("probes the stripped discovery module's vendor list", async () => {
    authFetch.mockResolvedValue(json(200, { vendors: [] }))
    render(<GenerateConnectorPage />)
    await screen.findByTestId("discovery-wizard")
    const probed = authFetch.mock.calls.map(([url]) => String(url))
    expect(probed.some((u) => u.endsWith("/v1/vendors?include_learned=false"))).toBe(true)
  })

  it("offers the upload screen when the service answers 404", async () => {
    authFetch.mockResolvedValue(json(404, { detail: "Not Found" }))
    render(<GenerateConnectorPage />)
    expect(await screen.findByTestId("spec-upload-flow")).toBeTruthy()
    expect(screen.queryByTestId("discovery-wizard")).toBeNull()
    expect(screen.getByText(/OpenAPI or Swagger document to a running MCP connector/)).toBeTruthy()
  })

  it.each([
    ["a 502", () => Promise.resolve(json(502, { error: "bad gateway" }))],
    ["a 403", () => Promise.resolve(json(403, { error: "forbidden" }))],
    ["a network error", () => Promise.reject(new TypeError("Failed to fetch"))],
  ])("keeps the wizard on %s -- only a 404 means the service is absent", async (_, answer) => {
    authFetch.mockImplementation(answer)
    render(<GenerateConnectorPage />)
    expect(await screen.findByTestId("discovery-wizard")).toBeTruthy()
    expect(screen.queryByTestId("spec-upload-flow")).toBeNull()
  })
})

describe("SpecUploadFlow", () => {
  it("sends an uploaded document inline under a kebab-case name taken from its title", async () => {
    authFetch.mockResolvedValue(
      json(200, { success: true, status: "completed", connector_name: "pet-store-api", operation_count: 1, version: "v1.0.0" }),
    )
    render(<SpecUploadFlow />)
    await uploadPetstore()
    expect((screen.getByLabelText("Connector name") as HTMLInputElement).value).toBe("pet-store-api")

    fireEvent.click(screen.getByTestId("spec-generate"))
    await screen.findByTestId("spec-result-success")

    const body = generateBody()
    expect(body).toMatchObject({ api_name: "pet-store-api", openapi_spec: PETSTORE, force_regenerate: false })
    expect(body).not.toHaveProperty("openapi_spec_url")
    expect(screen.getByTestId("spec-result-success").textContent).toContain("pet-store-api")
    expect(screen.getByTestId("spec-result-success").textContent).toContain("operations: 1")
  })

  it("fetches a URL in the browser without credentials and sends the text, never the URL", async () => {
    const browserFetch = vi.fn().mockResolvedValue(new Response(PETSTORE_YAML, { status: 200 }))
    vi.stubGlobal("fetch", browserFetch)
    authFetch.mockResolvedValue(json(200, { success: true, status: "completed", connector_name: "swagger-petstore" }))
    render(<SpecUploadFlow />)

    fireEvent.click(screen.getByTestId("spec-source-url"))
    fireEvent.change(screen.getByTestId("spec-url-input"), {
      target: { value: "https://petstore.example.com/openapi.yaml" },
    })
    fireEvent.click(screen.getByTestId("spec-url-fetch"))
    await screen.findByTestId("spec-loaded")

    expect(browserFetch).toHaveBeenCalledWith("https://petstore.example.com/openapi.yaml", { credentials: "omit" })
    // The third-party host never sees the session: authFetch is not used for it.
    expect(authFetch.mock.calls.some(([url]) => String(url).includes("petstore.example.com"))).toBe(false)
    expect((screen.getByLabelText("Connector name") as HTMLInputElement).value).toBe("swagger-petstore")

    fireEvent.click(screen.getByTestId("spec-generate"))
    await screen.findByTestId("spec-result-success")
    const body = generateBody()
    expect(body.openapi_spec).toBe(PETSTORE_YAML)
    expect(body).not.toHaveProperty("openapi_spec_url")
  })

  it("tells the user to upload the file when the browser cannot fetch the URL", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("Failed to fetch")))
    render(<SpecUploadFlow />)
    fireEvent.click(screen.getByTestId("spec-source-url"))
    fireEvent.change(screen.getByTestId("spec-url-input"), { target: { value: "https://no-cors.example.com/api.json" } })
    fireEvent.click(screen.getByTestId("spec-url-fetch"))
    const err = await screen.findByTestId("spec-result-error")
    expect(err.textContent).toContain("Download the file and upload it instead.")
    expect(authFetch).not.toHaveBeenCalled()
  })

  it("shows a refusal's message and suggestions", async () => {
    authFetch.mockResolvedValue(
      json(400, {
        success: false,
        status: "refused",
        error: "This build generates connectors from an OpenAPI document only",
        error_message: "This build generates connectors from an OpenAPI document only",
        suggestions: ["Upload the API's openapi.json"],
      }),
    )
    render(<SpecUploadFlow />)
    await uploadPetstore()
    fireEvent.click(screen.getByTestId("spec-generate"))
    const err = await screen.findByTestId("spec-result-error")
    expect(err.textContent).toContain("OpenAPI document only")
    expect(err.textContent).toContain("Upload the API's openapi.json")
  })

  it("treats a 200 with success:false as a failure", async () => {
    authFetch.mockResolvedValue(
      json(200, { success: false, status: "failed", error_message: "could not save artifacts", error_stage: "artifact_persistence" }),
    )
    render(<SpecUploadFlow />)
    await uploadPetstore()
    fireEvent.click(screen.getByTestId("spec-generate"))
    expect((await screen.findByTestId("spec-result-error")).textContent).toContain("could not save artifacts")
    expect(screen.queryByTestId("spec-result-success")).toBeNull()
  })

  it("explains already_exists, and Replace sends force_regenerate", async () => {
    authFetch.mockResolvedValueOnce(
      json(200, { success: true, status: "already_exists", connector_name: "pet-store-api", metadata: { already_exists: true } }),
    )
    authFetch.mockResolvedValueOnce(json(200, { success: true, status: "completed", connector_name: "pet-store-api" }))
    render(<SpecUploadFlow />)
    await uploadPetstore()

    fireEvent.click(screen.getByTestId("spec-generate"))
    await screen.findByTestId("spec-already-exists")
    expect(screen.queryByTestId("spec-result-success")).toBeNull()

    fireEvent.click(screen.getByTestId("spec-replace"))
    fireEvent.click(screen.getByTestId("spec-generate"))
    await screen.findByTestId("spec-result-success")
    await waitFor(() => expect(generateCalls()).toHaveLength(2))
    expect(generateBody(1).force_regenerate).toBe(true)
  })
})
