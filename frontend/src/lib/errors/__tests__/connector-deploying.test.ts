import { describe, it, expect } from "vitest"
import {
  CONNECTOR_DEPLOYING_MARKER,
  connectorDeployingErrorMessage,
  connectorDeployingMessage,
  connectorDeployingSaveNotice,
  isConnectorDeployingResponse,
} from "../connector-deploying"

// Verbatim backend text (backend-orchestrator mcp.ConnectorDeployingMessage("MongoDB")).
const BACKEND_MSG =
  "The MongoDB connector is still being set up for first use — this can take a minute or two. Please try again shortly."

describe("connector-deploying", () => {
  it("builds the same message as the backend", () => {
    expect(connectorDeployingMessage("MongoDB")).toBe(BACKEND_MSG)
    expect(connectorDeployingMessage("")).toContain(CONNECTOR_DEPLOYING_MARKER)
  })

  it("recognizes the gateway's retryable response", () => {
    expect(
      isConnectorDeployingResponse({ success: false, status: "connector_deploying", retryable: true, error: BACKEND_MSG }),
    ).toBe(true)
    // Marker alone (e.g. a gateway that did not add status/retryable).
    expect(isConnectorDeployingResponse({ success: false, status: "failed", error: BACKEND_MSG })).toBe(true)
    // A raw import error from an older backend is the same condition.
    expect(isConnectorDeployingResponse({ success: false, error: "No module named 'pymongo'" })).toBe(true)
  })

  it("does not classify real failures or successes as deploying", () => {
    expect(isConnectorDeployingResponse({ success: false, status: "failed", error: "Authentication failed." })).toBe(false)
    expect(isConnectorDeployingResponse({ success: true, status: "success", message: "Connection test successful" })).toBe(false)
    expect(isConnectorDeployingResponse(null)).toBe(false)
  })

  it("maps raw import errors to the friendly message and leaves other errors alone", () => {
    const raw =
      "No module named 'pymongo' [mcp stdio fallback: connector mongodb@v1.0.0 ran as a subprocess inside the orchestrator because no Docker container was reachable]"
    const mapped = connectorDeployingErrorMessage(raw, "MongoDB")
    expect(mapped).toBe(BACKEND_MSG)
    expect(mapped).not.toContain("No module named")

    expect(connectorDeployingErrorMessage(BACKEND_MSG, "Other")).toBe(BACKEND_MSG)
    expect(connectorDeployingErrorMessage("Connection refused", "MongoDB")).toBeNull()
    expect(connectorDeployingErrorMessage("", "MongoDB")).toBeNull()
  })

  it("maps the gateway's retryable save refusal (edit page Save) to a wait-and-retry notice", () => {
    const body = {
      error: "connector_deploying",
      status: "connector_deploying",
      retryable: true,
      message: "The connector is still being set up, so nothing was saved. Try again in a minute.",
      test_error: BACKEND_MSG,
    }
    expect(connectorDeployingSaveNotice(503, body)).toEqual({
      title: "Connector is still being set up — changes not saved",
      description: BACKEND_MSG,
    })
    // No test_error: fall back to the generic deploying message.
    expect(connectorDeployingSaveNotice(503, { error: "connector_deploying" })?.description).toContain(
      CONNECTOR_DEPLOYING_MARKER,
    )
  })

  it("leaves other save refusals to their normal handling", () => {
    expect(
      connectorDeployingSaveNotice(422, { error: "connection_test_failed", test_error: "Authentication failed." }),
    ).toBeNull()
    expect(connectorDeployingSaveNotice(503, { error: "connection_test_interrupted" })).toBeNull()
    expect(connectorDeployingSaveNotice(503, null)).toBeNull()
  })
})
