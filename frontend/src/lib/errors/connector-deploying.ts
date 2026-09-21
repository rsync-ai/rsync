/**
 * "Still being set up" connection-test results.
 *
 * A never-deployed connector's container is built/started on demand, so the first
 * Test Connection on a fresh install can arrive before it is ready. The backend
 * answers with a retryable result (status "connector_deploying", retryable: true)
 * instead of a failure. The UI must show it as "wait and try again", never as a
 * credential/connectivity error — and must never show a raw Python import error
 * ("No module named 'pymongo'"), which only ever means the connector's runtime was
 * not ready. See KI-FIRST-CONNECTION-TEST-FALLS-BACK-TO-AN-UNUSABLE-STDIO-INTERPRETER.
 *
 * Lockstep: CONNECTOR_DEPLOYING_MARKER matches backend-orchestrator
 * internal/mcp.ConnectorDeployingMarker and api-gateway connectorDeployingMarker.
 */

export const CONNECTOR_DEPLOYING_MARKER = "connector is still being set up"

export function connectorDeployingMessage(displayName?: string): string {
  const name = (displayName || "").trim() || "This"
  return `The ${name} ${CONNECTOR_DEPLOYING_MARKER} for first use — this can take a minute or two. Please try again shortly.`
}

const MISSING_MODULE_RE = /ModuleNotFoundError|No module named\s/i

export interface TestConnectionResponseLike {
  success?: boolean
  status?: string
  retryable?: boolean
  error?: string
  message?: string
}

/** True when a Test Connection response means "connector not ready yet, retry". */
export function isConnectorDeployingResponse(data: TestConnectionResponseLike | null | undefined): boolean {
  if (!data || data.success) return false
  if (data.status === "connector_deploying" || data.retryable === true) return true
  const text = `${data.error ?? ""} ${data.message ?? ""}`
  return text.includes(CONNECTOR_DEPLOYING_MARKER) || MISSING_MODULE_RE.test(text)
}

/**
 * The notice for a create/update save refused because the pre-save connectivity test
 * reached a connector that is still being set up (api-gateway answers 503 with
 * status "connector_deploying", retryable: true). Nothing was saved; the user should
 * wait and save again. Returns null for any other response.
 */
export function connectorDeployingSaveNotice(
  httpStatus: number,
  body: (TestConnectionResponseLike & { test_error?: string }) | null | undefined,
  displayName?: string,
): { title: string; description: string } | null {
  if (httpStatus !== 503 || !body) return null
  if (body.status !== "connector_deploying" && body.error !== "connector_deploying") return null
  return {
    title: "Connector is still being set up — changes not saved",
    description: connectorDeployingErrorMessage(body.test_error, displayName) ?? connectorDeployingMessage(displayName),
  }
}

/**
 * Maps a failed test's error text to the retryable "still being set up" message when
 * it is one (the backend's own message, or a raw import error from an older backend).
 * Returns null for any other error so callers keep their normal handling.
 */
export function connectorDeployingErrorMessage(raw: string | undefined | null, displayName?: string): string | null {
  if (!raw) return null
  if (raw.includes(CONNECTOR_DEPLOYING_MARKER)) return raw
  if (MISSING_MODULE_RE.test(raw)) return connectorDeployingMessage(displayName)
  return null
}
