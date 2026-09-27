import { describe, expect, it } from "vitest"

import { APIRequestError, ErrorCodes, parseAPIError } from "../api-errors"

// tracedJsonFetch throws `new APIRequestError("HTTP <status>: <body>")`, so each
// case below is the exact string the connection wizard's save path receives.
// The bodies are the gateway's own: respondError (`{error: <code>, message}`),
// SendDBError (`{error: <sentence>, code, details}`), and the 422/503 pre-save
// test refusals in connections.go.
function fromGateway(status: number, body: Record<string, unknown>) {
  return new APIRequestError(`HTTP ${status}: ${JSON.stringify(body)}`)
}

describe("the connection wizard shows the server's own reason", () => {
  // KI-CONNECTION-WIZARD-DISCARDS-SERVER-ERROR: every 5xx read "A server error
  // occurred", whatever the server said.
  it("keeps a 5xx's sentence instead of 'A server error occurred'", () => {
    const err = fromGateway(500, { error: "encrypt_credentials_failed", message: "Failed to encrypt credentials" })
    expect(err.message).toBe("Failed to encrypt credentials")
    expect(err.message).not.toMatch(/A server error occurred/)
    expect(err.code).toBe("encrypt_credentials_failed")
    // The server named the problem; a canned "try again in a moment" would
    // contradict it.
    expect(err.suggestion).toBeUndefined()
  })

  it("keeps a sentence sent in `error` with no code", () => {
    const err = fromGateway(500, { error: "Failed to save connection. Please try again or contact support." })
    expect(err.message).toBe("Failed to save connection. Please try again or contact support.")
  })

  it("reads `message` when `error` is only a code, and carries the connector's test error", () => {
    const err = fromGateway(422, {
      error: "connection_test_failed",
      message: "Connectivity test failed before saving. Fix the credentials and try again, or set force_save=true to persist anyway.",
      test_error: "storage.buckets.get denied on bucket landing-zone",
      trace_id: "t-1",
    })
    expect(err.message).toMatch(/^Connectivity test failed before saving/)
    expect(err.cause).toBe("storage.buckets.get denied on bucket landing-zone")
    expect(err.field).toBeUndefined()
  })

  it("says a connector that is still deploying is still deploying", () => {
    const err = fromGateway(503, {
      error: "connector_deploying",
      status: "connector_deploying",
      retryable: true,
      message: "The connector is still being set up, so nothing was saved. Try again in a minute.",
    })
    expect(err.message).toBe("The connector is still being set up, so nothing was saved. Try again in a minute.")
  })

  it("still falls back to a readable sentence when the server sent none", () => {
    expect(fromGateway(500, { error: "internal" }).message).toBe("A server error occurred")
    // A proxy's HTML page is markup, not a reason.
    const html = new APIRequestError("HTTP 502: <html><body><h1>502 Bad Gateway</h1></body></html>")
    expect(html.message).not.toMatch(/</)
  })
})

describe("a conflict is blamed on the connection name only when the server says so", () => {
  // KI-CONFLICT-ALWAYS-BLAMED-ON-CONNECTION-NAME: any 409 became "A connection
  // named X already exists" with field "name", which the form focuses.
  it("points at the name for SendDBError's taken-name conflict", () => {
    const err = fromGateway(409, {
      error: 'A connection named "orders-db" already exists. Please choose a different name.',
      code: "DUPLICATE_NAME",
      details: 'pq: duplicate key value violates unique constraint "uq_connections_ws_name"',
    })
    expect(err.code).toBe(ErrorCodes.DUPLICATE_NAME)
    expect(err.field).toBe("name")
    expect(err.message).toBe('A connection named "orders-db" already exists. Please choose a different name.')
  })

  it("keeps an apostrophe in the name (the old regex cut it there)", () => {
    const err = fromGateway(409, {
      error: `A connection named "Rahul's db" already exists. Please choose a different name.`,
      code: "DUPLICATE_NAME",
    })
    expect(err.message).toContain(`"Rahul's db"`)
  })

  it("does not blame the name for a duplicate that is not the name", () => {
    const err = fromGateway(409, {
      error: "A connection with these details already exists.",
      code: "DUPLICATE_NAME",
    })
    expect(err.field).toBeUndefined()
    expect(err.message).toBe("A connection with these details already exists.")
  })

  it("does not blame the name for a foreign-key 409", () => {
    const err = fromGateway(409, {
      error: "Cannot complete operation: this connection is referenced by other resources.",
      code: "DATABASE_ERROR",
    })
    expect(err.field).toBeUndefined()
    expect(err.message).not.toMatch(/already exists/)
  })

  it("does not blame the name for a bare 409 with no code", () => {
    const err = fromGateway(409, { error: "stale_write", message: "This query was changed by someone else." })
    expect(err.field).toBeUndefined()
    expect(err.message).toBe("This query was changed by someone else.")
  })
})

describe("an error the edit page throws keeps its status and sentence", () => {
  // connections/[id]/page.tsx throws the body with the status attached.
  it("reads a 500 sentence from a thrown object instead of guessing 'Connection test failed'", () => {
    const thrown = Object.assign(new Error("HTTP 500"), { error: "Failed to update connection", statusCode: 500 })
    const parsed = parseAPIError(thrown)
    expect(parsed.message).toBe("Failed to update connection")
    expect(parsed.message).not.toMatch(/Connection test failed/)
  })

  it("blames the name on a thrown taken-name 409", () => {
    const thrown = Object.assign(new Error("HTTP 409"), {
      error: 'A connection named "orders-db" already exists. Please choose a different name.',
      code: "DUPLICATE_NAME",
      statusCode: 409,
    })
    expect(parseAPIError(thrown).field).toBe("name")
  })
})
