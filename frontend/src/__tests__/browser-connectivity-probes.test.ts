/**
 * The two browser-side probes behind admin/health's "From this browser" card — what is left
 * of the System Test Suite page.
 *
 * That page reported Passed: 2 / Failed: 4 on a working install, because its probes called
 * fetch without credentials and the default "same-origin" mode dropped the session cookie
 * cross-origin (frontend :3000, gateway :5001). The guard is still written against the CALL,
 * not one probe's outcome: a pass here has to mean the app's own authFetch requests, which
 * send credentials, get through the gateway's CORS too.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"

import { BROWSER_PROBES } from "@/lib/diagnostics/browser-connectivity"

const CTX = {
  apiUrl: "http://35.192.196.2:5001",
  // Kept on the secure scheme because the plaintext one trips the insecure-websocket
  // scanner, and an inert fixture is not worth a suppression.
  wsUrl: "wss://35.192.196.2:5001/ws",
}

type Call = { url: string; init: RequestInit | undefined }

let calls: Call[]
let nextResponse: () => Response

function probe(id: string) {
  const p = BROWSER_PROBES.find((t) => t.id === id)
  if (!p) throw new Error(`no probe ${id}`)
  return p
}

beforeEach(() => {
  calls = []
  nextResponse = () => new Response(JSON.stringify({ status: "ok" }), { status: 200 })
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push({ url: String(url), init })
      return nextResponse()
    }),
  )
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe("browser connectivity probes", () => {
  it("are exactly the API and WebSocket checks, so the suite cannot pass vacuously", () => {
    expect(BROWSER_PROBES.map((t) => t.id)).toEqual(["api-health", "websocket"])
  })

  it("asks the gateway's /api/health, not the frontend's /health, with the session cookie", async () => {
    await expect(probe("api-health").run(CTX)).resolves.toBe("Reachable")

    expect(calls).toHaveLength(1)
    expect(calls[0]!.url).toBe("http://35.192.196.2:5001/api/health")
    expect(calls[0]!.init?.credentials).toBe("include")
  })

  it("reports an HTML error page as its status and URL, not as a wall of markup", async () => {
    nextResponse = () =>
      new Response("<!DOCTYPE html><html><body>404: This page could not be found.</body></html>", {
        status: 404,
        statusText: "Not Found",
      })

    await expect(probe("api-health").run(CTX)).rejects.toThrow(
      "HTTP 404 Not Found from http://35.192.196.2:5001/api/health",
    )
  })

  it("passes when the socket opens and fails when it errors", async () => {
    const sockets: FakeSocket[] = []
    class FakeSocket {
      onopen: (() => void) | null = null
      onerror: (() => void) | null = null
      closed = false
      constructor(public url: string) {
        sockets.push(this)
      }
      close() {
        this.closed = true
      }
    }
    vi.stubGlobal("WebSocket", FakeSocket)

    const opened = probe("websocket").run(CTX)
    sockets[0]!.onopen?.()
    await expect(opened).resolves.toBe("Connected")
    expect(sockets[0]!.url).toBe(CTX.wsUrl)
    expect(sockets[0]!.closed).toBe(true)

    const refused = probe("websocket").run(CTX)
    sockets[1]!.onerror?.()
    await expect(refused).rejects.toThrow(/Connection failed/)
  })
})
