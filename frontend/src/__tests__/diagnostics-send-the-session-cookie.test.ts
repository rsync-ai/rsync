/**
 * Every API probe on the System Test Suite page must send the session cookie.
 *
 * The suite reported Passed: 2 / Failed: 4 on a working install:
 *
 *   Backend /health    ❌ Failed to fetch
 *   Connectors API     ❌ {"error":"No authorization token"}
 *   Connections API    ❌ {"error":"No authorization token"}
 *   Pipelines API      ❌ {"error":"No authorization token"}
 *
 * which reads as "the backend is down" and is the first thing an evaluator runs. The
 * backend was fine. The session lives in a cookie on the API's origin, and on every real
 * install the API is cross-origin from the frontend (frontend :3000, gateway :5001), where
 * fetch's default credentials mode of "same-origin" omits it. The page's own admin gate
 * reaches the same API with the same cookie through authFetch and succeeds — same page,
 * same browser, same moment — which is what localises the defect to these probes.
 *
 * The guard is written against the CALL, not against one probe's outcome, because the
 * defect was structural: a new probe added tomorrow that forgets the option fails here.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"

import { DIAGNOSTIC_TESTS } from "@/lib/diagnostics/tests"

const CTX = {
  apiUrl: "http://35.192.196.2:5001",
  wsUrl: "ws://35.192.196.2:5001/ws",
  authHeaders: {} as Record<string, string>,
}

type Call = { url: string; init: RequestInit | undefined }

let calls: Call[]

beforeEach(() => {
  calls = []
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push({ url: String(url), init })
      return new Response(JSON.stringify({ connectors: [], connections: [], pipelines: [] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      })
    }),
  )
})

afterEach(() => {
  vi.unstubAllGlobals()
})

/** Every probe that talks HTTP. The websocket probe uses WebSocket, not fetch. */
const HTTP_PROBES = DIAGNOSTIC_TESTS.filter((t) => t.id !== "env" && t.id !== "websocket")

describe("diagnostics HTTP probes", () => {
  it("covers every HTTP probe on the page, so this suite cannot pass vacuously", () => {
    // The positive denominator. If a rename or refactor made the filter match nothing,
    // the per-probe assertions below would all pass without checking anything.
    expect(HTTP_PROBES.map((t) => t.id).sort()).toEqual([
      "backend-health",
      "connections-api",
      "connectors-api",
      "pipelines-api",
    ])
  })

  for (const probe of HTTP_PROBES) {
    it(`${probe.id} sends the session cookie cross-origin`, async () => {
      await probe.run(CTX)

      expect(calls).toHaveLength(1)
      expect(calls[0]!.init?.credentials).toBe("include")
      // The probe's own headers must survive alongside it — the caller's init is spread
      // AFTER the default, so a botched merge would drop them rather than the credentials.
      expect(calls[0]!.init?.headers).toBeDefined()
    })
  }
})
