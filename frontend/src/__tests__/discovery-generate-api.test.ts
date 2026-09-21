import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import { DiscoveryError, generateFromSession } from "@/lib/api/discovery"

// Generating a connector the deterministic paths can't build needs an LLM. On an
// install without one, api-gateway (connector_generator.go) answers 503 with
// error="llm_not_configured" and the sentence in error_message. The wizard shows
// the thrown message as is, so it must be the sentence, never the code.
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock

afterEach(() => vi.clearAllMocks())

function failure(body: unknown, status: number) {
  return { ok: false, status, statusText: "Service Unavailable", json: async () => body }
}

async function thrown(): Promise<DiscoveryError> {
  try {
    await generateFromSession("acme", "session-1")
  } catch (e) {
    return e as DiscoveryError
  }
  throw new Error("generateFromSession did not throw")
}

const SENTENCE = "Set up an LLM first: add OPENAI_API_KEY to .env, then restart rsync."

describe("generateFromSession errors", () => {
  it("shows the sentence, not the code, when no LLM is set up", async () => {
    mockFetch.mockResolvedValue(
      failure(
        { success: false, error: "llm_not_configured", error_message: SENTENCE, error_stage: "llm_not_configured" },
        503,
      ),
    )
    const e = await thrown()
    expect(e).toBeInstanceOf(DiscoveryError)
    expect(e.status).toBe(503)
    expect(e.message).toBe(SENTENCE)
  })

  it("still falls back to error, then detail, when there is no error_message", async () => {
    mockFetch.mockResolvedValue(failure({ error: "generator exploded" }, 500))
    expect((await thrown()).message).toBe("generator exploded")

    mockFetch.mockResolvedValue(failure({ detail: "bad request" }, 400))
    expect((await thrown()).message).toBe("bad request")
  })

  it("uses the status text when the body is not JSON", async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 502,
      statusText: "Bad Gateway",
      json: async () => {
        throw new SyntaxError("not json")
      },
    })
    expect((await thrown()).message).toBe("Bad Gateway")
  })
})
