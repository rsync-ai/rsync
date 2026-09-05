import { afterEach, describe, expect, it } from "vitest"
import { isLeakedLocalhostUrl } from "@/lib/config/api"

// Regression guard for the localhost-leak hardening (see @/lib/config/api).
// isLeakedLocalhostUrl() is the primitive used to HIDE links to separate hosts
// (SigNoz, a connector's Docker health port, Superset) that can't be same-origin
// rebased, so a mis-baked localhost value never becomes a dead link in prod.

const realLocation = window.location

function setPageOrigin(origin: string) {
  // Swap window.location for a URL object exposing .hostname/.origin.
  Object.defineProperty(window, "location", { configurable: true, value: new URL(origin) })
}

afterEach(() => {
  Object.defineProperty(window, "location", { configurable: true, value: realLocation })
})

describe("isLeakedLocalhostUrl", () => {
  it("flags a localhost URL when the page is on a real (non-localhost) origin", () => {
    setPageOrigin("https://app.rsync.ai")
    expect(isLeakedLocalhostUrl("http://localhost:8081")).toBe(true)
    expect(isLeakedLocalhostUrl("http://127.0.0.1:3301")).toBe(true)
    expect(isLeakedLocalhostUrl("http://localhost:8123/health")).toBe(true)
    expect(isLeakedLocalhostUrl("http://[::1]:9000")).toBe(true)
  })

  it("does NOT flag a real-domain URL on a real origin", () => {
    setPageOrigin("https://app.rsync.ai")
    expect(isLeakedLocalhostUrl("https://observability.example.com")).toBe(false)
    expect(isLeakedLocalhostUrl("https://app.rsync.ai/orchestrator")).toBe(false)
  })

  it("does NOT flag anything when the page itself is on localhost (dev / self-host)", () => {
    setPageOrigin("http://localhost:8080")
    expect(isLeakedLocalhostUrl("http://localhost:3301")).toBe(false)
    expect(isLeakedLocalhostUrl("http://127.0.0.1:8123/health")).toBe(false)
  })

  it("handles empty / malformed input safely", () => {
    setPageOrigin("https://app.rsync.ai")
    expect(isLeakedLocalhostUrl(undefined)).toBe(false)
    expect(isLeakedLocalhostUrl(null)).toBe(false)
    expect(isLeakedLocalhostUrl("")).toBe(false)
  })
})
