import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { readRuntimeConfig, runtimeConfigScript } from "@/lib/config/runtime-env"

/**
 * The published images are built once, by CI, with
 * `NEXT_PUBLIC_API_URL=http://localhost:5001` baked in. `NEXT_PUBLIC_*` is
 * substituted by the compiler, so on a server install every runtime value
 * compose passes under that name is discarded and the browser is left believing
 * the api-gateway lives on the machine the operator is sitting at. On a laptop
 * that happens to be true, which is exactly why this class of bug survives every
 * localhost test -- and why the tests below hold the address non-loopback
 * throughout.
 */

const BAKED = "http://localhost:5001"
const SERVER_ORIGIN = "http://192.168.0.116:5098"
const GATEWAY = "http://192.168.0.116:5099"

const realLocation = window.location

function setPageOrigin(origin: string) {
  Object.defineProperty(window, "location", { configurable: true, value: new URL(origin) })
}

afterEach(() => {
  Object.defineProperty(window, "location", { configurable: true, value: realLocation })
  delete window.__RSYNC_RUNTIME__
  vi.unstubAllEnvs()
})

// ---------------------------------------------------------------------------
// the server half: what gets written into the document
// ---------------------------------------------------------------------------

describe("readRuntimeConfig", () => {
  beforeEach(() => {
    vi.stubEnv("PUBLIC_URL", "")
    vi.stubEnv("PUBLIC_WS_URL", "")
  })

  it("passes an absolute URL through", () => {
    vi.stubEnv("PUBLIC_URL", GATEWAY)
    vi.stubEnv("PUBLIC_WS_URL", `ws://192.168.0.116:5099/ws`)
    expect(readRuntimeConfig()).toEqual({
      apiUrl: GATEWAY,
      wsUrl: "ws://192.168.0.116:5099/ws",
    })
  })

  it("strips trailing slashes so callers can concatenate a path", () => {
    vi.stubEnv("PUBLIC_URL", `${GATEWAY}///`)
    expect(readRuntimeConfig().apiUrl).toBe(GATEWAY)
  })

  it("drops a value that is not a parseable absolute URL", () => {
    // Forwarding this would build every request against a string that cannot be
    // a base -- a stack of opaque TypeErrors instead of a fallback that works.
    vi.stubEnv("PUBLIC_URL", "not a url")
    expect(readRuntimeConfig().apiUrl).toBe("")
  })

  it("drops a URL whose scheme is not the one the caller will use", () => {
    vi.stubEnv("PUBLIC_URL", "file:///etc/passwd")
    vi.stubEnv("PUBLIC_WS_URL", GATEWAY) // http:// where ws:// is required
    expect(readRuntimeConfig()).toEqual({ apiUrl: "", wsUrl: "" })
  })

  it("is empty when nothing is set, which is every dev and cloud deployment", () => {
    expect(readRuntimeConfig()).toEqual({ apiUrl: "", wsUrl: "" })
  })
})

describe("runtimeConfigScript", () => {
  it("cannot be made to close its own <script> element", () => {
    // Defence at the point of writing, not two functions away in the validator.
    // sanitize() already rejects non-URLs, but this string is emitted into an
    // HTML script context and that is where the escaping belongs.
    vi.stubEnv("PUBLIC_URL", "http://x.example.com/a</script><script>alert(1)</script>")
    const script = runtimeConfigScript()
    expect(script).not.toContain("</script>")
    expect(script).toContain("\\u003c/script>")
  })

  it("assigns the global the client reads", () => {
    vi.stubEnv("PUBLIC_URL", GATEWAY)
    expect(runtimeConfigScript()).toContain("window.__RSYNC_RUNTIME__=")
  })
})

// ---------------------------------------------------------------------------
// the client half: the value must beat the baked one
// ---------------------------------------------------------------------------

/**
 * api.ts resolves its base at module scope, so each case needs a fresh module
 * registry with the global already in place. That is not a testing workaround --
 * it is the production constraint restated: if the global is not there before
 * the module evaluates, the app has already decided on the wrong address.
 */
async function importApiConfig() {
  vi.resetModules()
  return import("@/lib/config/api")
}

describe("API base resolution on a server install", () => {
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_API_URL", BAKED)
    setPageOrigin(SERVER_ORIGIN)
  })

  it("uses the runtime address, not the one baked into the image", async () => {
    window.__RSYNC_RUNTIME__ = { apiUrl: GATEWAY }
    const { API_GATEWAY_URL } = await importApiConfig()
    expect(API_GATEWAY_URL).toBe(GATEWAY)
  })

  it("falls back to the page origin when the operator sets nothing", async () => {
    // The pre-existing self-correcting behaviour, unchanged. Note it does NOT
    // fall back to the baked localhost value -- that would point the browser at
    // the operator's own machine.
    const { API_GATEWAY_URL } = await importApiConfig()
    expect(API_GATEWAY_URL).toBe(SERVER_ORIGIN)
    expect(API_GATEWAY_URL).not.toBe(BAKED)
  })

  it("still rebases a runtime value that leaked localhost", async () => {
    // An operator who sets PUBLIC_HOST=localhost and then browses by IP gets the
    // origin, not a loopback address resolving to the wrong machine.
    window.__RSYNC_RUNTIME__ = { apiUrl: BAKED }
    const { API_GATEWAY_URL } = await importApiConfig()
    expect(API_GATEWAY_URL).toBe(SERVER_ORIGIN)
  })

  it("leaves cross-origin local dev alone", async () => {
    // `next dev` on :3000 talking to the gateway on :5001 still needs the
    // explicit build-time override, and no runtime config is served there.
    setPageOrigin("http://localhost:3000")
    const { API_GATEWAY_URL } = await importApiConfig()
    expect(API_GATEWAY_URL).toBe(BAKED)
  })
})

// ---------------------------------------------------------------------------
// the ordering invariant, which no behavioural test in jsdom can observe
// ---------------------------------------------------------------------------

describe("the root layout injects the config in a way that wins the race", () => {
  // Read from disk rather than importing: the assertions below are about the
  // SHAPE of the source, and a compiled module no longer has one. vitest runs
  // with frontend/ as cwd.
  const layout = readFileSync(resolve(process.cwd(), "src/app/layout.tsx"), "utf8")

  // Comments are stripped before the negative assertions run. layout.tsx
  // EXPLAINS the rejected mechanisms by name, so matching raw text would fail on
  // the documentation of the very invariant being enforced -- and, worse, would
  // pass the moment someone deleted the explanation.
  const code = layout.replace(/\{?\/\*[\s\S]*?\*\/\}?/g, "")

  it("was actually read and stripped to something still substantial", () => {
    // A missing or renamed file would otherwise make every `not.toContain`
    // below pass on an empty string.
    expect(layout).toContain("export default function RootLayout")
    expect(code).toContain("export default function RootLayout")
    expect(code.length).toBeGreaterThan(500)
  })

  it("emits the script INLINE rather than deferring it", () => {
    // Two mechanisms were tried on a real browser and both lost to the login
    // chunk, sending every request to the UI's own origin:
    //
    //   next/script beforeInteractive -- in the App Router this renders as
    //     `(self.__next_s=self.__next_s||[]).push([...])`, a queue the Next
    //     runtime drains AFTER the first chunks have already run.
    //   <script src> in <head>    -- Next injects 14 async chunk tags ahead of
    //     anything this layout renders (measured position: 15 of 17), and an
    //     async script runs as soon as it downloads. Sometimes it wins.
    //
    // An inline script executes inside the parser's own task, which an async
    // external script cannot preempt, regardless of position in <head>.
    expect(code).toContain("dangerouslySetInnerHTML")
    expect(code).toContain("runtimeConfigScript()")
    expect(code).not.toMatch(/from\s+["']next\/script["']/)
    expect(code).not.toMatch(/<Script\b/)
    expect(code).not.toMatch(/<script[^>]*\bsrc=/)
  })

  it("renders per request, or the inlined value would freeze at build time", () => {
    // Prerendering this layout would bake the BUILDER's environment into the
    // HTML, which is the original bug wearing a different hat.
    expect(code).toMatch(/export const dynamic\s*=\s*["']force-dynamic["']/)
  })
})
