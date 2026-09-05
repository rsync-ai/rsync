/**
 * The one piece of configuration that cannot be baked into the image.
 *
 * `NEXT_PUBLIC_*` is substituted by the compiler at `docker build` time, in the
 * server bundle as well as the client one. The published images are built once,
 * by CI, with `NEXT_PUBLIC_API_URL=http://localhost:5001` -- so on a server
 * install every runtime value compose passes under that name is discarded, and
 * the browser is left believing the api-gateway is wherever the *builder* said
 * it was. On a laptop that happens to be true, which is why this class of bug
 * survives every localhost test.
 *
 * So the values are read here, from the environment of the container that is
 * actually running, and emitted into the document. Read
 * src/app/layout.tsx for why they are emitted INLINE rather than fetched.
 */

/** Env var names, kept off the `NEXT_PUBLIC_` prefix precisely so the compiler leaves them alone. */
const API_URL_VAR = "PUBLIC_URL"
const WS_URL_VAR = "PUBLIC_WS_URL"

/**
 * Only absolute http(s)/ws(s) URLs are passed through. A malformed value is
 * dropped rather than forwarded, so the client falls back to its origin-based
 * guess instead of building every request against a string that cannot be a base.
 */
function sanitize(raw: string | undefined, schemes: readonly string[]): string {
  const value = (raw ?? "").trim()
  if (!value) return ""
  try {
    const parsed = new URL(value)
    if (!schemes.includes(parsed.protocol)) return ""
    return value.replace(/\/+$/, "")
  } catch {
    return ""
  }
}

export interface RuntimeConfig {
  apiUrl: string
  wsUrl: string
}

export function readRuntimeConfig(): RuntimeConfig {
  return {
    apiUrl: sanitize(process.env[API_URL_VAR], ["http:", "https:"]),
    wsUrl: sanitize(process.env[WS_URL_VAR], ["ws:", "wss:"]),
  }
}

/**
 * The script body to inline into the document head.
 *
 * `<` is escaped to `<` so a value containing `</script>` cannot close the
 * element and inject markup. That is belt-and-braces on top of sanitize(), which
 * already rejects anything that is not a parseable absolute URL -- but this
 * string is written into an HTML script context, and defence there belongs at
 * the point of writing, not at the point of validation two functions away.
 */
export function runtimeConfigScript(): string {
  const json = JSON.stringify(readRuntimeConfig()).replace(/</g, "\\u003c")
  return `window.__RSYNC_RUNTIME__=${json};`
}
