/**
 * Centralized, connector-agnostic error handling utilities.
 *
 * Goals:
 * - Extract a useful human message from ANY backend error format (Go/Gin, FastAPI, tool-generator v1/v2, plain text)
 * - Work with our existing APIRequestError + tracedJsonFetch patterns
 * - Avoid leaking secrets (do not stringify configs)
 */

// ─── Structured UI error ────────────────────────────────────────────────────
// Use AppError anywhere you need to render a structured error in the UI
// (title as heading, message in mono, hint as actionable 💡 tip).
// Build one with classifyError() instead of constructing it by hand.

export type AppError = {
  /** Short label shown as the error heading. */
  title: string
  /** Raw server message or human fallback — shown in monospace. */
  message: string
  /** Actionable suggestion shown below the message (optional). */
  hint?: string
}

type ErrorContext =
  | "connections.load"
  | "connections.test"
  | "connections.create"
  | "connections.update"
  | "connections.delete"
  | "connectors.load"
  | "connector.save"
  | "connector.generate"
  | "connector.validate"
  | "pipeline.load"
  | "pipeline.run"
  | "pipeline.pause"
  | "pipeline.resume"
  | "pipeline.stop"
  | "pipeline.delete"
  | "pipeline.export"
  | "general"

/**
 * Convert any thrown value into a structured AppError for display.
 * Uses parseApiError() internally so every known backend error format is handled.
 */
export function classifyError(err: unknown, context: ErrorContext = "general"): AppError {
  const parsed = parseApiError(err)
  const status = parsed.statusCode
  const raw = parsed.message || "An unexpected error occurred"

  // ── Auth ────────────────────────────────────────────────────────────────
  if (status === 401 || status === 403) {
    return {
      title: "Authentication error",
      message: raw,
      hint: "Your session may have expired. Refresh the page to sign in again.",
    }
  }

  // ── Service unavailable ─────────────────────────────────────────────────
  if (status === 503) {
    return {
      title: "Service unavailable",
      message: raw,
      hint: _serviceHint(context),
    }
  }

  // ── Validation / bad request ─────────────────────────────────────────────
  if (status === 422 || status === 400) {
    return {
      title: "Invalid request",
      message: raw,
      hint: parsed.suggestion || "Check that all required fields are filled in correctly.",
    }
  }

  // ── Conflict ─────────────────────────────────────────────────────────────
  if (status === 409) {
    return {
      title: "Conflict",
      message: raw,
      hint: "This resource is still in use by other components. Remove those dependencies first.",
    }
  }

  // ── Not found ────────────────────────────────────────────────────────────
  if (status === 404) {
    return { title: "Not found", message: raw }
  }

  // ── Network / ECONNREFUSED ───────────────────────────────────────────────
  if (parsed.code === "NETWORK_ERROR" || raw.toLowerCase().includes("failed to fetch") || raw.toLowerCase().includes("networkerror")) {
    return {
      title: "Connection failed",
      message: "Could not reach the server.",
      hint: _networkHint(context),
    }
  }

  // ── Context-specific titles ──────────────────────────────────────────────
  const title = _titleForContext(context)
  const hint = parsed.suggestion || _hintForContext(context, raw)
  return { title, message: raw, hint }
}

/** Build an AppError with an explicit title override (e.g. for validation messages). */
export function makeAppError(title: string, message: string, hint?: string): AppError {
  return { title, message, hint }
}

// ── Private helpers ──────────────────────────────────────────────────────────

function _titleForContext(ctx: ErrorContext): string {
  const map: Record<ErrorContext, string> = {
    "connections.load": "Could not load connections",
    "connections.test": "Connection test failed",
    "connections.create": "Failed to create connection",
    "connections.update": "Failed to update connection",
    "connections.delete": "Failed to delete connection",
    "connectors.load": "Could not load connectors",
    "connector.save": "Failed to save connector",
    "connector.generate": "Connector generation failed",
    "connector.validate": "Validation failed",
    "pipeline.load": "Could not load pipelines",
    "pipeline.run": "Could not start pipeline",
    "pipeline.pause": "Could not pause pipeline",
    "pipeline.resume": "Could not resume pipeline",
    "pipeline.stop": "Could not stop pipeline",
    "pipeline.delete": "Could not delete pipeline",
    "pipeline.export": "Export failed",
    "general": "Something went wrong",
  }
  return map[ctx] ?? "Something went wrong"
}

function _hintForContext(ctx: ErrorContext, raw: string): string | undefined {
  const lower = raw.toLowerCase()
  if (ctx === "connections.test") {
    return lower.includes("timeout") || lower.includes("timed out")
      ? "The database host may be unreachable or blocked by a firewall."
      : lower.includes("password") || lower.includes("authentication") || lower.includes("auth")
      ? "Check your username and password."
      : lower.includes("ssl") || lower.includes("certificate")
      ? "Try toggling SSL mode in your connection config."
      : "Check your credentials and host settings, then try again."
  }
  if (ctx === "connector.generate") {
    return lower.includes("timeout") || lower.includes("timed out")
      ? "Generation timed out. Try again — large APIs can take 2-3 minutes."
      : lower.includes("rate limit") || lower.includes("429")
      ? "API rate limit hit. Wait a moment then retry."
      : "Check your API docs URL and try again."
  }
  if (ctx === "pipeline.run") {
    return lower.includes("connection") ? "Verify your source and destination connections are still active." : undefined
  }
  return undefined
}

function _serviceHint(ctx: ErrorContext): string {
  if (ctx === "connector.generate" || ctx === "connector.validate") {
    return "The connector generation service may be temporarily unavailable. Try again in a moment."
  }
  if (ctx.startsWith("pipeline.")) {
    return "The pipeline runner service may be temporarily unavailable. Try again shortly."
  }
  return "A backend service is temporarily unavailable. Check that Docker and the API Gateway are running."
}

function _networkHint(ctx: ErrorContext): string {
  if (ctx.startsWith("connections.") || ctx.startsWith("connectors.") || ctx.startsWith("pipeline.")) {
    return "Make sure Docker Desktop is running and the API Gateway is up: `docker compose up -d`"
  }
  return "Check your network connection and try again."
}

export type ParsedApiError = {
  message: string
  code?: string
  details?: string
  field?: string
  suggestion?: string
  statusCode?: number
  raw?: unknown
}

/**
 * Convert a failed fetch Response into an Error that `parseApiError()` can understand.
 * We encode the HTTP status + raw body as: "HTTP <code>: <body>"
 */
export async function httpErrorFromResponse(response: Response, fallbackMessage: string): Promise<Error> {
  let body = ""
  try {
    body = await response.text()
  } catch {
    body = ""
  }
  const payload = body && body.trim() ? body : JSON.stringify({ error: fallbackMessage })
  return new Error(`HTTP ${response.status}: ${payload}`)
}

/**
 * Turn a failed `Response` into the one line a toast should show.
 *
 * KI-CDC-CONTROL-502-BODY-NOT-IN-TOAST — why this exists, and why the body is
 * read as TEXT exactly once:
 *
 * The CDC control buttons each carried their own copy of a helper that did
 * `await res.json().catch(() => null)` first and then fell back to
 * `await res.text().catch(() => "")`. That fallback is DEAD CODE. Per the Fetch
 * spec `Response.json()` marks the body disturbed *before* it parses, so once
 * `json()` has run — success or failure — `text()` always rejects with
 * "Body has already been read". On a 502 whose body is non-JSON or empty the
 * chain therefore evaluated to `"" || res.statusText || "Request failed"`, and
 * `statusText` is empty on HTTP/2, so the operator saw the bare literal
 * "Request failed" with the orchestrator's real reason nowhere in sight.
 *
 * So: read text ONCE, parse it ourselves, and make the last resort carry the
 * HTTP status — a message that names nothing is never acceptable.
 *
 * @param res    the failed response (already known to be !ok)
 * @param action optional verb for the last-resort line, e.g. "Pause" ->
 *               "Pause failed (HTTP 502)". Omit it when the toast title already
 *               names the action, otherwise the user reads "Stop failed" twice.
 */
export async function readResponseErrorMessage(res: Response, action?: string): Promise<string> {
  let body = ""
  try {
    body = await res.text()
  } catch {
    body = ""
  }
  const trimmed = body.trim()

  if (trimmed) {
    const json = safeJsonParse(trimmed)
    if (json && typeof json === "object") {
      const rec = json as Record<string, unknown>
      const msg = String(rec["message"] || rec["error"] || rec["detail"] || "").trim()
      if (msg) return msg
      // Structured, but with nothing human-readable in it. Fall through to the
      // status line rather than dumping a JSON blob into a toast.
    } else {
      // Not JSON at all (an nginx/Envoy error page, a proxy's plain text). This
      // is the case the old json()-first ordering could never reach.
      return trimmed.length > 300 ? `${trimmed.slice(0, 300)}…` : trimmed
    }
  }

  const what = action ? `${action} failed` : "Request failed"
  const statusText = res.statusText.trim()
  return statusText ? `${what}: ${statusText} (HTTP ${res.status})` : `${what} (HTTP ${res.status})`
}

/**
 * Parse an unknown thrown error into a normalized structure.
 * This is safe to call from anywhere (UI, API wrappers, hooks).
 */
export function parseApiError(err: unknown): ParsedApiError {
  // If it's our structured APIRequestError, preserve its fields (duck-typed to avoid cycles).
  const apiRequest = asApiRequestError(err)
  if (apiRequest) return apiRequest

  // Handle common patterns like: "HTTP 500: { ...json... }"
  if (err instanceof Error) {
    const parsed = parseFromHttpPrefixedError(err.message)
    if (parsed) return parsed
  }
  if (typeof err === "string") {
    const parsed = parseFromHttpPrefixedError(err)
    if (parsed) return parsed

    // Raw JSON string bodies (common when we use response.text()).
    const json = safeJsonParse(err)
    if (json && typeof json === "object") {
      return { ...normalizeErrorObject(json as Record<string, unknown>), raw: json }
    }
  }

  // Network errors
  if (err instanceof TypeError) {
    const msg = err.message || ""
    if (msg.includes("Failed to fetch") || msg.includes("NetworkError")) {
      return {
        message: "Unable to connect to the server",
        code: "NETWORK_ERROR",
        suggestion: "Please check your connection and try again.",
        raw: err,
      }
    }
  }

  // JSON-like objects from fetch clients
  if (err && typeof err === "object") {
    const obj = err as Record<string, unknown>
    // Sometimes errors include a raw string body
    const rawBody = asString(obj.body) || asString(obj.response) || asString(obj.data)
    if (rawBody) {
      const parsedHttp = parseFromHttpPrefixedError(rawBody)
      if (parsedHttp) return parsedHttp
      const json = safeJsonParse(rawBody)
      if (json && typeof json === "object") {
        return { ...normalizeErrorObject(json as Record<string, unknown>), raw: json }
      }
    }

    // Direct structured shape
    if (hasAnyKey(obj, ["error_message", "error", "message", "detail", "details", "code"])) {
      return { ...normalizeErrorObject(obj), raw: obj }
    }
  }

  // Plain Error fallback
  if (err instanceof Error) {
    return { message: err.message || "An unexpected error occurred", raw: err }
  }

  return { message: "An unexpected error occurred", raw: err }
}

/**
 * Best-effort message extraction for toasts, inline errors, etc.
 */
export function extractErrorMessage(err: unknown): string {
  const parsed = parseApiError(err)
  return parsed.message || "Unknown error"
}

// -----------------------------
// Internal helpers
// -----------------------------

function parseFromHttpPrefixedError(input: string): ParsedApiError | null {
  const str = String(input || "")
  const m = str.match(/^HTTP\s+(\d+)\s*:\s*([\s\S]+)$/)
  if (!m) return null

  const statusCode = Number(m[1])
  const body = m[2] || ""

  // Try to parse JSON response body
  const json = safeJsonParse(body)
  if (json && typeof json === "object") {
    return {
      statusCode,
      ...normalizeErrorObject(json as Record<string, unknown>),
      raw: json,
    }
  }

  return {
    statusCode,
    message: body.trim() || `Request failed (HTTP ${statusCode})`,
    raw: body,
  }
}

function normalizeErrorObject(obj: Record<string, unknown>): Omit<ParsedApiError, "statusCode"> {
  // Tool-generator v2 / API Gateway patterns
  const msg =
    asString(obj.error_message) ||
    asString(obj.error) ||
    asString(obj.message) ||
    asString(obj.detail) ||
    // Some handlers return { errors: [...] }
    normalizeFastApiDetail(obj.detail) ||
    "An unexpected error occurred"

  const code = asString(obj.code) || asString(obj.error_code)
  const details = asString(obj.details) || asString(obj.hint)
  const field = asString(obj.field)
  const suggestion = asString(obj.suggestion)

  return { message: msg, code, details, field, suggestion }
}

function normalizeFastApiDetail(detail: unknown): string | undefined {
  // FastAPI/Pydantic: { detail: [{loc,msg,type}, ...] }
  if (!Array.isArray(detail)) return undefined
  const first = detail[0] as Record<string, unknown>
  if (!first) return undefined
  const msg = asString(first["msg"]) || asString(first["message"])
  const loc = Array.isArray(first["loc"]) ? (first["loc"] as unknown[]).join(".") : undefined
  if (msg && loc) return `${loc}: ${msg}`
  return msg
}

function asString(v: unknown): string | undefined {
  if (typeof v === "string" && v.trim()) return v.trim()
  return undefined
}

function safeJsonParse(text: string): unknown | null {
  try {
    return JSON.parse(text)
  } catch {
    return null
  }
}

function hasAnyKey(obj: Record<string, unknown>, keys: string[]): boolean {
  return keys.some((k) => Object.prototype.hasOwnProperty.call(obj, k))
}

function asApiRequestError(err: unknown): ParsedApiError | null {
  if (!err || typeof err !== "object") return null
  const errObj = err as Record<string, unknown>
  if (errObj["name"] !== "APIRequestError") return null
  const apiErr = errObj["apiError"]
  if (!apiErr || typeof apiErr !== "object") {
    return { message: String(errObj["message"] || "An unexpected error occurred"), raw: err }
  }
  const api = apiErr as Record<string, unknown>
  return {
    message: String(api["message"] || errObj["message"] || "An unexpected error occurred"),
    code: api["code"] ? String(api["code"]) : undefined,
    details: api["details"] ? String(api["details"]) : undefined,
    field: api["field"] ? String(api["field"]) : undefined,
    suggestion: api["suggestion"] ? String(api["suggestion"]) : undefined,
    raw: apiErr,
  }
}


