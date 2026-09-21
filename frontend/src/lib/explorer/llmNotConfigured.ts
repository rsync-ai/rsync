// An install without an LLM is supported. The features that need a model answer
// 503 {"error":"llm_not_configured","message":"Set up an LLM first: ..."}
// (llm-service/src/utils/llm_gate.py, relayed by the gateway's
// llm_not_configured.go). Raw SQL runs without one, so the error says both.

export const LLM_NOT_CONFIGURED = "llm_not_configured"

export interface LlmNotConfiguredError {
  code: typeof LLM_NOT_CONFIGURED
  title: string
  message: string
  hint: string
}

const FALLBACK_MESSAGE = "Asking in plain English needs an LLM, and none is set up yet."

// Returns the Explorer error for an llm_not_configured body, flat or nested under
// "detail", and null for every other body -- including a plain 503 from a busy
// service, which must not tell the operator to configure what is already set up.
export function llmNotConfiguredError(data: unknown): LlmNotConfiguredError | null {
  if (!data || typeof data !== "object") return null
  const body = data as Record<string, unknown>
  const detail = body.detail
  const gate =
    body.error === LLM_NOT_CONFIGURED
      ? body
      : detail && typeof detail === "object" && (detail as Record<string, unknown>).error === LLM_NOT_CONFIGURED
        ? (detail as Record<string, unknown>)
        : null
  if (!gate) return null
  const message = typeof gate.message === "string" ? gate.message.trim() : ""
  return {
    code: LLM_NOT_CONFIGURED,
    title: "Set up an LLM first",
    message: message || FALLBACK_MESSAGE,
    hint: "Write SQL in the editor instead; it runs without an LLM.",
  }
}
