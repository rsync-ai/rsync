/**
 * Pure decisions about which chat turn is "the request" and when a prompt
 * belongs in a fresh thread. Kept free of React so they are unit-tested
 * directly (src/__tests__/chat-thread-intent.test.ts).
 */

/** Response types that mean the thread already produced a pipeline. */
export const PIPELINE_CREATED_RESPONSE_TYPES = ["pipeline_started", "pipeline_scheduled"] as const

// Short replies that answer the previous question instead of stating a request.
// Mirrors the leading words of api-gateway chat.ParseConfirmation.
const REPLY_RE =
  /^(yes|y|yeah|yep|yup|sure|ok|okay|go ahead|do it|create it|run it|start it|looks good|confirm(ed)?|proceed|continue|lgtm|correct|no|n|nope|nah|cancel|stop|abort|not yet)\b/i

/**
 * True while the gateway is waiting for a slot answer (a connector name or a
 * source/destination role), where a short typed reply is NOT a new request.
 * awaiting_confirmation is excluded: a typed prompt there may be a new request
 * ("mysql to aws s3") and the gateway re-intents it.
 */
export function isSlotFillingState(state: string | undefined | null): boolean {
  const s = String(state || "")
  return s.startsWith("awaiting_") && s !== "awaiting_confirmation"
}

export function isConversationalReply(prompt: string): boolean {
  return REPLY_RE.test(String(prompt || "").trim())
}

/**
 * The run header's "YOU ASKED" must stay the user's original request (issue
 * #3). Only a freely-typed prompt that starts a request may replace it: not a
 * UI command whose chat text is a label (a sync-mode card click passes
 * displayText "CDC (snapshot + changes)"), not a yes/no reply, and not an
 * answer to a slot-filling question ("postgresql" after "which destination?").
 */
export function nextActiveIntent(args: {
  current: string
  prompt: string
  displayText?: string
  awaitingSlot: boolean
}): string {
  const current = String(args.current || "")
  const prompt = String(args.prompt || "").trim()
  if (!prompt) return current
  if (String(args.displayText || "").trim()) return current
  if (current && (args.awaitingSlot || isConversationalReply(prompt))) return current
  return prompt
}

// A prompt that asks for a new pipeline, e.g. "Create a pipeline from MongoDB
// to GCS" or "sync orders from mysql into bigquery".
const NEW_PIPELINE_RE =
  /\b(create|build|set\s*up|setup|make|start|new)\b[\s\S]*\bpipeline\b|\b(sync|replicate|copy|move|stream|load|migrate|export)\b[\s\S]*\b(to|into)\b/i

/**
 * A new pipeline request typed after the thread already created a pipeline
 * starts its own thread (issue #13): otherwise the second MongoDB→GCS request
 * was appended under the first pipeline's transcript and sent on the first
 * pipeline's chat session. Replies, UI commands and slot answers stay put.
 */
export function shouldStartFreshThread(args: {
  prompt: string
  displayText?: string
  awaitingSlot: boolean
  threadResponseTypes: ReadonlyArray<string | undefined>
}): boolean {
  const prompt = String(args.prompt || "").trim()
  if (!prompt || String(args.displayText || "").trim()) return false
  if (args.awaitingSlot || isConversationalReply(prompt)) return false
  const created = args.threadResponseTypes.some((t) =>
    (PIPELINE_CREATED_RESPONSE_TYPES as ReadonlyArray<string>).includes(String(t || ""))
  )
  return created && NEW_PIPELINE_RE.test(prompt)
}

/** The browser's IANA time zone, or "" when unavailable. */
export function browserTimeZone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || ""
  } catch {
    return ""
  }
}
