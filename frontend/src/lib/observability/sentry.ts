"use client"

import * as Sentry from "@sentry/nextjs"

/**
 * Set Sentry user context after authentication.
 * Call this once login/session is confirmed.
 */
export function setSentryUser(user: { id: string; email?: string; workspaceId?: string }) {
  Sentry.setUser({
    id: user.id,
    email: user.email,
  })
  if (user.workspaceId) {
    Sentry.setTag("workspace_id", user.workspaceId)
  }
}

/** Clear Sentry user context on logout */
export function clearSentryUser() {
  Sentry.setUser(null)
}

/**
 * Capture an error with extra context.
 * Wraps Sentry.captureException for convenience.
 */
export function captureError(
  err: unknown,
  context?: { pipelineId?: string; connectionId?: string; action?: string }
) {
  Sentry.withScope((scope) => {
    if (context?.pipelineId) scope.setTag("pipeline_id", context.pipelineId)
    if (context?.connectionId) scope.setTag("connection_id", context.connectionId)
    if (context?.action) scope.setTag("action", context.action)
    Sentry.captureException(err)
  })
}

/**
 * Propagate the current Sentry trace ID as a request header.
 * Allows correlating frontend actions with backend OTel traces.
 */
export function getSentryTraceHeaders(): Record<string, string> {
  const traceData = Sentry.getTraceData()
  const headers: Record<string, string> = {}
  if (traceData["sentry-trace"]) headers["sentry-trace"] = traceData["sentry-trace"]
  if (traceData["baggage"]) headers["baggage"] = traceData["baggage"]
  return headers
}
