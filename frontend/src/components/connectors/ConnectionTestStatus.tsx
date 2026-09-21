"use client"

/**
 * The stored result of a connection's last test, shared by /connections and
 * /connections/:id.
 *
 * POST /connections/:id/test persists last_tested_at / last_test_status /
 * last_test_error (api-gateway connections.go TestConnection), and every GET
 * derives is_connected from last_test_status == "success". The error is run
 * through llmscrub before it is stored, so it is safe to show as-is. A
 * "connector still being set up" answer is not a verdict and is not stored.
 */

import { AlertTriangle, CheckCircle2, RefreshCw, XCircle } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { formatAbsoluteTime, formatRelativeTime } from "@/lib/utils"

export type ConnectionTestState = "connected" | "expired" | "failed" | "untested"

export interface ConnectionTestFields {
  is_connected?: boolean
  is_expired?: boolean
  last_tested_at?: string | null
  last_test_status?: string | null
  last_test_error?: string | null
}

/**
 * A passing test wins over an expired token (the test just proved the
 * credentials work); an expired token wins over a failed test, because
 * re-authenticating is the fix to offer first.
 */
export function connectionTestState(c: ConnectionTestFields): ConnectionTestState {
  if (c.is_connected || c.last_test_status === "success") return "connected"
  if (c.is_expired) return "expired"
  if (c.last_test_status === "failed") return "failed"
  return "untested"
}

function when(iso?: string | null): { relative: string; absolute: string } | null {
  if (!iso) return null
  const absolute = formatAbsoluteTime(iso)
  if (!absolute) return null
  const relative = formatRelativeTime(iso)
  return { relative: relative === "Just now" ? "just now" : relative, absolute }
}

const NO_MESSAGE = "The connector returned no error message."

export function ConnectionTestBadge({ connection }: { connection: ConnectionTestFields }) {
  switch (connectionTestState(connection)) {
    case "connected":
      return (
        <Badge className="bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300">
          <CheckCircle2 className="h-3 w-3 mr-1" />
          Connected
        </Badge>
      )
    case "expired":
      return (
        <Badge className="bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300">
          <AlertTriangle className="h-3 w-3 mr-1" />
          Token expired
        </Badge>
      )
    case "failed":
      return (
        <Badge className="bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300">
          <XCircle className="h-3 w-3 mr-1" />
          Test failed
        </Badge>
      )
    default:
      return <Badge variant="secondary">Not tested</Badge>
  }
}

/** One line under a connection in the list: why its last test failed. */
export function LastTestErrorLine({ connection }: { connection: ConnectionTestFields }) {
  if (connectionTestState(connection) !== "failed") return null
  const t = when(connection.last_tested_at)
  const message = connection.last_test_error?.trim() || NO_MESSAGE
  return (
    <p
      className="mb-2 line-clamp-2 break-words text-xs text-red-700 dark:text-red-400"
      title={message}
      data-testid="connection-last-test-error"
    >
      <span className="font-medium">Last test failed{t ? ` ${t.relative}` : ""}:</span> {message}
    </p>
  )
}

/** The detail page's explanation of a failed last test, with a way to re-run it. */
export function LastTestFailedAlert({
  connection,
  onRetest,
  testing,
}: {
  connection: ConnectionTestFields
  onRetest: () => void
  testing: boolean
}) {
  if (connectionTestState(connection) !== "failed") return null
  const t = when(connection.last_tested_at)
  const message = connection.last_test_error?.trim() || NO_MESSAGE
  return (
    <Alert variant="destructive" data-testid="connection-last-test-failed">
      <XCircle className="h-4 w-4" />
      <AlertTitle>
        The last connection test failed
        {t && (
          <span className="font-normal" title={t.absolute}>
            {" "}
            · {t.relative}
          </span>
        )}
      </AlertTitle>
      <AlertDescription className="space-y-3">
        <pre className="max-h-40 overflow-auto whitespace-pre-wrap break-words rounded-md bg-red-50 p-2 font-mono text-xs text-red-900 dark:bg-red-950/40 dark:text-red-200">
          {message}
        </pre>
        <div className="flex flex-wrap items-center gap-3 text-sm">
          <span className="text-zinc-600 dark:text-zinc-400">Fix the settings with Edit Connection, then test again.</span>
          <Button variant="outline" size="sm" onClick={onRetest} disabled={testing}>
            <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${testing ? "animate-spin" : ""}`} />
            {testing ? "Testing..." : "Test again"}
          </Button>
        </div>
      </AlertDescription>
    </Alert>
  )
}

/** "Succeeded · 5m ago" / "Failed · 5m ago" / "Never" for the detail page's facts grid. */
export function lastTestSummary(c: ConnectionTestFields): string {
  const t = when(c.last_tested_at)
  if (!c.last_test_status || !t) return "Never"
  const verdict = c.last_test_status === "success" ? "Succeeded" : c.last_test_status === "failed" ? "Failed" : c.last_test_status
  return `${verdict} · ${t.relative}`
}
