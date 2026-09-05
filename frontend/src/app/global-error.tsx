"use client"

import * as Sentry from "@sentry/nextjs"
import { useEffect } from "react"

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  useEffect(() => {
    Sentry.captureException(error)
  }, [error])

  return (
    <html>
      <body className="flex min-h-screen items-center justify-center bg-zinc-50 dark:bg-zinc-950 p-6">
        <div className="max-w-md text-center space-y-4">
          <h1 className="text-2xl font-semibold text-zinc-900 dark:text-white">Something went wrong</h1>
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            An unexpected error occurred. The team has been notified.
            {error.digest && (
              <span className="block mt-1 font-mono text-xs text-zinc-400">
                Error ID: {error.digest}
              </span>
            )}
          </p>
          <button
            onClick={reset}
            className="inline-flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-medium text-white hover:bg-violet-700 transition-colors"
          >
            Try again
          </button>
        </div>
      </body>
    </html>
  )
}
