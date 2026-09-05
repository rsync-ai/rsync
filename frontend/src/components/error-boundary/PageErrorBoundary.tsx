"use client"

import { useEffect } from "react"
import Link from "next/link"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"

export type PageErrorBoundaryProps = {
  title: string
  description: string
  backHref: string
  backLabel: string
  error: Error & { digest?: string }
  reset: () => void
  showDebug?: boolean
}

export function PageErrorBoundary({
  title,
  description,
  backHref,
  backLabel,
  error,
  reset,
  showDebug = process.env.NODE_ENV === "development",
}: PageErrorBoundaryProps) {
  useEffect(() => {
    // eslint-disable-next-line no-console
    console.error("[PageErrorBoundary]", { message: error.message, digest: error.digest, error })
  }, [error])

  return (
    <div className="min-h-[60vh] flex items-center justify-center p-8">
      <Card className="w-full max-w-xl p-8 border-zinc-200 dark:border-zinc-800">
        <div className="text-center">
          <div className="text-6xl">⚠️</div>
          <div className="mt-4 text-2xl font-bold text-zinc-900 dark:text-white">{title}</div>
          <div className="mt-2 text-sm text-zinc-600 dark:text-zinc-400">{description}</div>

          <div className="mt-4 rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900/40 dark:bg-red-950/20 dark:text-red-300">
            {error.message || "Unknown error"}
          </div>

          <div className="mt-6 flex flex-col gap-3 sm:flex-row sm:justify-center">
            <Button onClick={reset} className="bg-gradient-to-r from-violet-600 to-indigo-600">
              Try Again
            </Button>
            <Button variant="outline" asChild>
              <Link href={backHref}>{backLabel}</Link>
            </Button>
          </div>

          {showDebug && (
            <details className="mt-8 text-left">
              <summary className="cursor-pointer text-sm text-zinc-500">Debug Info</summary>
              <pre className="mt-3 overflow-auto rounded-lg bg-zinc-950 p-4 text-xs text-emerald-300">
                {error.stack || "No stack trace available"}
              </pre>
            </details>
          )}
        </div>
      </Card>
    </div>
  )
}
