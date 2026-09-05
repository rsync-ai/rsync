"use client"

import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"

export function AccessDeniedState({
  title = "Access denied",
  message = "You don't have access to this section.",
}: {
  title?: string
  message?: string
}) {
  return (
    <Card className="border border-red-200 bg-red-50 p-4 dark:border-red-900/40 dark:bg-red-950/20">
      <div className="font-semibold text-red-800 dark:text-red-200">{title}</div>
      <div className="mt-1 text-sm text-red-700 dark:text-red-300">{message}</div>
      <div className="mt-3">
        <Button variant="outline" asChild>
          <Link href="/">Back to home</Link>
        </Button>
      </div>
    </Card>
  )
}

export function RateLimitExceededState({
  retryAfterSeconds,
}: {
  retryAfterSeconds?: number | null
}) {
  return (
    <Card className="border border-amber-200 bg-amber-50 p-4 dark:border-amber-900/40 dark:bg-amber-950/20">
      <div className="font-semibold text-amber-900 dark:text-amber-100">Rate limit exceeded</div>
      <div className="mt-1 text-sm text-amber-800 dark:text-amber-200">
        Please retry{typeof retryAfterSeconds === "number" ? ` in ~${retryAfterSeconds}s` : ""}.
      </div>
    </Card>
  )
}

export function LoadingState({ label = "Loading…" }: { label?: string }) {
  return (
    <Card className="border border-zinc-200 bg-white p-4 dark:border-zinc-800 dark:bg-zinc-900">
      <div className="text-sm text-zinc-600 dark:text-zinc-300">{label}</div>
    </Card>
  )
}

