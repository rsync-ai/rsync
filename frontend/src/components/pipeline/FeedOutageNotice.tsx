"use client"

import { useEffect, useState } from "react"
import { WifiOff } from "lucide-react"
import { describeFeedOutage, type FeedHealth } from "@/lib/polling/feedHealth"

/**
 * Says, in the live pipeline views, that the live part has stopped.
 *
 * Renders nothing while the poll is healthy, so the normal case is untouched.
 * Its own 1s timer exists only to keep the "stopped Ns ago" figure honest while
 * the feed is down -- there are no state updates arriving to re-render it.
 */
export function FeedOutageNotice({ health, className }: { health: FeedHealth; className?: string }) {
  const [, setTick] = useState(0)
  const stale = health.consecutiveFailures >= 3

  useEffect(() => {
    if (!stale) return
    const t = setInterval(() => setTick((n) => n + 1), 1000)
    return () => clearInterval(t)
  }, [stale])

  const message = describeFeedOutage(health)
  if (!message) return null

  return (
    <div
      role="status"
      className={`flex items-start gap-2 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-700/60 dark:bg-amber-950/40 dark:text-amber-200 ${className || ""}`}
    >
      <WifiOff className="mt-0.5 h-3.5 w-3.5 shrink-0" />
      <span>{message}</span>
    </div>
  )
}
