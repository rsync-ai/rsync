"use client"

import { useSyncExternalStore } from "react"
import { formatAbsoluteTime } from "@/lib/utils"

// A server component that formats a time formats it in the SERVER's zone — UTC on
// every deployment we ship — and prints it with nothing saying so, so a viewer in
// IST reads 10:00 as their own 10:00 when it was 15:30. The viewer's zone is known
// only in the browser, so the local string can only be produced there.

// false during SSR and the first client render (so hydration matches), true after.
const noopSubscribe = () => () => {}
export function useHydrated(): boolean {
  return useSyncExternalStore(noopSubscribe, () => true, () => false)
}

/**
 * "2026-08-05 10:00 UTC". Built from toISOString, so it is the same on every server
 * and browser whatever their locale or zone — which is what lets the server render
 * and the first client render agree — and it names its zone.
 */
export function formatUtcDateTime(d: Date): string {
  return `${d.toISOString().slice(0, 16).replace("T", " ")} UTC`
}

/**
 * A point in time, in the viewer's own zone with the zone named. Before hydration
 * (and with JavaScript off) it shows the UTC time, labelled as UTC.
 */
export function LocalDateTime({
  value,
  fallback = "Unknown",
  className,
}: {
  value: Date | string | null | undefined
  fallback?: string
  className?: string
}) {
  const hydrated = useHydrated()
  if (value === null || value === undefined || value === "") return <>{fallback}</>
  const d = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(d.getTime())) return <>{fallback}</>
  const iso = d.toISOString()
  return (
    <time dateTime={iso} title={iso} className={className}>
      {hydrated ? formatAbsoluteTime(iso) : formatUtcDateTime(d)}
    </time>
  )
}
