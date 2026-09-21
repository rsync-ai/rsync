import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDate(date: Date | string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(date))
}

export function formatDateTime(date: Date | string): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(date))
}

export function formatRelativeTime(date: Date | string): string {
  const now = new Date()
  const then = new Date(date)
  const diff = now.getTime() - then.getTime()
  
  const seconds = Math.floor(diff / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)
  const days = Math.floor(hours / 24)
  
  if (days > 0) return `${days}d ago`
  if (hours > 0) return `${hours}h ago`
  if (minutes > 0) return `${minutes}m ago`
  return "Just now"
}

/**
 * An elapsed time, truncated at each unit ("4m 45s", "1h 45m", "12.9s", "850ms"),
 * and "—" when there is nothing measured. Every part is floored: rounding the
 * leading unit turned 4m45s into "5m 45s" and could print "2m 60s" (#37).
 *
 * This was the copy that carried the #37 fix and the executions pages' own
 * formatter; it is now the shared one, which keeps the same flooring and adds a
 * tenth of a second below the minute — a 12.9 s stage read "12s" here and
 * "12.9s" on the pipeline page for the same run.
 */
export { formatDurationOrDash as formatElapsed } from "@/lib/duration"

/**
 * An absolute time in the zone of whoever runs it, with the zone named ("Aug 15,
 * 10:00 AM GMT+5:30"). A relative string is easier to read at a glance but ambiguous
 * when it matters, and a bare wall-clock time is ambiguous about whose wall.
 *
 * Call it in the browser: on the server it formats in the server's zone (usually
 * UTC). A server-rendered page uses <LocalDateTime> instead. Returns "" for an
 * unparseable timestamp rather than "Invalid Date".
 */
export function formatAbsoluteTime(iso: string): string {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return ""
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  })
}

export function truncate(str: string, length: number): string {
  if (str.length <= length) return str
  return str.slice(0, length) + "..."
}

export const isProduction = process.env.NODE_ENV === "production"
export const isDevelopment = process.env.NODE_ENV === "development"

