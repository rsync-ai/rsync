"use client"

/**
 * Saved-query version retention — the UI for GET/PUT
 * /api/v1/explorer/version-retention (migration 097), which shipped with no
 * screen. Until this card existed the only way to bound `saved_query_versions`
 * was an UPDATE by hand against the workspaces table.
 *
 * Three things about the backend shape the design here, all from
 * api-gateway/internal/handlers/saved_query_retention.go:
 *
 *  1. `retention_days: null` means KEEP FOREVER, and it is the migration
 *     default. It is a real value, not an absence — so the form models it as an
 *     explicit choice ("Keep forever" vs "Delete after N days"), never as an
 *     empty number input that a blur might turn into 0.
 *
 *  2. `min_versions` is REQUIRED on every PUT (400 otherwise), deliberately:
 *     "a client that meant to change only the age axis can silently widen what
 *     the age axis is allowed to delete". So the form always sends both axes.
 *
 *  3. The prune is NOT scheduled. It runs after the next successful edit of a
 *     given saved query (`pruneSavedQueryVersionsBestEffort`, called from
 *     saved_queries.go:836 and :1364), per query. Saying "saved" without saying
 *     that would leave an admin expecting a purge that will not happen until
 *     somebody edits something.
 */

import { useCallback, useEffect, useRef, useState } from "react"
import { toast } from "sonner"
import { History, Loader2 } from "lucide-react"

import { authFetch } from "@/lib/api/auth-fetch"
import { meetsRole } from "@/lib/workspace/roles"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

export const RETENTION_ENDPOINT = "/api/v1/explorer/version-retention"

/** Mirrors the CHECK constraints in migration 097 and the handler's 400s. */
export const MIN_VERSIONS_FLOOR = 5
export const MIN_VERSIONS_CEIL = 1000
export const RETENTION_DAYS_MIN = 1
export const RETENTION_DAYS_MAX = 3650

export type RetentionPolicy = {
  retention_days: number | null
  min_versions: number
}

/**
 * Validate locally before spending a round-trip, using the SAME wording the
 * server uses. The server re-checks — this is a faster path to the same
 * sentence, not a substitute for it, so a drift between the two shows up as the
 * server's message winning rather than as a silent accept.
 */
export function validateRetention(policy: RetentionPolicy): string | null {
  if (!Number.isInteger(policy.min_versions)) {
    return "min_versions must be a whole number"
  }
  if (policy.min_versions < MIN_VERSIONS_FLOOR || policy.min_versions > MIN_VERSIONS_CEIL) {
    return "min_versions must be between 5 and 1000; keeping fewer than 5 versions would leave nothing to restore from"
  }
  if (policy.retention_days !== null) {
    if (!Number.isInteger(policy.retention_days)) {
      return "retention_days must be a whole number"
    }
    if (policy.retention_days < RETENTION_DAYS_MIN || policy.retention_days > RETENTION_DAYS_MAX) {
      return "retention_days must be between 1 and 3650, or null to keep history forever"
    }
  }
  return null
}

/** Plain-English restatement of what the two axes do together. */
export function describePolicy(policy: RetentionPolicy): string {
  if (policy.retention_days === null) {
    return "Every version is kept forever. Nothing is deleted."
  }
  return (
    `Versions older than ${policy.retention_days} ${policy.retention_days === 1 ? "day" : "days"} are deleted — ` +
    `but the newest ${policy.min_versions} versions of each saved query are always kept, however old they are.`
  )
}

export function SavedQueryRetentionCard({ currentRole }: { currentRole: string }) {
  // Admin-only, mirroring security.WSAdmin on the PUT. The GET is viewer+ on
  // purpose ("knowing how long your own edit history survives is not privileged
  // information"), so everyone sees the policy and only admins get the form.
  const canEdit = meetsRole(currentRole, "admin")

  const [policy, setPolicy] = useState<RetentionPolicy | null>(null)
  const [loading, setLoading] = useState(true)
  // A failed read is a third outcome, not "no policy": rendering the default
  // ("keep forever") because a GET 500'd would tell an admin their history is
  // safe when the real stored policy might be deleting it.
  const [error, setError] = useState<string | null>(null)

  const [keepForever, setKeepForever] = useState(true)
  const [days, setDays] = useState("90")
  const [minVersions, setMinVersions] = useState(String(MIN_VERSIONS_FLOOR))
  const [saving, setSaving] = useState(false)
  // Synchronous in-flight latch — the disabled prop lands a render later, so a
  // fast double-click can fire two PUTs in one tick.
  const savingRef = useRef(false)

  const applyPolicy = useCallback((p: RetentionPolicy) => {
    setPolicy(p)
    setKeepForever(p.retention_days === null)
    // Keep the last non-null day count in the field when the admin toggles to
    // "keep forever", so toggling back does not lose what they typed.
    if (p.retention_days !== null) setDays(String(p.retention_days))
    setMinVersions(String(p.min_versions))
  }, [])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const res = await authFetch(RETENTION_ENDPOINT, { cache: "no-store" })
      const data = (await res.json().catch(() => null)) as (RetentionPolicy & { error?: string }) | null
      if (!res.ok) {
        setError(data?.error || `Could not load the retention policy (HTTP ${res.status})`)
        return
      }
      if (!data || typeof data.min_versions !== "number") {
        setError("Could not load the retention policy — unexpected response")
        return
      }
      applyPolicy({ retention_days: data.retention_days ?? null, min_versions: data.min_versions })
      setError(null)
    } catch {
      setError("Could not load the retention policy — the API is unreachable")
    } finally {
      setLoading(false)
    }
  }, [applyPolicy])

  useEffect(() => {
    void load()
  }, [load])

  const save = useCallback(async () => {
    if (savingRef.current) return

    const next: RetentionPolicy = {
      retention_days: keepForever ? null : Number(days.trim()),
      // Always sent, even when only the age axis changed — the handler requires it.
      min_versions: Number(minVersions.trim()),
    }

    const localError = validateRetention(next)
    if (localError) {
      toast.error(localError)
      return
    }

    savingRef.current = true
    setSaving(true)
    try {
      const res = await authFetch(RETENTION_ENDPOINT, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(next),
      })
      const data = (await res.json().catch(() => null)) as (RetentionPolicy & { error?: string }) | null
      if (!res.ok) {
        toast.error(data?.error || `Could not save the retention policy (HTTP ${res.status})`)
        return
      }
      const saved: RetentionPolicy = {
        retention_days: data?.retention_days ?? null,
        min_versions: typeof data?.min_versions === "number" ? data.min_versions : next.min_versions,
      }
      applyPolicy(saved)
      // The second sentence is the one that matters: the prune is per-query and
      // runs on the next edit of that query, not on a timer.
      toast.success(
        saved.retention_days === null
          ? "Saved — version history is now kept forever."
          : `Saved. Old versions are pruned the next time each saved query is edited, not on a schedule.`
      )
    } catch {
      toast.error("Could not save the retention policy — the API is unreachable")
    } finally {
      savingRef.current = false
      setSaving(false)
    }
  }, [applyPolicy, days, keepForever, minVersions])

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-sm font-medium">
          <History className="h-4 w-4" />
          Saved-query version history
        </CardTitle>
        <CardDescription className="text-xs">
          How much edit history the Data Explorer keeps for each saved query. The default keeps everything.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {loading && policy === null ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" />
            Loading…
          </div>
        ) : error ? (
          <div
            role="alert"
            className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300"
          >
            {error}
          </div>
        ) : (
          <>
            <p className="text-sm text-muted-foreground">{describePolicy(policy!)}</p>

            {canEdit ? (
              <div className="space-y-4">
                <fieldset className="space-y-2">
                  <legend className="text-sm font-medium">Age limit</legend>
                  <label className="flex items-center gap-2 text-sm">
                    <input
                      type="radio"
                      name="retention-mode"
                      checked={keepForever}
                      onChange={() => setKeepForever(true)}
                      disabled={saving}
                    />
                    Keep every version forever
                  </label>
                  <label className="flex items-center gap-2 text-sm">
                    <input
                      type="radio"
                      name="retention-mode"
                      checked={!keepForever}
                      onChange={() => setKeepForever(false)}
                      disabled={saving}
                    />
                    Delete versions older than
                    <Input
                      aria-label="Retention days"
                      className="h-8 w-24"
                      type="number"
                      min={RETENTION_DAYS_MIN}
                      max={RETENTION_DAYS_MAX}
                      value={days}
                      onChange={(e) => setDays(e.target.value)}
                      onFocus={() => setKeepForever(false)}
                      disabled={saving}
                    />
                    days
                  </label>
                </fieldset>

                <div className="space-y-1">
                  <Label htmlFor="min-versions" className="text-sm font-medium">
                    Always keep at least
                  </Label>
                  <div className="flex items-center gap-2">
                    <Input
                      id="min-versions"
                      aria-label="Minimum versions"
                      className="h-8 w-24"
                      type="number"
                      min={MIN_VERSIONS_FLOOR}
                      max={MIN_VERSIONS_CEIL}
                      value={minVersions}
                      onChange={(e) => setMinVersions(e.target.value)}
                      disabled={saving}
                    />
                    <span className="text-sm text-muted-foreground">versions per saved query</span>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    This floor wins over the age limit, so a query nobody has touched in a year still has something
                    to restore from. Between {MIN_VERSIONS_FLOOR} and {MIN_VERSIONS_CEIL}.
                  </p>
                </div>

                <div className="flex items-center gap-3">
                  <Button size="sm" onClick={() => void save()} disabled={saving}>
                    {saving && <Loader2 className="mr-2 h-3 w-3 animate-spin" />}
                    Save policy
                  </Button>
                  <span className="text-xs text-muted-foreground">
                    Deleted versions cannot be restored.
                  </span>
                </div>
              </div>
            ) : (
              <p className="text-xs text-muted-foreground">Only workspace admins can change this.</p>
            )}
          </>
        )}
      </CardContent>
    </Card>
  )
}
