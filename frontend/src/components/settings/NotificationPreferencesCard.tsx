"use client"

import { useCallback, useEffect, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import { Switch } from "@/components/ui/switch"
import { AlertTriangle, Bell, Info, Loader2 } from "lucide-react"
import { toast } from "sonner"
import {
  getNotificationPreferences,
  updateNotificationPreferences,
  type NotificationPreferences,
} from "@/lib/api/notification-settings"

/**
 * The caller's own email opt-out: email on/off, then per-category mutes.
 * Each switch saves on change and reverts if the save fails.
 *
 * Fails closed: if the read fails there are no switches, so a default can never
 * be shown as the user's setting or saved over it.
 */
export function NotificationPreferencesCard() {
  const [prefs, setPrefs] = useState<NotificationPreferences | null>(null)
  const [status, setStatus] = useState<"loading" | "failed" | "ready">("loading")
  const [saving, setSaving] = useState(false)

  // State is only set after the await, so the mount effect never sets state
  // synchronously; Retry flips back to "loading" itself.
  const fetchPrefs = useCallback(async () => {
    try {
      setPrefs(await getNotificationPreferences())
      setStatus("ready")
    } catch {
      setStatus("failed")
    }
  }, [])

  useEffect(() => { void fetchPrefs() }, [fetchPrefs])

  const load = () => {
    setStatus("loading")
    void fetchPrefs()
  }

  const save = async (next: NotificationPreferences) => {
    const previous = prefs
    setPrefs(next)
    setSaving(true)
    try {
      setPrefs(
        await updateNotificationPreferences({
          email_enabled: next.email_enabled,
          email_categories: next.email_categories,
        }),
      )
    } catch (err) {
      setPrefs(previous)
      toast.error(err instanceof Error ? err.message : "Failed to save notification preferences")
    } finally {
      setSaving(false)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Bell className="h-5 w-5 text-zinc-500" />
          Notifications
        </CardTitle>
        <CardDescription>
          Alerts about your pipelines always appear in the bell. Choose which ones are also emailed to you.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {status === "loading" ? (
          <p className="flex items-center gap-2 text-sm text-zinc-500">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading notification preferences…
          </p>
        ) : status === "failed" || !prefs ? (
          <div className="flex items-center justify-between gap-4">
            <p className="flex items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400">
              <AlertTriangle className="h-4 w-4 text-amber-500" />
              Could not load your notification preferences.
            </p>
            <Button variant="outline" size="sm" onClick={load}>Retry</Button>
          </div>
        ) : (
          <div className="space-y-4">
            {!prefs.channels.email && (
              <p className="flex items-start gap-2 rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-300">
                <Info className="mt-0.5 h-4 w-4 shrink-0" />
                Email delivery is not set up on this instance yet, so no email is sent. Your choices are
                saved and apply once an admin configures SMTP.
              </p>
            )}
            <div className="flex items-center justify-between gap-4">
              <div>
                <p className="font-medium">Email notifications</p>
                <p className="text-sm text-zinc-500">Send alerts about your pipelines to your account email</p>
              </div>
              {/* Radix renders a <button role="switch"> whose only child is the thumb —
                  no text, no associated label, so the name must come from aria-label. */}
              <Switch
                aria-label="Email notifications"
                checked={prefs.email_enabled}
                disabled={saving}
                onCheckedChange={(v) => void save({ ...prefs, email_enabled: v })}
              />
            </div>
            <Separator />
            <div className="space-y-3">
              {prefs.categories.map((cat) => (
                <div key={cat.id} className="flex items-center justify-between gap-4">
                  <div className={prefs.email_enabled ? "" : "opacity-60"}>
                    <p className="text-sm font-medium">{cat.label}</p>
                    <p className="text-xs text-zinc-500">{cat.description}</p>
                  </div>
                  <Switch
                    aria-label={`Email: ${cat.label}`}
                    checked={prefs.email_enabled && prefs.email_categories[cat.id] !== false}
                    disabled={saving || !prefs.email_enabled}
                    onCheckedChange={(v) =>
                      void save({ ...prefs, email_categories: { ...prefs.email_categories, [cat.id]: v } })
                    }
                  />
                </div>
              ))}
            </div>
            {prefs.channels.slack && (
              <p className="text-xs text-zinc-500">
                This instance also posts alerts to a shared Slack channel. An admin chooses which categories go there.
              </p>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
