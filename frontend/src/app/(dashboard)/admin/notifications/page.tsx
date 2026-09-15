"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Separator } from "@/components/ui/separator"
import { Switch } from "@/components/ui/switch"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { toast } from "sonner"
import { AlertTriangle, Info, Loader2, Mail, MessageSquare, Save, Send } from "lucide-react"
import {
  getNotificationChannels,
  NotificationSettingsError,
  sendTestNotification,
  updateNotificationChannels,
  type CategoryToggles,
  type NotificationChannels,
  type NotificationChannelsUpdate,
  type SmtpTlsMode,
} from "@/lib/api/notification-settings"

interface ChannelForm {
  slackEnabled: boolean
  /** Blank keeps the saved webhook. */
  slackWebhook: string
  clearWebhook: boolean
  slackCategories: CategoryToggles
  emailEnabled: boolean
  smtpHost: string
  smtpPort: string
  smtpUsername: string
  /** Blank keeps the saved password. */
  smtpPassword: string
  clearPassword: boolean
  from: string
  tlsMode: SmtpTlsMode
}

const TLS_MODES: { value: SmtpTlsMode; label: string }[] = [
  { value: "starttls", label: "STARTTLS (usually port 587)" },
  { value: "tls", label: "Implicit TLS (usually port 465)" },
  { value: "none", label: "None (local relay only)" },
]

function formFromView(view: NotificationChannels): ChannelForm {
  const tls = view.email.tls_mode
  return {
    slackEnabled: view.slack.enabled,
    slackWebhook: "",
    clearWebhook: false,
    slackCategories: { ...view.slack.categories },
    emailEnabled: view.email.enabled,
    smtpHost: view.email.smtp_host,
    smtpPort: view.email.smtp_port ? String(view.email.smtp_port) : "587",
    smtpUsername: view.email.smtp_username,
    smtpPassword: "",
    clearPassword: false,
    from: view.email.from,
    // The environment fallback reports "opportunistic" (STARTTLS when offered),
    // which cannot be saved. STARTTLS is the strict version of the same thing.
    tlsMode: tls === "tls" || tls === "none" ? tls : "starttls",
  }
}

function updateFromForm(form: ChannelForm): NotificationChannelsUpdate {
  const webhook = form.slackWebhook.trim()
  return {
    slack: {
      enabled: form.slackEnabled,
      webhook_url: form.clearWebhook ? "" : webhook || null,
      categories: form.slackCategories,
    },
    email: {
      enabled: form.emailEnabled,
      smtp_host: form.smtpHost.trim(),
      smtp_port: parseInt(form.smtpPort, 10) || 0,
      smtp_username: form.smtpUsername.trim(),
      smtp_password: form.clearPassword ? "" : form.smtpPassword || null,
      from: form.from.trim(),
      tls_mode: form.tlsMode,
    },
  }
}

export default function AdminNotificationsPage() {
  const [loading, setLoading] = useState(true)
  const [pageStatus, setPageStatus] = useState<number | null>(null)
  const [loadFailed, setLoadFailed] = useState(false)
  const [view, setView] = useState<NotificationChannels | null>(null)
  const [form, setForm] = useState<ChannelForm | null>(null)
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState<"slack" | "email" | null>(null)

  // State is only set after the await, so the mount effect never sets state
  // synchronously; Retry resets the flags itself.
  const fetchChannels = useCallback(async () => {
    try {
      const next = await getNotificationChannels()
      setView(next)
      setForm(formFromView(next))
    } catch (err) {
      const status = err instanceof NotificationSettingsError ? err.status : 0
      if (status === 401) {
        window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`
        return
      }
      if (status === 403 || status === 429) setPageStatus(status)
      // Fail closed: without a read there is no form, so blank fields can never
      // be saved over a working SMTP or Slack setup.
      else setLoadFailed(true)
    }
    setLoading(false)
  }, [])

  useEffect(() => { void fetchChannels() }, [fetchChannels])

  const load = () => {
    setLoading(true)
    setPageStatus(null)
    setLoadFailed(false)
    void fetchChannels()
  }

  const dirty = useMemo(() => {
    if (!view || !form) return false
    return JSON.stringify(form) !== JSON.stringify(formFromView(view))
  }, [view, form])

  const patch = (changes: Partial<ChannelForm>) => setForm((f) => (f ? { ...f, ...changes } : f))

  const handleSave = async () => {
    if (!form) return
    setSaving(true)
    try {
      const next = await updateNotificationChannels(updateFromForm(form))
      setView(next)
      setForm(formFromView(next))
      toast.success("Notification channels saved")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to save notification channels")
    }
    setSaving(false)
  }

  const handleTest = async (channel: "slack" | "email") => {
    setTesting(channel)
    try {
      const result = await sendTestNotification(channel)
      toast.success(
        channel === "email" ? `Test email sent to ${result.sent_to ?? "your address"}` : "Test message sent to Slack",
      )
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Test notification failed")
    }
    setTesting(null)
  }

  const testDisabledReason = (channel: "slack" | "email"): string | null => {
    if (dirty) return "Save your changes first — the test uses the saved settings."
    if (view && !view[channel].enabled) return "Enable and save this channel to send a test."
    return null
  }

  return (
    <div className="space-y-6">
      <PageHeader heading="Admin" description="Notification delivery" />
      <AdminNav />

      {loading ? (
        <LoadingState />
      ) : pageStatus === 403 ? (
        <AccessDeniedState />
      ) : pageStatus === 429 ? (
        <RateLimitExceededState retryAfterSeconds={null} />
      ) : loadFailed || !view || !form ? (
        <Card className="max-w-2xl">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-amber-500" />
              Could not load notification channels
            </CardTitle>
            <CardDescription>
              The current Slack and email settings are unknown, so they are not shown and cannot be
              changed from here. Delivery keeps using whatever is saved.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" onClick={load}>Retry</Button>
          </CardContent>
        </Card>
      ) : (
        <div className="max-w-2xl space-y-6">
          <div className="flex items-start gap-2 rounded-md border border-zinc-200 bg-zinc-50 p-3 text-sm text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-400">
            <Info className="mt-0.5 h-4 w-4 shrink-0" />
            {view.source === "environment" ? (
              <p>
                Nothing saved yet. Delivery currently uses the <code>SMTP_*</code> and{" "}
                <code>NOTIFIER_SLACK_WEBHOOK_URL</code> environment variables. Saving here replaces them.
              </p>
            ) : (
              <p>
                Alerts always appear in the in-app bell. These settings also send them to Slack and to
                each pipeline owner by email
                {view.updated_at ? ` · last saved ${new Date(view.updated_at).toLocaleString()}` : ""}.
              </p>
            )}
          </div>

          {/* Slack */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between gap-4">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <MessageSquare className="h-5 w-5 text-zinc-500" />
                    Slack
                  </CardTitle>
                  <CardDescription>Post alerts to one channel through an incoming webhook.</CardDescription>
                </div>
                <Switch
                  aria-label="Send alerts to Slack"
                  checked={form.slackEnabled}
                  onCheckedChange={(v) => patch({ slackEnabled: v })}
                />
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="slack-webhook">Incoming webhook URL</Label>
                <Input
                  id="slack-webhook"
                  type="password"
                  autoComplete="off"
                  placeholder={
                    view.slack.webhook_configured && !form.clearWebhook
                      ? "Saved — leave blank to keep it"
                      : "https://hooks.slack.com/services/…"
                  }
                  value={form.slackWebhook}
                  onChange={(e) => patch({ slackWebhook: e.target.value, clearWebhook: false })}
                />
                {view.slack.webhook_configured && (
                  <p className="text-xs text-zinc-500">
                    {form.clearWebhook ? (
                      <>The saved webhook will be removed when you save. </>
                    ) : (
                      <>A webhook is saved. </>
                    )}
                    <button
                      type="button"
                      className="underline"
                      onClick={() => patch({ clearWebhook: !form.clearWebhook, slackWebhook: "" })}
                    >
                      {form.clearWebhook ? "Keep it" : "Remove it"}
                    </button>
                  </p>
                )}
              </div>

              <Separator />
              <div className="space-y-3">
                <div>
                  <p className="text-sm font-medium">Alerts sent to Slack</p>
                  <p className="text-xs text-zinc-500">Muted categories still appear in the bell and in email.</p>
                </div>
                {view.categories.map((cat) => (
                  <div key={cat.id} className="flex items-center justify-between gap-4">
                    <div>
                      <p className="text-sm">{cat.label}</p>
                      <p className="text-xs text-zinc-500">{cat.description}</p>
                    </div>
                    <Switch
                      aria-label={`Slack: ${cat.label}`}
                      checked={form.slackCategories[cat.id] !== false}
                      onCheckedChange={(v) =>
                        patch({ slackCategories: { ...form.slackCategories, [cat.id]: v } })
                      }
                    />
                  </div>
                ))}
              </div>

              <TestRow
                label="Send test message"
                busy={testing === "slack"}
                disabledReason={testDisabledReason("slack")}
                onTest={() => handleTest("slack")}
              />
            </CardContent>
          </Card>

          {/* Email */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between gap-4">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <Mail className="h-5 w-5 text-zinc-500" />
                    Email (SMTP)
                  </CardTitle>
                  <CardDescription>
                    Email each pipeline owner. Users choose which categories they receive under Settings.
                  </CardDescription>
                </div>
                <Switch
                  aria-label="Send alerts by email"
                  checked={form.emailEnabled}
                  onCheckedChange={(v) => patch({ emailEnabled: v })}
                />
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-4 sm:grid-cols-[1fr_7rem]">
                <div className="space-y-2">
                  <Label htmlFor="smtp-host">SMTP host</Label>
                  <Input
                    id="smtp-host"
                    placeholder="smtp.example.com"
                    value={form.smtpHost}
                    onChange={(e) => patch({ smtpHost: e.target.value })}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smtp-port">Port</Label>
                  <Input
                    id="smtp-port"
                    inputMode="numeric"
                    value={form.smtpPort}
                    onChange={(e) => patch({ smtpPort: e.target.value.replace(/\D/g, "") })}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="smtp-tls">Encryption</Label>
                <Select value={form.tlsMode} onValueChange={(v) => patch({ tlsMode: v as SmtpTlsMode })}>
                  <SelectTrigger id="smtp-tls">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {TLS_MODES.map((m) => (
                      <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {view.email.tls_mode === "opportunistic" && (
                  <p className="text-xs text-zinc-500">
                    The environment fallback upgrades to TLS only when the server offers it. Saving pins the
                    mode chosen here.
                  </p>
                )}
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor="smtp-username">Username</Label>
                  <Input
                    id="smtp-username"
                    autoComplete="off"
                    value={form.smtpUsername}
                    onChange={(e) => patch({ smtpUsername: e.target.value })}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="smtp-password">Password</Label>
                  <Input
                    id="smtp-password"
                    type="password"
                    autoComplete="new-password"
                    placeholder={
                      view.email.password_configured && !form.clearPassword ? "Saved — leave blank to keep it" : ""
                    }
                    value={form.smtpPassword}
                    onChange={(e) => patch({ smtpPassword: e.target.value, clearPassword: false })}
                  />
                  {view.email.password_configured && (
                    <p className="text-xs text-zinc-500">
                      {form.clearPassword ? "The saved password will be removed when you save. " : "A password is saved. "}
                      <button
                        type="button"
                        className="underline"
                        onClick={() => patch({ clearPassword: !form.clearPassword, smtpPassword: "" })}
                      >
                        {form.clearPassword ? "Keep it" : "Remove it"}
                      </button>
                    </p>
                  )}
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="smtp-from">From address</Label>
                <Input
                  id="smtp-from"
                  placeholder="rsync alerts <alerts@example.com>"
                  value={form.from}
                  onChange={(e) => patch({ from: e.target.value })}
                />
              </div>

              <TestRow
                label="Send test email to me"
                busy={testing === "email"}
                disabledReason={testDisabledReason("email")}
                onTest={() => handleTest("email")}
              />
            </CardContent>
          </Card>

          <div className="flex items-center justify-end gap-3">
            {dirty && <span className="text-sm text-zinc-500">Unsaved changes</span>}
            <Button onClick={handleSave} disabled={saving || !dirty}>
              {saving ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" /> : <Save className="mr-1.5 h-4 w-4" />}
              {saving ? "Saving..." : "Save changes"}
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}

function TestRow({
  label,
  busy,
  disabledReason,
  onTest,
}: {
  label: string
  busy: boolean
  disabledReason: string | null
  onTest: () => void
}) {
  return (
    <div className="flex flex-wrap items-center justify-end gap-3 pt-2">
      {disabledReason && <span className="text-xs text-zinc-500">{disabledReason}</span>}
      <Button variant="outline" size="sm" onClick={onTest} disabled={busy || disabledReason !== null}>
        {busy ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" /> : <Send className="mr-1.5 h-4 w-4" />}
        {label}
      </Button>
    </div>
  )
}
