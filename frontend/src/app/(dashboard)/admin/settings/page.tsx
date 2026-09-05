"use client"

import { useEffect, useState, useCallback } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group"
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { toast } from "sonner"
import { AlertTriangle, Save, UserPlus } from "lucide-react"
import { adminUpdateSettings } from "@/lib/api/admin"

export default function AdminSettingsPage() {
  const [loading, setLoading] = useState(true)
  const [pageStatus, setPageStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)
  const [registrationMode, setRegistrationMode] = useState("open")
  const [loadFailed, setLoadFailed] = useState(false)
  const [saving, setSaving] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    setPageStatus(null)
    setLoadFailed(false)
    try {
      const res = await authFetch("/api/v1/admin/settings", { method: "GET" })
      if (res.status === 401) { window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`; return }
      if (res.status === 403) { setPageStatus(403); setLoading(false); return }
      if (res.status === 429) {
        setPageStatus(429)
        setRetryAfter(parseInt(res.headers.get("Retry-After") || "", 10))
        setLoading(false)
        return
      }
      // Every other non-2xx used to fall through to res.json(), which SUCCEEDS —
      // the API returns {"error": "..."} on failure. So the catch never ran,
      // `?.registration_mode` was undefined, and `|| "open"` rendered Open
      // Registration over a read that never came back, with Save live to write
      // that invention back to an invite-only instance.
      if (!res.ok) throw new Error(`settings read failed with ${res.status}`)
      const json = await res.json()
      setRegistrationMode(json.settings?.registration_mode || "open")
    } catch {
      setLoadFailed(true)
    }
    setLoading(false)
  }, [])

  useEffect(() => { load() }, [load])

  const handleSave = async () => {
    setSaving(true)
    try {
      await adminUpdateSettings({ registration_mode: registrationMode })
      toast.success("Settings saved")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to save settings")
    }
    setSaving(false)
  }

  return (
    <div className="space-y-6">
      <PageHeader heading="Admin" description="Instance settings" />
      <AdminNav />

      {loading ? (
        <LoadingState />
      ) : pageStatus === 403 ? (
        <AccessDeniedState />
      ) : pageStatus === 429 ? (
        <RateLimitExceededState retryAfterSeconds={retryAfter} />
      ) : loadFailed ? (
        // No form here on purpose. Showing the controls would mean showing a
        // registration mode we never read, and leaving Save reachable would let
        // that guess be written back.
        <Card className="max-w-2xl">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-amber-500" />
              Could not load instance settings
            </CardTitle>
            <CardDescription>
              The current registration mode is unknown, so it is not shown and cannot be
              changed from here. Existing settings are unaffected.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" onClick={load}>Retry</Button>
          </CardContent>
        </Card>
      ) : (
        <div className="max-w-2xl space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <UserPlus className="h-5 w-5 text-zinc-500" />
                Registration
              </CardTitle>
              <CardDescription>
                Control how new users can sign up for this instance.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <RadioGroup value={registrationMode} onValueChange={setRegistrationMode}>
                <div className="flex items-start gap-3">
                  <RadioGroupItem value="open" id="reg-open" className="mt-1" />
                  <div>
                    <Label htmlFor="reg-open" className="font-medium cursor-pointer">Open Registration</Label>
                    <p className="text-sm text-zinc-500">Anyone can create an account.</p>
                  </div>
                </div>
                <div className="flex items-start gap-3">
                  <RadioGroupItem value="invite_only" id="reg-invite" className="mt-1" />
                  <div>
                    <Label htmlFor="reg-invite" className="font-medium cursor-pointer">Invite Only</Label>
                    <p className="text-sm text-zinc-500">
                      New users must have a valid invite link to register. Admins can create invite links from the Invitations tab.
                    </p>
                  </div>
                </div>
              </RadioGroup>

              <div className="flex justify-end pt-2">
                <Button onClick={handleSave} disabled={saving}>
                  <Save className="h-4 w-4 mr-1.5" />
                  {saving ? "Saving..." : "Save Changes"}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  )
}
