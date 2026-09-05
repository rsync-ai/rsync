"use client"

import { useState, useEffect } from "react"
import { useTheme } from "next-themes"
import { useRouter } from "next/navigation"
import { PageHeader } from "@/components/layout/PageHeader"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Separator } from "@/components/ui/separator"
import { Switch } from "@/components/ui/switch"
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar"
import { User, Bell, Palette, Save, Loader2, Lock, Eye, EyeOff } from "lucide-react"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { toast } from "sonner"
import { cn } from "@/lib/utils"

export default function SettingsPage() {
  const router = useRouter()
  const { resolvedTheme, setTheme } = useTheme()
  const [mounted, setMounted] = useState(false)

  const [name, setName] = useState("")
  const [email, setEmail] = useState("")
  const [profileLoaded, setProfileLoaded] = useState(false)
  const [saving, setSaving] = useState(false)

  const [notifPipelines, setNotifPipelines] = useState(true)
  const [notifSystem, setNotifSystem] = useState(false)

  const [currentPassword, setCurrentPassword] = useState("")
  const [newPassword, setNewPassword] = useState("")
  const [confirmPassword, setConfirmPassword] = useState("")
  const [showCurrentPw, setShowCurrentPw] = useState(false)
  const [showNewPw, setShowNewPw] = useState(false)
  const [showConfirmPw, setShowConfirmPw] = useState(false)
  const [changingPassword, setChangingPassword] = useState(false)

  useEffect(() => {
    setMounted(true)
    void (async () => {
      try {
        const res = await authFetch(API_ENDPOINTS.AUTH.ME)
        if (!res.ok) {
          router.push(`/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`)
          return
        }
        const data = await res.json() as { name?: string; email: string }
        setName(data.name ?? "")
        setEmail(data.email)
        setProfileLoaded(true)
      } catch {
        router.push(`/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`)
      }
    })()
  }, [router])

  const initials = !profileLoaded
    ? null
    : name
      ? name.split(" ").map((n) => n[0]).join("").toUpperCase()
      : email[0]?.toUpperCase() ?? "?"

  const handleSave = async () => {
    setSaving(true)
    try {
      const res = await authFetch(API_ENDPOINTS.AUTH.UPDATE_ME, {
        method: "PATCH",
        body: JSON.stringify({ name }),
      })
      if (!res.ok) {
        toast.error("Failed to save profile")
        return
      }
      toast.success("Profile updated")
    } catch {
      toast.error("Failed to save profile")
    } finally {
      setSaving(false)
    }
  }

  const handleChangePassword = async () => {
    if (newPassword !== confirmPassword) {
      toast.error("New passwords do not match")
      return
    }
    if (newPassword.length < 8) {
      toast.error("New password must be at least 8 characters")
      return
    }
    setChangingPassword(true)
    try {
      const res = await authFetch(API_ENDPOINTS.AUTH.CHANGE_PASSWORD, {
        method: "PATCH",
        body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
      })
      if (!res.ok) {
        const data = await res.json() as { error?: string }
        toast.error(data.error ?? "Failed to change password")
        return
      }
      toast.success("Password changed successfully")
      setCurrentPassword("")
      setNewPassword("")
      setConfirmPassword("")
    } catch {
      toast.error("Failed to change password")
    } finally {
      setChangingPassword(false)
    }
  }

  return (
    <div className="space-y-6 max-w-4xl">
      <PageHeader
        heading="Settings"
        description="Manage your account settings and preferences"
      />

      {/* Profile */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <User className="h-5 w-5 text-zinc-500" />
            Profile
          </CardTitle>
          <CardDescription>Your personal information</CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex items-center gap-6">
            <Avatar className="h-20 w-20">
              <AvatarImage src={undefined} alt={name} />
              <AvatarFallback className="text-xl bg-violet-100 text-violet-700">
                {initials ?? ""}
              </AvatarFallback>
            </Avatar>
            <div>
              <p className="text-xs text-zinc-500">Profile photo</p>
            </div>
          </div>

          <Separator />

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <label htmlFor="settings-name" className="text-sm font-medium">Full Name</label>
              <Input
                id="settings-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Enter your name"
              />
            </div>
            <div className="space-y-2">
              {/* htmlFor/id, not proximity: this input has no placeholder, so before the
                  association it had NO accessible name at all (axe `label`, critical). */}
              <label htmlFor="settings-email" className="text-sm font-medium">Email</label>
              <Input id="settings-email" value={email} disabled />
            </div>
          </div>

          <div className="flex justify-end">
            <Button onClick={() => void handleSave()} disabled={saving}>
              {saving ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Save className="h-4 w-4 mr-2" />
              )}
              {saving ? "Saving…" : "Save Changes"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Change Password */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Lock className="h-5 w-5 text-zinc-500" />
            Change Password
          </CardTitle>
          <CardDescription>Update your account password</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <label htmlFor="settings-current-password" className="text-sm font-medium">Current Password</label>
            <div className="relative">
              <Input
                id="settings-current-password"
                type={showCurrentPw ? "text" : "password"}
                value={currentPassword}
                onChange={(e) => setCurrentPassword(e.target.value)}
                placeholder="Enter current password"
                autoComplete="current-password"
              />
              <button
                type="button"
                aria-label={showCurrentPw ? "Hide current password" : "Show current password"}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-600"
                onClick={() => setShowCurrentPw((v) => !v)}
              >
                {showCurrentPw ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
              </button>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <label htmlFor="settings-new-password" className="text-sm font-medium">New Password</label>
              <div className="relative">
                <Input
                  id="settings-new-password"
                  type={showNewPw ? "text" : "password"}
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  placeholder="At least 8 characters"
                  autoComplete="new-password"
                />
                <button
                  type="button"
                  aria-label={showNewPw ? "Hide new password" : "Show new password"}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-600"
                  onClick={() => setShowNewPw((v) => !v)}
                >
                  {showNewPw ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
            </div>
            <div className="space-y-2">
              <label htmlFor="settings-confirm-password" className="text-sm font-medium">Confirm New Password</label>
              <div className="relative">
                <Input
                  id="settings-confirm-password"
                  type={showConfirmPw ? "text" : "password"}
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  placeholder="Repeat new password"
                  autoComplete="new-password"
                />
                <button
                  type="button"
                  aria-label={showConfirmPw ? "Hide password confirmation" : "Show password confirmation"}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-600"
                  onClick={() => setShowConfirmPw((v) => !v)}
                >
                  {showConfirmPw ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
            </div>
          </div>

          <div className="flex justify-end">
            <Button
              onClick={() => void handleChangePassword()}
              disabled={changingPassword || !currentPassword || !newPassword || !confirmPassword}
            >
              {changingPassword ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Lock className="h-4 w-4 mr-2" />
              )}
              {changingPassword ? "Updating…" : "Update Password"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Notifications */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Bell className="h-5 w-5 text-zinc-500" />
            Notifications
          </CardTitle>
          <CardDescription>Configure how you receive notifications</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="font-medium">Pipeline Executions</p>
                <p className="text-sm text-zinc-500">Get notified when pipelines complete or fail</p>
              </div>
              {/* Radix renders a <button role="switch"> whose only child is the thumb —
                  no text, no associated label, so the name must come from aria-label. */}
              <Switch
                aria-label="Pipeline execution notifications"
                checked={notifPipelines}
                onCheckedChange={setNotifPipelines}
              />
            </div>
            <Separator />
            <div className="flex items-center justify-between">
              <div>
                <p className="font-medium">System Updates</p>
                <p className="text-sm text-zinc-500">Important updates and maintenance notifications</p>
              </div>
              <Switch
                aria-label="System update notifications"
                checked={notifSystem}
                onCheckedChange={setNotifSystem}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Appearance */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Palette className="h-5 w-5 text-zinc-500" />
            Appearance
          </CardTitle>
          <CardDescription>Customize the look and feel</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex items-center justify-between">
            <div>
              <p className="font-medium">Theme</p>
              <p className="text-sm text-zinc-500">Select your preferred theme</p>
            </div>
            {mounted && (
              <div className="flex gap-2">
                {(["light", "dark", "system"] as const).map((t) => (
                  <Button
                    key={t}
                    variant="outline"
                    size="sm"
                    className={cn(
                      resolvedTheme === t || (t === "system" && !["light", "dark"].includes(resolvedTheme ?? ""))
                        ? "border-violet-500 bg-violet-50 text-violet-700 dark:bg-violet-950 dark:text-violet-300"
                        : ""
                    )}
                    onClick={() => setTheme(t)}
                  >
                    {t.charAt(0).toUpperCase() + t.slice(1)}
                  </Button>
                ))}
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
