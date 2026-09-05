"use client"

import { useEffect, useState } from "react"
import { MailWarning, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { authFetch } from "@/lib/api/auth-fetch"
import { toast } from "sonner"

export function EmailVerificationBanner() {
  const [showBanner, setShowBanner] = useState(false)
  const [sending, setSending] = useState(false)
  const [dismissed, setDismissed] = useState(false)

  useEffect(() => {
    async function checkVerification() {
      try {
        const res = await authFetch("/api/v1/auth/me")
        if (!res.ok) return
        const data = await res.json() as { email_verified?: boolean }
        if (data.email_verified === false) {
          setShowBanner(true)
        }
      } catch {
        // non-critical — silently ignore
      }
    }
    void checkVerification()
  }, [])

  const handleResend = async () => {
    setSending(true)
    try {
      const res = await authFetch("/api/v1/auth/resend-verification", { method: "POST" })
      if (res.ok) {
        toast.success("Verification email sent — check your inbox")
      } else {
        const err = await res.json().catch(() => ({})) as { error?: string }
        toast.error(err.error ?? "Failed to send verification email")
      }
    } catch {
      toast.error("Failed to send verification email")
    } finally {
      setSending(false)
    }
  }

  if (!showBanner || dismissed) return null

  return (
    <div className="sticky top-16 z-20 -mx-4 sm:-mx-6 -mt-4 sm:-mt-6 mb-4 flex items-center justify-between gap-4 border-b border-amber-200 bg-amber-50 px-4 py-2.5 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-900/30 dark:text-amber-200">
      <div className="flex min-w-0 items-center gap-2">
        <MailWarning className="h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
        <span>
          Please verify your email — check your inbox or{" "}
          <button
            onClick={() => void handleResend()}
            disabled={sending}
            className="font-medium underline hover:text-amber-900 disabled:opacity-50 dark:hover:text-amber-100"
          >
            {sending ? "Sending…" : "Resend verification email"}
          </button>
        </span>
      </div>
      <Button
        variant="ghost"
        size="icon-sm"
        className="shrink-0 text-amber-700 hover:bg-amber-100 hover:text-amber-900 dark:text-amber-300 dark:hover:bg-amber-800"
        onClick={() => setDismissed(true)}
        aria-label="Dismiss"
      >
        <X className="h-4 w-4" />
      </Button>
    </div>
  )
}
