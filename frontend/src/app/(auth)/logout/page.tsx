"use client"

import { Suspense, useEffect } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { API_GATEWAY_URL } from "@/lib/config/api"
import { clearAllWorkspaceScopes } from "@/lib/workspace/scoped-storage"

// Self-correcting base URL (see @/lib/config/api): a mis-baked localhost value
// is ignored in favor of the current origin on a real deployment.
const API_URL = API_GATEWAY_URL

function LogoutContent() {
  const router = useRouter()
  const searchParams = useSearchParams()

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        // Clear server-side session + cookies.
        await fetch(`${API_URL}/api/v1/auth/logout`, {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
        })
      } catch {
        // Ignore network failures; still clear client state and proceed.
      } finally {
        if (typeof window !== "undefined") {
          try {
            localStorage.removeItem("user_id")
            localStorage.removeItem("user_email")
            localStorage.removeItem("user_role")
            // Workspace-scoped keys: clear every workspace's slot, not just the
            // one that happened to be active (mirrors lib/auth.ts logout()).
            clearAllWorkspaceScopes("rsync_chat_session_id")
            clearAllWorkspaceScopes("rsync_chat_ui_v2_state")
            clearAllWorkspaceScopes("rsync_active_pipelines")
          } catch {
            // ignore
          }
        }
        if (!cancelled) {
          const next = searchParams.get("next")
          router.replace(next ? `/login?next=${encodeURIComponent(next)}` : "/login")
        }
      }
    })()
    return () => {
      cancelled = true
    }
  }, [router, searchParams])

  return <div className="min-h-screen flex items-center justify-center text-sm text-muted-foreground">Signing out…</div>
}

export default function LogoutPage() {
  return (
    <Suspense>
      <LogoutContent />
    </Suspense>
  )
}
