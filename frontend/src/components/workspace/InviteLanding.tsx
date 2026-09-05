"use client"

import { useCallback, useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import Link from "next/link"
import { Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { toast } from "sonner"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { setActiveWorkspaceId } from "@/lib/workspace/active-workspace"

// P5b-UI #3: the /invite/[token] landing page body. The preview is PUBLIC (an
// invitee inspects the workspace before authenticating); accept is AUTHED and
// email-bound server-side. We mirror only what improves UX:
//   - logged out  → login/signup CTAs carrying ?next back here so they return
//   - logged in, matching email → Accept (joins, marks active, goes home)
//   - logged in, different email → block + offer an account switch (the backend
//     would 403; don't make the user discover that by clicking)
// The backend remains authoritative for every guard; these are convenience only.

interface InvitePreview {
  workspace_id: string
  workspace_name: string
  email: string
  role: string
  status: string
  expires_at: string
}

interface CurrentUser {
  user_id: string
  email: string
}

function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-zinc-50 to-zinc-100 dark:from-zinc-900 dark:to-zinc-800 p-4">
      <Card className="w-full max-w-md">{children}</Card>
    </div>
  )
}

export default function InviteLanding({ token }: { token: string }) {
  const router = useRouter()
  const [loading, setLoading] = useState(true)
  const [preview, setPreview] = useState<InvitePreview | null>(null)
  const [previewError, setPreviewError] = useState<string | null>(null)
  const [currentUser, setCurrentUser] = useState<CurrentUser | null>(null)
  const [loggedIn, setLoggedIn] = useState(false)
  const [accepting, setAccepting] = useState(false)

  const returnPath = `/invite/${token}`

  const loadPreview = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.INVITE_PREVIEW(token))
      if (res.ok) {
        setPreview((await res.json()) as InvitePreview)
        setPreviewError(null)
        return
      }
      const body = (await res.json().catch(() => ({}))) as { error?: string; status?: string }
      setPreview(null)
      if (res.status === 404) {
        setPreviewError("This invitation link is invalid or no longer exists.")
      } else if (res.status === 410) {
        const expired = body?.status === "expired" || body?.error === "invite has expired"
        setPreviewError(expired ? "This invitation has expired." : "This invitation is no longer valid.")
      } else {
        setPreviewError("Couldn't load this invitation. Please try again.")
      }
    } catch {
      setPreviewError("Couldn't load this invitation. Please try again.")
    }
  }, [token])

  const loadMe = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.AUTH.ME)
      if (res.ok) {
        const data = (await res.json()) as { user_id: string; email: string }
        setCurrentUser({ user_id: data.user_id, email: data.email })
        setLoggedIn(true)
        return
      }
    } catch {
      /* fall through to logged-out */
    }
    setCurrentUser(null)
    setLoggedIn(false)
  }, [])

  useEffect(() => {
    setLoading(true)
    void Promise.all([loadPreview(), loadMe()]).finally(() => setLoading(false))
  }, [loadPreview, loadMe])

  const accept = useCallback(async () => {
    if (accepting) return // a claim is already in flight — ignore (single-use token)
    setAccepting(true)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.INVITE_ACCEPT(token), { method: "POST" })
      if (res.ok) {
        const data = (await res.json()) as { workspace_id: string }
        // Make the freshly joined workspace active so the dashboard lands there.
        setActiveWorkspaceId(data.workspace_id)
        toast.success(`You've joined ${preview?.workspace_name ?? "the workspace"}.`)
        router.push("/")
        return
      }
      const body = (await res.json().catch(() => ({}))) as { error?: string }
      const msg =
        res.status === 403
          ? "This invite was issued to a different email address."
          : res.status === 409
            ? "This invitation has already been used or revoked."
            : res.status === 410
              ? "This invitation has expired."
              : res.status === 404
                ? "This invitation link is invalid or no longer exists."
                : typeof body?.error === "string"
                  ? body.error
                  : "Couldn't accept the invitation."
      toast.error(msg)
      // A terminal invite state means our preview is stale — reflect it so the
      // page stops offering an Accept that can never succeed.
      if (res.status === 404 || res.status === 409 || res.status === 410) {
        setPreview(null)
        setPreviewError(msg)
      }
    } catch {
      toast.error("Couldn't accept the invitation.")
    } finally {
      setAccepting(false)
    }
  }, [accepting, token, preview, router])

  if (loading) {
    return (
      <Shell>
        <CardContent className="flex items-center justify-center py-16">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </CardContent>
      </Shell>
    )
  }

  if (previewError || !preview) {
    return (
      <Shell>
        <CardHeader className="space-y-1">
          <CardTitle className="text-2xl font-bold text-center">Invitation unavailable</CardTitle>
          <CardDescription className="text-center text-red-600">
            {previewError ?? "This invitation could not be loaded."}
          </CardDescription>
        </CardHeader>
        <CardContent className="text-center">
          <Button asChild variant="outline">
            <Link href="/login">Go to login</Link>
          </Button>
        </CardContent>
      </Shell>
    )
  }

  const emailMismatch =
    loggedIn &&
    currentUser !== null &&
    currentUser.email.trim().toLowerCase() !== preview.email.trim().toLowerCase()

  return (
    <Shell>
      <CardHeader className="space-y-2 text-center">
        <CardTitle className="text-2xl font-bold">You&apos;re invited</CardTitle>
        <CardDescription>
          Join <span className="font-semibold text-foreground">{preview.workspace_name}</span> on Rsync
        </CardDescription>
        <div className="flex justify-center pt-1">
          <Badge variant="secondary" className="text-sm">
            Invited as {preview.role}
          </Badge>
        </div>
        <p className="text-xs text-muted-foreground">
          Invitation for <span className="font-medium text-foreground">{preview.email}</span>
        </p>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {loggedIn ? (
          emailMismatch ? (
            <>
              <p className="text-center text-sm text-muted-foreground">
                You&apos;re signed in as{" "}
                <span className="font-medium text-foreground">{currentUser?.email}</span>, but this invite
                was sent to <span className="font-medium text-foreground">{preview.email}</span>.
              </p>
              <Button asChild variant="outline" className="w-full">
                <Link href={`/logout?next=${encodeURIComponent(returnPath)}`}>
                  Sign in with a different account
                </Link>
              </Button>
            </>
          ) : (
            <Button
              className="w-full bg-gradient-to-r from-violet-600 to-indigo-600"
              onClick={() => void accept()}
              disabled={accepting}
            >
              {accepting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Joining…
                </>
              ) : (
                "Accept invitation"
              )}
            </Button>
          )
        ) : (
          <>
            <Button asChild className="w-full bg-gradient-to-r from-violet-600 to-indigo-600">
              <Link href={`/login?next=${encodeURIComponent(returnPath)}`}>Log in to accept</Link>
            </Button>
            <Button asChild variant="outline" className="w-full">
              <Link href={`/signup?next=${encodeURIComponent(returnPath)}`}>Create an account</Link>
            </Button>
          </>
        )}
      </CardContent>
    </Shell>
  )
}
