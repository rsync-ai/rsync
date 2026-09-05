"use client"

import { useEffect, useState } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import { Suspense } from "react"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { CheckCircle, XCircle, Loader2, Mail } from "lucide-react"
import { toast } from "sonner"
import { safeNextPath, POST_VERIFY_NEXT_KEY } from "@/lib/auth/safe-next"
import { API_GATEWAY_URL } from "@/lib/config/api"

// Self-correcting base URL (see @/lib/config/api): a mis-baked localhost value
// is ignored in favor of the current origin on a real deployment.
const API_URL = API_GATEWAY_URL

type VerifyState = "loading" | "success" | "error" | "expired" | "resent"

function VerifyEmailContent() {
  const searchParams = useSearchParams()
  const router = useRouter()
  const token = searchParams.get("token")

  const [state, setState] = useState<VerifyState>(token ? "loading" : "error")
  const [errorMessage, setErrorMessage] = useState<string>("")
  const [resending, setResending] = useState(false)
  // Where to send the user after a successful verify. Defaults to the dashboard;
  // an invite flow stashes a safe return path (see signup-client + safe-next).
  const [postVerifyNext, setPostVerifyNext] = useState<string>("/")

  useEffect(() => {
    if (!token) {
      setErrorMessage("No verification token found in the URL.")
      setState("error")
      return
    }

    const verify = async () => {
      try {
        const response = await fetch(
          `${API_URL}/api/v1/auth/verify-email?token=${encodeURIComponent(token)}`,
          { method: "GET", credentials: "include" }
        )
        const data = await response.json()

        if (response.ok) {
          // Consume a stashed return path (best-effort, same browser) so an invitee
          // lands back on their invite rather than the dashboard.
          let dest = "/"
          try {
            const stored = sessionStorage.getItem(POST_VERIFY_NEXT_KEY)
            if (stored) {
              dest = safeNextPath(stored)
              sessionStorage.removeItem(POST_VERIFY_NEXT_KEY)
            }
          } catch {
            /* sessionStorage unavailable — default to the dashboard */
          }
          setPostVerifyNext(dest)
          setState("success")
          return
        }

        if (data.error === "token_expired") {
          setState("expired")
          setErrorMessage(data.message || "Verification link has expired.")
        } else {
          setState("error")
          setErrorMessage(data.message || "Invalid or already-used verification link.")
        }
      } catch {
        setState("error")
        setErrorMessage("Something went wrong. Please try again.")
      }
    }

    verify()
  }, [token])

  const handleResend = async () => {
    setResending(true)
    try {
      const response = await fetch(`${API_URL}/api/v1/auth/resend-verification`, {
        method: "POST",
        credentials: "include",
      })
      const data = await response.json()

      if (response.ok) {
        setState("resent")
        toast.success("Verification email sent! Check your inbox.")
      } else if (response.status === 401) {
        toast.error("Please log in first, then request a new verification email.")
        router.push(`/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`)
      } else {
        toast.error(data.message || "Failed to resend. Please try again.")
      }
    } catch {
      toast.error("Something went wrong. Please try again.")
    } finally {
      setResending(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-zinc-50 to-zinc-100 dark:from-zinc-900 dark:to-zinc-800 p-4">
      <Card className="w-full max-w-md">
        {state === "loading" && (
          <>
            <CardHeader className="space-y-1 text-center">
              <div className="flex justify-center mb-2">
                <Loader2 className="h-12 w-12 text-indigo-500 animate-spin" />
              </div>
              <CardTitle className="text-2xl font-bold">Verifying your email…</CardTitle>
              <CardDescription>Please wait while we confirm your address.</CardDescription>
            </CardHeader>
          </>
        )}

        {state === "success" && (
          <>
            <CardHeader className="space-y-1 text-center">
              <div className="flex justify-center mb-2">
                <CheckCircle className="h-12 w-12 text-green-500" />
              </div>
              <CardTitle className="text-2xl font-bold">Email verified!</CardTitle>
              <CardDescription>
                Your email has been verified. You now have full access to rsync-ai.
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <Button
                className="w-full bg-gradient-to-r from-violet-600 to-indigo-600"
                onClick={() => router.push(postVerifyNext)}
              >
                {postVerifyNext === "/" ? "Go to Dashboard" : "Continue"}
              </Button>
            </CardContent>
          </>
        )}

        {(state === "error" || state === "expired") && (
          <>
            <CardHeader className="space-y-1 text-center">
              <div className="flex justify-center mb-2">
                <XCircle className="h-12 w-12 text-red-500" />
              </div>
              <CardTitle className="text-2xl font-bold">
                {state === "expired" ? "Link expired" : "Verification failed"}
              </CardTitle>
              <CardDescription>{errorMessage}</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <Button
                variant="default"
                className="w-full bg-gradient-to-r from-violet-600 to-indigo-600"
                onClick={handleResend}
                disabled={resending}
              >
                {resending ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Sending…
                  </>
                ) : (
                  <>
                    <Mail className="mr-2 h-4 w-4" />
                    Resend verification email
                  </>
                )}
              </Button>
              <div className="text-center text-sm text-muted-foreground">
                <Link href="/login" className="text-violet-600 hover:underline font-medium">
                  Back to login
                </Link>
              </div>
            </CardContent>
          </>
        )}

        {state === "resent" && (
          <>
            <CardHeader className="space-y-1 text-center">
              <div className="flex justify-center mb-2">
                <Mail className="h-12 w-12 text-indigo-500" />
              </div>
              <CardTitle className="text-2xl font-bold">Check your inbox</CardTitle>
              <CardDescription>
                We&apos;ve sent a new verification link. It expires in 24 hours.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-center text-sm text-muted-foreground">
                <Link href="/login" className="text-violet-600 hover:underline font-medium">
                  Back to login
                </Link>
              </div>
            </CardContent>
          </>
        )}
      </Card>
    </div>
  )
}

export default function VerifyEmailPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen flex items-center justify-center text-sm text-muted-foreground">
          Loading…
        </div>
      }
    >
      <VerifyEmailContent />
    </Suspense>
  )
}
