"use client"

import { Suspense, useState } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { toast } from "sonner"
import { safeNextPath } from "@/lib/auth/safe-next"
import { API_GATEWAY_URL } from "@/lib/config/api"

// Self-correcting base URL (see @/lib/config/api): on a real origin a mis-baked
// localhost value is ignored in favor of the current origin, so this can never
// leak a localhost link to the browser.
const API_URL = API_GATEWAY_URL
const IS_DEV = process.env.NODE_ENV !== "production"

function LoginForm() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [loading, setLoading] = useState(false)

  const useDevCredentials = () => {
    if (!IS_DEV) return
    setEmail("default@rsync-ai.local")
    setPassword("password123")
  }

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)

    try {
      const normalizedEmail = email.trim().toLowerCase()
      // NOTE: We intentionally trim surrounding whitespace from the password to avoid copy/paste issues in dev.
      // If you need leading/trailing spaces as a real password, remove the trim.
      const normalizedPassword = password.trim()

      const response = await fetch(`${API_URL}/api/v1/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ email: normalizedEmail, password: normalizedPassword }),
      })

      const data = await response.json()

      if (!response.ok) {
        // Improve local-dev UX: if auth fails, hint the default seeded credentials.
        const msg = data?.error || "Login failed"
        if (IS_DEV && response.status === 401) {
          throw new Error(`${msg}. Dev default: default@rsync-ai.local / password123`)
        }
        throw new Error(msg)
      }

      toast.success("Login successful!")
      router.push(safeNextPath(searchParams.get("next")))
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Login failed")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-zinc-50 to-zinc-100 dark:from-zinc-900 dark:to-zinc-800 p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-1">
          <CardTitle className="text-2xl font-bold text-center">Welcome to Rsync</CardTitle>
          <CardDescription className="text-center">
            Enter your credentials to access your account
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleLogin} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                placeholder="you@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
                autoComplete="email"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <Input
                id="password"
                type="password"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                autoComplete="current-password"
              />
            </div>
            {IS_DEV && (
              <div className="rounded-md border border-zinc-200 bg-zinc-50 p-3 text-sm text-zinc-700 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-300">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="font-medium">Local dev quick login</div>
                    <div className="text-xs text-zinc-500 dark:text-zinc-400">
                      Uses the seeded dev user: <span className="font-mono">default@rsync-ai.local</span> /{" "}
                      <span className="font-mono">password123</span>
                    </div>
                  </div>
                  <Button type="button" variant="outline" onClick={useDevCredentials} disabled={loading}>
                    Use
                  </Button>
                </div>
              </div>
            )}
            <Button
              type="submit"
              className="w-full bg-gradient-to-r from-violet-600 to-indigo-600"
              disabled={loading}
            >
              {loading ? "Signing in..." : "Sign in"}
            </Button>
          </form>

          <div className="mt-4 text-center text-sm">
            Don't have an account?{" "}
            <Link href="/signup" className="text-violet-600 hover:underline font-medium">
              Sign up
            </Link>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

export default function LoginPage() {
  return (
    <Suspense>
      <LoginForm />
    </Suspense>
  )
}
