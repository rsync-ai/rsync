"use client"

import { useState, useEffect } from "react"
import { useRouter, useSearchParams } from "next/navigation"
import Link from "next/link"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Mail, Loader2 } from "lucide-react"
import { toast } from "sonner"
import type { InviteValidation } from "@/lib/api/admin"
import { safeNextPath, POST_VERIFY_NEXT_KEY } from "@/lib/auth/safe-next"
import { API_GATEWAY_URL } from "@/lib/config/api"

// Self-correcting base URL (see @/lib/config/api): a mis-baked localhost value
// is ignored in favor of the current origin on a real deployment.
const API_URL = API_GATEWAY_URL

const COUNTRIES = [
  { name: "United States", code: "US", dial: "+1" },
  { name: "United Kingdom", code: "GB", dial: "+44" },
  { name: "Canada", code: "CA", dial: "+1" },
  { name: "Australia", code: "AU", dial: "+61" },
  { name: "Germany", code: "DE", dial: "+49" },
  { name: "France", code: "FR", dial: "+33" },
  { name: "India", code: "IN", dial: "+91" },
  { name: "Japan", code: "JP", dial: "+81" },
  { name: "Brazil", code: "BR", dial: "+55" },
  { name: "Mexico", code: "MX", dial: "+52" },
  { name: "Netherlands", code: "NL", dial: "+31" },
  { name: "Spain", code: "ES", dial: "+34" },
  { name: "Italy", code: "IT", dial: "+39" },
  { name: "Sweden", code: "SE", dial: "+46" },
  { name: "Norway", code: "NO", dial: "+47" },
  { name: "Denmark", code: "DK", dial: "+45" },
  { name: "Finland", code: "FI", dial: "+358" },
  { name: "Switzerland", code: "CH", dial: "+41" },
  { name: "Austria", code: "AT", dial: "+43" },
  { name: "Belgium", code: "BE", dial: "+32" },
  { name: "Portugal", code: "PT", dial: "+351" },
  { name: "Poland", code: "PL", dial: "+48" },
  { name: "Singapore", code: "SG", dial: "+65" },
  { name: "South Korea", code: "KR", dial: "+82" },
  { name: "China", code: "CN", dial: "+86" },
  { name: "Israel", code: "IL", dial: "+972" },
  { name: "South Africa", code: "ZA", dial: "+27" },
  { name: "Nigeria", code: "NG", dial: "+234" },
  { name: "Argentina", code: "AR", dial: "+54" },
  { name: "Chile", code: "CL", dial: "+56" },
  { name: "Colombia", code: "CO", dial: "+57" },
  { name: "New Zealand", code: "NZ", dial: "+64" },
  { name: "Ireland", code: "IE", dial: "+353" },
  { name: "United Arab Emirates", code: "AE", dial: "+971" },
]

const EMPLOYEE_RANGES = [
  "1–10",
  "11–50",
  "51–200",
  "201–500",
  "501–1,000",
  "1,001–5,000",
  "5,001+",
]

export default function SignupClient() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const inviteToken = searchParams.get("invite") || ""
  // A safe return path (e.g. set by the /invite/[token] landing page) so a new
  // user is sent back to finish what they came for. Same guard login uses:
  // only honor app-relative paths, never an absolute/external URL.
  const next = searchParams.get("next")

  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [confirmPassword, setConfirmPassword] = useState("")
  const [firstName, setFirstName] = useState("")
  const [lastName, setLastName] = useState("")
  const [company, setCompany] = useState("")
  const [phone, setPhone] = useState("")
  const [country, setCountry] = useState("")
  const [employeeRange, setEmployeeRange] = useState("")
  const [loading, setLoading] = useState(false)
  // After signup, if email needs verification, show "check your inbox" screen
  const [pendingVerification, setPendingVerification] = useState(false)
  const [registeredEmail, setRegisteredEmail] = useState("")
  const [resending, setResending] = useState(false)

  // Invite validation state
  const [inviteValid, setInviteValid] = useState<boolean | null>(null)
  const [inviteData, setInviteData] = useState<InviteValidation | null>(null)
  const [inviteChecking, setInviteChecking] = useState(false)

  // Validate invite token on mount
  useEffect(() => {
    if (!inviteToken) return
    setInviteChecking(true)
    fetch(`${API_URL}/api/v1/auth/invite/${inviteToken}`)
      .then((res) => res.json())
      .then((data: InviteValidation) => {
        setInviteData(data)
        setInviteValid(data.valid)
        if (data.valid && data.email_hint) {
          setEmail(data.email_hint)
        }
      })
      .catch(() => {
        setInviteValid(false)
        setInviteData({ valid: false, reason: "network_error" })
      })
      .finally(() => setInviteChecking(false))
  }, [inviteToken])

  const handleSignup = async (e: React.FormEvent) => {
    e.preventDefault()

    if (password !== confirmPassword) {
      toast.error("Passwords do not match")
      return
    }

    if (password.length < 8) {
      toast.error("Password must be at least 8 characters")
      return
    }

    setLoading(true)

    try {
      const url = inviteToken
        ? `${API_URL}/api/v1/auth/register?invite=${encodeURIComponent(inviteToken)}`
        : `${API_URL}/api/v1/auth/register`

      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          email,
          password,
          first_name: firstName,
          last_name: lastName,
          company,
          phone,
          country,
          employee_range: employeeRange,
        }),
      })

      const data = await response.json()

      if (!response.ok) {
        // `error` is a machine code the client branches on ("invite_required",
        // "registration_unavailable"); `message` is the sentence written for the
        // person reading it. Prefer the sentence, and fall back to the code only
        // when the API sent nothing else.
        throw new Error(data.message || data.error || "Registration failed")
      }

      if (data.email_verified === false) {
        // Email verification required — show the "check your inbox" screen. Stash a
        // safe return path so the verify-email page can send the user back here
        // (e.g. to an invite) instead of stranding them at the dashboard.
        const safeNext = safeNextPath(next)
        if (safeNext !== "/") {
          try {
            sessionStorage.setItem(POST_VERIFY_NEXT_KEY, safeNext)
          } catch {
            /* sessionStorage unavailable — degrade to the default post-verify dest */
          }
        }
        setRegisteredEmail(data.email || email)
        setPendingVerification(true)
        toast.success("Account created! Please verify your email to continue.")
      } else {
        toast.success("Account created successfully!")
        router.push(safeNextPath(next))
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Registration failed")
    } finally {
      setLoading(false)
    }
  }

  const handleResendVerification = async () => {
    setResending(true)
    try {
      const response = await fetch(`${API_URL}/api/v1/auth/resend-verification`, {
        method: "POST",
        credentials: "include",
      })
      const data = await response.json()
      if (response.ok) {
        toast.success("Verification email resent! Check your inbox.")
      } else {
        toast.error(data.message || "Failed to resend. Please try again.")
      }
    } catch {
      toast.error("Something went wrong. Please try again.")
    } finally {
      setResending(false)
    }
  }

  // Show "check your inbox" screen after registration when verification is required
  if (pendingVerification) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-zinc-50 to-zinc-100 dark:from-zinc-900 dark:to-zinc-800 p-4">
        <Card className="w-full max-w-md">
          <CardHeader className="space-y-1 text-center">
            <div className="flex justify-center mb-2">
              <Mail className="h-12 w-12 text-indigo-500" />
            </div>
            <CardTitle className="text-2xl font-bold">Check your email</CardTitle>
            <CardDescription>
              We&apos;ve sent a verification link to{" "}
              <span className="font-medium text-zinc-800 dark:text-zinc-200">{registeredEmail}</span>.
              Click the link to activate your account.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <p className="text-center text-sm text-zinc-500">
              The link expires in 24 hours. Check your spam folder if you don&apos;t see it.
            </p>
            <Button
              variant="outline"
              className="w-full"
              onClick={handleResendVerification}
              disabled={resending}
            >
              {resending ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Sending…
                </>
              ) : (
                "Resend verification email"
              )}
            </Button>
            <div className="text-center text-sm text-muted-foreground">
              <Link href="/login" className="text-violet-600 hover:underline font-medium">
                Back to login
              </Link>
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  // Show error for invalid invite
  if (inviteToken && inviteValid === false && !inviteChecking) {
    const reasonText: Record<string, string> = {
      expired: "This invitation has expired.",
      used: "This invitation has already been used.",
      not_found: "This invitation link is invalid.",
      network_error: "Unable to verify invitation. Please try again.",
    }
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-zinc-50 to-zinc-100 dark:from-zinc-900 dark:to-zinc-800 p-4">
        <Card className="w-full max-w-md">
          <CardHeader className="space-y-1">
            <CardTitle className="text-2xl font-bold text-center">Invalid Invitation</CardTitle>
            <CardDescription className="text-center text-red-600">
              {reasonText[inviteData?.reason || ""] || "This invitation is no longer valid."}
            </CardDescription>
          </CardHeader>
          <CardContent className="text-center">
            <Link href="/login">
              <Button variant="outline">Go to Login</Button>
            </Link>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-zinc-50 to-zinc-100 dark:from-zinc-900 dark:to-zinc-800 p-4">
      <Card className="w-full max-w-lg">
        <CardHeader className="space-y-1">
          <CardTitle className="text-2xl font-bold text-center">Create an account</CardTitle>
          <CardDescription className="text-center">Enter your details to get started</CardDescription>
          {inviteToken && inviteValid && inviteData && (
            <div className="flex justify-center pt-2">
              <Badge variant="secondary" className="text-sm">
                Invited as {inviteData.role}
              </Badge>
            </div>
          )}
        </CardHeader>
        <CardContent>
          {inviteChecking ? (
            <div className="text-center py-8 text-sm text-zinc-500">Validating invite...</div>
          ) : (
            <form onSubmit={handleSignup} className="space-y-4">
              {/* First + Last Name */}
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="firstName">First Name</Label>
                  <Input
                    id="firstName"
                    type="text"
                    placeholder="Jane"
                    value={firstName}
                    onChange={(e) => setFirstName(e.target.value)}
                    autoComplete="given-name"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="lastName">Last Name</Label>
                  <Input
                    id="lastName"
                    type="text"
                    placeholder="Smith"
                    value={lastName}
                    onChange={(e) => setLastName(e.target.value)}
                    autoComplete="family-name"
                  />
                </div>
              </div>

              {/* Company + Employee Range */}
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label htmlFor="company">Company</Label>
                  <Input
                    id="company"
                    type="text"
                    placeholder="Acme Corp"
                    value={company}
                    onChange={(e) => setCompany(e.target.value)}
                    autoComplete="organization"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Employee Range</Label>
                  <Select value={employeeRange} onValueChange={setEmployeeRange}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select" />
                    </SelectTrigger>
                    <SelectContent>
                      {EMPLOYEE_RANGES.map((r) => (
                        <SelectItem key={r} value={r}>{r}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {/* Work Email */}
              <div className="space-y-2">
                <Label htmlFor="email">Work Email</Label>
                <Input
                  id="email"
                  type="email"
                  placeholder="you@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                  autoComplete="email"
                />
              </div>

              {/* Country + Phone */}
              <div className="grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  <Label>Country</Label>
                  <Select value={country} onValueChange={setCountry}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select" />
                    </SelectTrigger>
                    <SelectContent>
                      {COUNTRIES.map((c) => (
                        <SelectItem key={c.code} value={c.code}>
                          {c.name} ({c.dial})
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="phone">Phone</Label>
                  <Input
                    id="phone"
                    type="tel"
                    placeholder="555 000 0000"
                    value={phone}
                    onChange={(e) => setPhone(e.target.value)}
                    autoComplete="tel"
                  />
                </div>
              </div>

              {/* Password */}
              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <Input
                  id="password"
                  type="password"
                  placeholder="••••••••"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  autoComplete="new-password"
                  minLength={8}
                />
                <p className="text-xs text-zinc-500">Minimum 8 characters</p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="confirmPassword">Confirm Password</Label>
                <Input
                  id="confirmPassword"
                  type="password"
                  placeholder="••••••••"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  required
                  autoComplete="new-password"
                />
              </div>

              <Button type="submit" className="w-full bg-gradient-to-r from-violet-600 to-indigo-600" disabled={loading}>
                {loading ? "Creating account..." : "Create account"}
              </Button>

              <p className="text-center text-xs text-zinc-500 dark:text-zinc-400">
                By creating an account, you agree to our{" "}
                <a
                  href="https://rsync.ai/terms"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-violet-600 hover:underline"
                >
                  Terms of Service
                </a>{" "}
                and{" "}
                <a
                  href="https://rsync.ai/privacy"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-violet-600 hover:underline"
                >
                  Privacy Policy
                </a>
                .
              </p>
            </form>
          )}

          <div className="mt-4 text-center text-sm">
            Already have an account?{" "}
            <Link href="/login" className="text-violet-600 hover:underline font-medium">
              Sign in
            </Link>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
