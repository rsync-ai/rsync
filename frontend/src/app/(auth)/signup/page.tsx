import { Suspense } from "react"
import SignupClient from "./signup-client"

export default function SignupPage() {
  return (
    <Suspense fallback={<div className="min-h-screen flex items-center justify-center text-sm text-muted-foreground">Loading…</div>}>
      <SignupClient />
    </Suspense>
  )
}
