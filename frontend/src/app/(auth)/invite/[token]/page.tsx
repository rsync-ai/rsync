import { Suspense } from "react"
import InviteLanding from "@/components/workspace/InviteLanding"

// /invite/[token] — a workspace invitee lands here from an invite link. It sits in
// the (auth) group (NOT the dashboard auth gate) because an unauthenticated invitee
// must be able to preview the workspace and be routed to login/signup. Middleware
// leaves /invite/* alone (neither public-bounce nor protected-redirect).
export default async function InvitePage({ params }: { params: Promise<{ token: string }> }) {
  const { token } = await params
  return (
    <Suspense
      fallback={
        <div className="min-h-screen flex items-center justify-center text-sm text-muted-foreground">
          Loading…
        </div>
      }
    >
      <InviteLanding token={token} />
    </Suspense>
  )
}
