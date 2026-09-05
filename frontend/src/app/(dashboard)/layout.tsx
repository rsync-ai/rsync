import { redirect } from "next/navigation"
import { Providers } from "@/components/providers"
import { Sidebar } from "@/components/layout/Sidebar"
import { Header } from "@/components/layout/Header"
import { DashboardShell } from "@/components/layout/DashboardShell"
import { EmailVerificationBanner } from "@/components/layout/EmailVerificationBanner"
import { PlanBanner } from "@/components/layout/PlanBanner"
import { GlobalConnectorModal } from "@/components/modal/GlobalConnectorModal"
import { getCurrentUser } from "@/lib/api/auth-fetch"

export default async function DashboardLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const user = await getCurrentUser()
  if (!user) {
    // Session missing or invalid (gateway returned 401). Route through /logout, NOT
    // /login: /logout clears the (possibly stale) HttpOnly `auth_token` cookie via the
    // API gateway, then lands on /login. Redirecting straight to /login would loop
    // forever — middleware bounces /login -> / whenever an auth_token cookie is merely
    // *present* (it can't validate it), and this layout bounces / -> back, so a stale
    // cookie produces ERR_TOO_MANY_REDIRECTS. /logout is the one path middleware lets
    // through with a cookie present (see middleware.ts), which breaks the loop.
    redirect("/logout")
  }

  return (
    <Providers>
      <div className="relative min-h-screen">
        <Sidebar role={user.role} />
        <Header user={{ email: user.email, name: user.name, avatarUrl: undefined }} />
        <DashboardShell>
          <EmailVerificationBanner />
          <PlanBanner />
          {children}
        </DashboardShell>
        <GlobalConnectorModal />
      </div>
    </Providers>
  )
}
