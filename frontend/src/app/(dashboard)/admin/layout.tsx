import { cookies } from "next/headers"
import { redirect } from "next/navigation"

export default async function AdminLayout({ children }: { children: React.ReactNode }) {
  const authToken = (await cookies()).get("auth_token")?.value
  if (!authToken) {
    redirect("/login")
  }

  // Important: Do NOT do allowlist checks here. The API Gateway is the source of truth
  // and will return 403 for non-allowlisted users.
  return <>{children}</>
}

