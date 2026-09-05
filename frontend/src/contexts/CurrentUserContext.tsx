"use client"

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react"
import { getCurrentUser, type CurrentUser } from "@/lib/api/auth-fetch"

/**
 * Shared client-side current-user context. Fetches /auth/me once so role-gated
 * UI (e.g. the connector-generation CTA, which the api-gateway restricts to
 * power_user/admin via PowerUserOrAdminMiddleware) can render the right
 * affordance instead of sending base users into a flow that 403s.
 *
 * The gateway still enforces every action — this is advisory UI state and fails
 * closed (empty role) until loaded, mirroring WorkspaceContext.
 */

interface CurrentUserContextValue {
  user: CurrentUser | null
  /** Platform tier from /auth/me: "user" | "power_user" | "admin" (or "viewer" fallback). */
  role: string
  isLoading: boolean
}

// Fail closed when no provider is mounted (isolated component / public page):
// no role, still "loading" so gated UI doesn't flash an allowed state.
const DEFAULT_VALUE: CurrentUserContextValue = {
  user: null,
  role: "",
  isLoading: true,
}

const CurrentUserContext = createContext<CurrentUserContextValue>(DEFAULT_VALUE)

export function CurrentUserProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<CurrentUser | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    getCurrentUser()
      .then((u) => {
        if (!cancelled) setUser(u)
      })
      .catch(() => {
        if (!cancelled) setUser(null)
      })
      .finally(() => {
        if (!cancelled) setIsLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const value = useMemo<CurrentUserContextValue>(
    () => ({ user, role: user?.role ?? "", isLoading }),
    [user, isLoading],
  )

  return <CurrentUserContext.Provider value={value}>{children}</CurrentUserContext.Provider>
}

export function useCurrentUser(): CurrentUserContextValue {
  return useContext(CurrentUserContext)
}

/**
 * Workspace roles allowed to run LLM connector generation: owner or admin.
 * Mirrors the api-gateway WorkspaceGeneratorMiddleware gate (workspace role
 * >= admin). Connector generation is a TENANT capability — every self-serve
 * signup owns their personal workspace — so it is gated on the WORKSPACE axis,
 * NOT the global user|power_user|admin platform-staff tier. Pass the caller's
 * active-workspace role (from useWorkspaceRole), not the global user role.
 * Fails closed for member/viewer/unknown.
 */
export function canGenerateConnectors(workspaceRole: string | null | undefined): boolean {
  const r = String(workspaceRole ?? "").trim().toLowerCase()
  return r === "owner" || r === "admin"
}
