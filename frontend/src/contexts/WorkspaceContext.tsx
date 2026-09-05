"use client"

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import {
  getActiveWorkspaceId,
  onActiveWorkspaceChange,
  setActiveWorkspaceId,
} from "@/lib/workspace/active-workspace"
import { can as canRole, meetsRole, type WorkspaceAction, type WorkspaceRole } from "@/lib/workspace/roles"

/**
 * Shared client-side workspace context — the single place that fetches the caller's
 * workspaces and tracks the active selection, so the header switcher, role-gated
 * controls, and the settings hub all agree (previously each re-fetched /workspaces
 * and re-derived the role independently). The api-gateway still enforces every
 * mutation; this is advisory UI state.
 */

export interface WorkspaceSummary {
  id: string
  name: string
  slug?: string
  role: string
  /** Personal (identity-anchor) workspace — can't be deleted or left. */
  is_personal?: boolean
}

interface WorkspaceContextValue {
  workspaces: WorkspaceSummary[]
  activeWorkspace: WorkspaceSummary | null
  /** The caller's role in the active workspace ("" when none/unknown — fails closed). */
  role: string
  isLoading: boolean
  error: boolean
  /** Re-fetch the workspace list (e.g. after creating/deleting one). */
  refresh: () => Promise<void>
  /** Whether the caller may perform `action` in the active workspace (fail-closed). */
  can: (action: WorkspaceAction) => boolean
  /** Whether the caller's active-workspace role is at least `min` (fail-closed). */
  meets: (min: WorkspaceRole) => boolean
}

// Default value used when no provider is mounted: fail closed (no role, no
// permissions) and not loading, so an isolated component renders safely instead of
// throwing.
const DEFAULT_VALUE: WorkspaceContextValue = {
  workspaces: [],
  activeWorkspace: null,
  role: "",
  isLoading: false,
  error: false,
  refresh: async () => {},
  can: () => false,
  meets: () => false,
}

const WorkspaceContext = createContext<WorkspaceContextValue>(DEFAULT_VALUE)

/** Resolve the active workspace from the list: saved id first, else the first one. */
function resolveActive(list: WorkspaceSummary[], savedId: string | null): WorkspaceSummary | null {
  if (savedId) {
    const found = list.find((w) => w.id === savedId)
    if (found) return found
  }
  return list[0] ?? null
}

export function WorkspaceProvider({ children }: { children: ReactNode }) {
  const [workspaces, setWorkspaces] = useState<WorkspaceSummary[]>([])
  const [activeId, setActiveId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState(false)

  const refresh = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.LIST)
      if (!res.ok) {
        setError(true)
        return
      }
      const data = (await res.json()) as { workspaces?: WorkspaceSummary[] }
      const list = data.workspaces ?? []
      setWorkspaces(list)
      setError(false)
      // Resolve (and persist) the active selection from this fresh list. Clearing a
      // stale selection here keeps the X-Workspace-ID header from pointing at a
      // workspace the caller no longer belongs to.
      const saved = getActiveWorkspaceId()
      const resolved = resolveActive(list, saved)
      const nextId = resolved?.id ?? null
      setActiveId(nextId)
      if (nextId !== saved) setActiveWorkspaceId(nextId)
    } catch {
      setError(true)
    } finally {
      setIsLoading(false)
    }
  }, [])

  useEffect(() => {
    void refresh()
  }, [refresh])

  // Mirror the latest list into a ref so the change-listener below can consult it
  // without re-subscribing on every list mutation.
  const workspacesRef = useRef<WorkspaceSummary[]>([])
  useEffect(() => {
    workspacesRef.current = workspaces
  }, [workspaces])

  // Re-resolve the active workspace when the selection changes in place (header
  // switcher, delete re-point, or another tab) — no refetch needed for a pure switch.
  // But if the newly-selected id isn't in our current list, it was just created (or
  // added elsewhere): refetch so the settings hub and role gates resolve against the
  // real workspace instead of silently falling back to list[0] (the stale-pin bug).
  useEffect(
    () =>
      onActiveWorkspaceChange(() => {
        const nextId = getActiveWorkspaceId()
        setActiveId(nextId)
        if (nextId && !workspacesRef.current.some((w) => w.id === nextId)) {
          void refresh()
        }
      }),
    [refresh],
  )

  const activeWorkspace = useMemo(() => resolveActive(workspaces, activeId), [workspaces, activeId])
  const role = activeWorkspace?.role ?? ""

  const value = useMemo<WorkspaceContextValue>(
    () => ({
      workspaces,
      activeWorkspace,
      role,
      isLoading,
      error,
      refresh,
      can: (action: WorkspaceAction) => canRole(role, action),
      meets: (min: WorkspaceRole) => meetsRole(role, min),
    }),
    [workspaces, activeWorkspace, role, isLoading, error, refresh],
  )

  return <WorkspaceContext.Provider value={value}>{children}</WorkspaceContext.Provider>
}

/** Full workspace context: list, active workspace, loading/error, refresh. */
export function useWorkspace(): WorkspaceContextValue {
  return useContext(WorkspaceContext)
}

/** Role-focused slice for gating controls. Fail-closed outside a provider. */
export function useWorkspaceRole(): {
  role: string
  isLoading: boolean
  error: boolean
  activeWorkspace: WorkspaceSummary | null
  can: (action: WorkspaceAction) => boolean
  meets: (min: WorkspaceRole) => boolean
} {
  const { role, isLoading, error, activeWorkspace, can, meets } = useContext(WorkspaceContext)
  return { role, isLoading, error, activeWorkspace, can, meets }
}
