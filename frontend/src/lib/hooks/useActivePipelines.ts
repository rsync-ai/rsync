"use client"

import { useState, useEffect, useCallback } from "react"

import { onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"
import { dropLegacyUnscopedValue, workspaceScopedKey } from "@/lib/workspace/scoped-storage"

// Base key only — the real key is workspace-scoped. These entries carry pipeline
// ids and names, which belong to exactly one workspace; kept unscoped they showed
// the previous tenant's pipelines in the chat sidebar after a switch.
const ACTIVE_PIPELINES_KEY = "rsync_active_pipelines"
const MAX_TRACKED_PIPELINES = 10

const TTL_MS = 24 * 60 * 60 * 1000

/** Reads this workspace's tracked pipelines, dropping entries older than 24h. */
function readStored(): ActivePipeline[] {
  if (typeof window === "undefined") return []
  try {
    const stored = localStorage.getItem(workspaceScopedKey(ACTIVE_PIPELINES_KEY))
    if (!stored) return []
    const parsed = JSON.parse(stored) as ActivePipeline[]
    const now = Date.now()
    return parsed.filter((p) => now - new Date(p.lastUpdated).getTime() < TTL_MS)
  } catch {
    return []
  }
}

export interface ActivePipeline {
  id: string
  name?: string
  status?: string
  startedAt: string
  lastUpdated: string
}

/**
 * Hook to track multiple active pipelines across chat sessions
 * Stores in localStorage for persistence across page reloads
 */
export function useActivePipelines() {
  const [pipelines, setPipelines] = useState<ActivePipeline[]>([])

  // Load this workspace's list on mount, and re-load it whenever the active
  // workspace changes — the in-memory list belongs to the workspace we just left,
  // so it must be swapped, not carried over. Same-tab and cross-tab both.
  useEffect(() => {
    if (typeof window === "undefined") return

    dropLegacyUnscopedValue(ACTIVE_PIPELINES_KEY)
    setPipelines(readStored())
    return onActiveWorkspaceChange(() => setPipelines(readStored()))
  }, [])

  // Save to localStorage when pipelines change. The key is resolved here (not
  // captured) so a write always lands in the workspace that is active now.
  useEffect(() => {
    if (typeof window === "undefined") return

    try {
      localStorage.setItem(workspaceScopedKey(ACTIVE_PIPELINES_KEY), JSON.stringify(pipelines))
    } catch {
      // Ignore storage errors
    }
  }, [pipelines])

  const addPipeline = useCallback((id: string, name?: string, status?: string) => {
    setPipelines((prev) => {
      // Check if already exists
      const existingIndex = prev.findIndex((p) => p.id === id)
      const now = new Date().toISOString()

      if (existingIndex >= 0) {
        // Update existing
        const updated = [...prev]
        updated[existingIndex] = {
          ...updated[existingIndex],
          name: name || updated[existingIndex].name,
          status: status || updated[existingIndex].status,
          lastUpdated: now,
        }
        return updated
      }

      // Add new
      const newPipeline: ActivePipeline = {
        id,
        name,
        status: status || "processing",
        startedAt: now,
        lastUpdated: now,
      }

      // Keep only the most recent pipelines
      const updated = [newPipeline, ...prev].slice(0, MAX_TRACKED_PIPELINES)
      return updated
    })
  }, [])

  const updatePipeline = useCallback((id: string, updates: Partial<Omit<ActivePipeline, "id">>) => {
    setPipelines((prev) => {
      const index = prev.findIndex((p) => p.id === id)
      if (index < 0) return prev

      const updated = [...prev]
      updated[index] = {
        ...updated[index],
        ...updates,
        lastUpdated: new Date().toISOString(),
      }
      return updated
    })
  }, [])

  const removePipeline = useCallback((id: string) => {
    setPipelines((prev) => prev.filter((p) => p.id !== id))
  }, [])

  const clearCompleted = useCallback(() => {
    setPipelines((prev) =>
      prev.filter((p) => p.status !== "completed" && p.status !== "failed")
    )
  }, [])

  const clearAll = useCallback(() => {
    setPipelines([])
  }, [])

  return {
    pipelines,
    addPipeline,
    updatePipeline,
    removePipeline,
    clearCompleted,
    clearAll,
  }
}
