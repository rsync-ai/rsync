import { useCallback } from "react"

import { dropLegacyUnscopedValue, workspaceScopedKey } from "@/lib/workspace/scoped-storage"

// Base key only — every read/write goes through workspaceScopedKey. A chat
// transcript names that workspace's connections, tables and pipelines, so an
// unscoped key replayed one tenant's conversation into the next one after a
// workspace switch.
export const CHAT_UI_STATE_KEY = "rsync_chat_ui_v2_state"
export const MAX_PERSISTED_MESSAGES = 50

const SESSION_TTL_MS = 30 * 60 * 1000 // 30 minutes

export function useChatPersistence<TMessage>() {
  const persistUiState = useCallback(
    (next: { messages: TMessage[]; activeIntent: string }) => {
      if (typeof window === "undefined") return
      try {
        const trimmed = next.messages.slice(-MAX_PERSISTED_MESSAGES)
        window.localStorage.setItem(
          workspaceScopedKey(CHAT_UI_STATE_KEY),
          JSON.stringify({
            v: 1,
            messages: trimmed,
            activeIntent: next.activeIntent,
            updatedAt: new Date().toISOString(),
          })
        )
      } catch {
        // ignore (private mode / quota)
      }
    },
    []
  )

  const loadPersistedUiState = useCallback((): { messages: TMessage[]; activeIntent: string } => {
    if (typeof window === "undefined") return { messages: [], activeIntent: "" }
    try {
      dropLegacyUnscopedValue(CHAT_UI_STATE_KEY)
      const key = workspaceScopedKey(CHAT_UI_STATE_KEY)
      const raw = window.localStorage.getItem(key)
      if (!raw) return { messages: [], activeIntent: "" }
      const parsed = JSON.parse(raw) as Record<string, unknown>
      const rawUpdatedAt = parsed?.["updatedAt"]
      const updatedAt =
        typeof rawUpdatedAt === "string" || typeof rawUpdatedAt === "number"
          ? new Date(rawUpdatedAt as string | number).getTime()
          : 0
      if (updatedAt && Date.now() - updatedAt > SESSION_TTL_MS) {
        window.localStorage.removeItem(key)
        return { messages: [], activeIntent: "" }
      }
      const persistedMessages = Array.isArray(parsed?.["messages"])
        ? (parsed["messages"] as TMessage[])
        : []
      const persistedActiveIntent =
        typeof parsed?.["activeIntent"] === "string" ? (parsed["activeIntent"] as string) : ""
      return {
        messages: persistedMessages.slice(-MAX_PERSISTED_MESSAGES),
        activeIntent: persistedActiveIntent,
      }
    } catch {
      return { messages: [], activeIntent: "" }
    }
  }, [])

  const appendPersistedMessage = useCallback(
    (msg: TMessage, nextActiveIntent?: string): TMessage[] => {
      const cur = loadPersistedUiState()
      const active = typeof nextActiveIntent === "string" ? nextActiveIntent : cur.activeIntent
      const nextMessages = [...cur.messages, msg].slice(-MAX_PERSISTED_MESSAGES)
      persistUiState({ messages: nextMessages, activeIntent: active })
      return nextMessages
    },
    [loadPersistedUiState, persistUiState]
  )

  const clearPersistedState = useCallback(() => {
    if (typeof window === "undefined") return
    try {
      window.localStorage.removeItem(workspaceScopedKey(CHAT_UI_STATE_KEY))
    } catch {
      // ignore
    }
  }, [])

  return { persistUiState, loadPersistedUiState, appendPersistedMessage, clearPersistedState }
}
