import { create } from "zustand"
import { persist, createJSONStorage } from "zustand/middleware"

interface UIState {
  /** Whether the desktop sidebar is collapsed to a narrow rail. Persisted. */
  sidebarCollapsed: boolean
  /** Whether the mobile sidebar drawer is open. Not persisted. */
  sidebarOpen: boolean
  /** Whether the command palette overlay is open. Not persisted. */
  commandPaletteOpen: boolean

  toggleSidebarCollapsed: () => void
  setSidebarCollapsed: (value: boolean) => void
  toggleSidebar: () => void
  setSidebarOpen: (value: boolean) => void
  toggleCommandPalette: () => void
  setCommandPaletteOpen: (value: boolean) => void
}

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      sidebarCollapsed: false,
      sidebarOpen: false,
      commandPaletteOpen: false,

      toggleSidebarCollapsed: () =>
        set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
      setSidebarCollapsed: (value) => set({ sidebarCollapsed: value }),
      toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),
      setSidebarOpen: (value) => set({ sidebarOpen: value }),
      toggleCommandPalette: () =>
        set((state) => ({ commandPaletteOpen: !state.commandPaletteOpen })),
      setCommandPaletteOpen: (value) => set({ commandPaletteOpen: value }),
    }),
    {
      name: "rsync-ui-state",
      storage: createJSONStorage(() => localStorage),
      // Only persist the desktop sidebar collapse preference. Mobile drawer
      // and command palette should always start closed on a fresh load.
      partialize: (state) => ({ sidebarCollapsed: state.sidebarCollapsed }),
    }
  )
)
