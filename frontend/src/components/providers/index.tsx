"use client"

import { QueryProvider } from "./QueryProvider"
import { ThemeProvider } from "./ThemeProvider"
import { TelemetryProvider, TelemetryDevTools } from "./TelemetryProvider"
import { WorkspaceProvider } from "@/contexts/WorkspaceContext"
import { CurrentUserProvider } from "@/contexts/CurrentUserContext"

interface ProvidersProps {
  children: React.ReactNode
}

export function Providers({ children }: ProvidersProps) {
  return (
    <ThemeProvider>
      <TelemetryProvider>
        <QueryProvider>
          {/* Shared active-workspace + role context for the dashboard tree. Fetches
              /workspaces once; on a public/unauthenticated mount the fetch simply
              fails closed (error state, no UI) — harmless. */}
          <WorkspaceProvider>
            {/* Shared current-user (platform tier) context. Fetches /auth/me
                once so role-gated CTAs (e.g. connector generation) render the
                right affordance; fails closed on unauthenticated mounts. */}
            <CurrentUserProvider>
              {children}
              {/* Shows trace ID in dev mode */}
              <TelemetryDevTools />
            </CurrentUserProvider>
          </WorkspaceProvider>
        </QueryProvider>
      </TelemetryProvider>
    </ThemeProvider>
  )
}

// Re-export telemetry hooks for convenience
export { useTelemetry, useTraceId, TelemetryProvider, TelemetryDevTools } from "./TelemetryProvider"
