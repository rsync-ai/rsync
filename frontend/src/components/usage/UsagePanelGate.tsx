"use client"

import type { ReactNode } from "react"
import { LoadingState } from "@/components/admin/AdminStates"
import { PageHeader } from "@/components/layout/PageHeader"
import { Card } from "@/components/ui/card"
import { useUsagePanelState } from "@/config/features"

/**
 * Gates a plan/usage page on the usage_panel feature flag.
 *
 * Hiding the two nav entries is not hiding the panel: /usage and /admin/usage
 * stay ordinary routes and a typed URL still renders them. This wrapper is what
 * makes the flag mean the page rather than the link. It sits OUTSIDE the page's
 * own component on purpose, so a hidden page never runs its /api/v1/usage fetch
 * either.
 *
 * `nav` is chrome the reader needs in order to leave — the admin section's own
 * nav bar — and renders above the message.
 */
export function UsagePanelGate({
  children,
  nav,
}: {
  children: ReactNode
  nav?: ReactNode
}) {
  const state = useUsagePanelState()

  // Not yet known. The build-time default is the cloud answer and every
  // deployment pulls the same prebuilt image, so rendering it here would show a
  // self-host a plan meter for a plan it does not have, for one frame.
  if (state === "loading") {
    return (
      <div className="space-y-6">
        {nav}
        <LoadingState />
      </div>
    )
  }

  if (state === "off") {
    return (
      <div className="space-y-6">
        {nav}
        <PageHeader
          heading="Usage"
          description="Plan and quota reporting is off on this deployment."
        />
        <Card className="p-6 text-sm text-zinc-600 dark:text-zinc-400">
          <p>
            This deployment does not enforce plan limits, so there are no quotas to
            report. Pipelines, queries and data transfer are unlimited.
          </p>
          <p className="mt-3">
            An operator can turn the page on by setting{" "}
            <code className="rounded bg-zinc-100 px-1 py-0.5 font-mono text-xs dark:bg-zinc-800">
              FEATURE_USAGE_PANEL=true
            </code>{" "}
            on the API gateway.
          </p>
        </Card>
      </div>
    )
  }

  return <>{children}</>
}
