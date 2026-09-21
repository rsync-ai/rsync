"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { usePathname } from "next/navigation"
import { Sparkles, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { UpgradeModal } from "@/components/plan/UpgradeModal"
import { authFetch } from "@/lib/api/auth-fetch"
import { captureWorkspace, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"
import { onPipelinesChanged } from "@/lib/plan/plan-events"

type PlanBannerVariant = "trial" | "plan_limit"

interface BannerState {
  variant: PlanBannerVariant
  plan?: string
  daysLeft?: number
  used?: number
  limit?: number
}

interface PlanSummary {
  plan?: string
  pipelines_used?: number
  pipelines_limit?: number | null
  trial_ends_at?: string | null
}

/**
 * The ACTIVE workspace's plan meter. /api/v1/usage/plan counts pipelines with
 * the same query, on the same workspace, as the create gate
 * (plan_quota.go countWorkspacePipelines), so the banner cannot disagree with
 * enforcement. /auth/me (personal workspace) is only a fallback for a gateway
 * that predates the endpoint.
 */
async function fetchPlanSummary(): Promise<PlanSummary | null> {
  const res = await authFetch("/api/v1/usage/plan")
  if (res.ok) return (await res.json()) as PlanSummary
  if (res.status !== 404) return null
  const me = await authFetch("/api/v1/auth/me")
  if (!me.ok) return null
  return (await me.json()) as PlanSummary
}

export function bannerFromPlan(data: PlanSummary): BannerState | null {
  if (!data.plan || data.plan === "pro") return null
  if (data.plan === "trial" && data.trial_ends_at) {
    const msLeft = new Date(data.trial_ends_at).getTime() - Date.now()
    const daysLeft = Math.max(0, Math.ceil(msLeft / 86_400_000))
    return { variant: "trial", daysLeft }
  }
  if ((data.plan === "free" || data.plan === "starter") && data.pipelines_limit != null) {
    const used = data.pipelines_used ?? 0
    const limit = data.pipelines_limit
    // Free: always nudge. Starter (paid): only nudge once at/over the cap,
    // so a paying customer with headroom isn't nagged every page load.
    if (data.plan === "free" || used >= limit) {
      return { variant: "plan_limit", plan: data.plan, used, limit }
    }
  }
  return null
}

export function PlanBanner() {
  const [banner, setBanner] = useState<BannerState | null>(null)
  const [dismissed, setDismissed] = useState(false)
  const [showUpgrade, setShowUpgrade] = useState(false)
  const pathname = usePathname()
  const seq = useRef(0)

  // The banner is mounted once in the persistent dashboard layout, so a single
  // fetch on mount went stale as soon as the user created a pipeline (it showed
  // 0/2 with one pipeline running). Re-read on navigation, tab focus, workspace
  // switch and pipeline create/delete; the newest request wins.
  const refresh = useCallback(async () => {
    const mine = ++seq.current
    const isStale = captureWorkspace()
    try {
      const data = await fetchPlanSummary()
      if (mine !== seq.current || isStale() || !data) return
      setBanner(bannerFromPlan(data))
    } catch {
      // non-critical
    }
  }, [])

  useEffect(() => {
    void refresh()
  }, [pathname, refresh])

  useEffect(() => {
    const onVisible = () => {
      if (document.visibilityState === "visible") void refresh()
    }
    const onFocus = () => void refresh()
    window.addEventListener("focus", onFocus)
    document.addEventListener("visibilitychange", onVisible)
    const offWorkspace = onActiveWorkspaceChange(() => void refresh())
    const offPipelines = onPipelinesChanged(() => void refresh())
    return () => {
      window.removeEventListener("focus", onFocus)
      document.removeEventListener("visibilitychange", onVisible)
      offWorkspace()
      offPipelines()
    }
  }, [refresh])

  if (!banner || dismissed) return null

  const planLabel = banner.plan === "starter" ? "Starter" : "Free"
  const label =
    banner.variant === "trial"
      ? `Trial — ${banner.daysLeft === 0 ? "expires today" : `${banner.daysLeft} day${banner.daysLeft === 1 ? "" : "s"} left`}`
      : `${planLabel} plan — ${banner.used}/${banner.limit} pipelines used`

  return (
    <>
      <div className="sticky top-16 z-20 -mx-4 sm:-mx-6 -mt-4 sm:-mt-6 mb-4 flex items-center justify-between gap-4 border-b border-primary/20 bg-primary/5 px-4 py-2.5 text-sm text-primary dark:border-primary/30 dark:bg-primary/10">
        <div className="flex min-w-0 items-center gap-2">
          <Sparkles className="h-4 w-4 shrink-0 text-primary" />
          <span>
            {label} ·{" "}
            <button
              type="button"
              onClick={() => setShowUpgrade(true)}
              className="cursor-pointer font-medium underline underline-offset-2 align-baseline hover:opacity-80"
            >
              Upgrade to Pro
            </button>
          </span>
        </div>
        <Button
          variant="ghost"
          size="icon-sm"
          className="shrink-0 text-primary/70 hover:bg-primary/10 hover:text-primary"
          onClick={() => setDismissed(true)}
          aria-label="Dismiss"
        >
          <X className="h-4 w-4" />
        </Button>
      </div>
      <UpgradeModal open={showUpgrade} onClose={() => setShowUpgrade(false)} />
    </>
  )
}
