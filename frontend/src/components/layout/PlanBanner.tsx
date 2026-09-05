"use client"

import { useEffect, useState } from "react"
import { Sparkles, X } from "lucide-react"
import { Button } from "@/components/ui/button"
import { UpgradeModal } from "@/components/plan/UpgradeModal"
import { authFetch } from "@/lib/api/auth-fetch"
import type { CurrentUser } from "@/lib/api/auth-fetch"

type PlanBannerVariant = "trial" | "plan_limit"

interface BannerState {
  variant: PlanBannerVariant
  plan?: string
  daysLeft?: number
  used?: number
  limit?: number
}

export function PlanBanner() {
  const [banner, setBanner] = useState<BannerState | null>(null)
  const [dismissed, setDismissed] = useState(false)
  const [showUpgrade, setShowUpgrade] = useState(false)

  useEffect(() => {
    async function checkPlan() {
      try {
        const res = await authFetch("/api/v1/auth/me")
        if (!res.ok) return
        const data = await res.json() as CurrentUser
        if (!data.plan || data.plan === "pro") return

        if (data.plan === "trial" && data.trial_ends_at) {
          const msLeft = new Date(data.trial_ends_at).getTime() - Date.now()
          const daysLeft = Math.max(0, Math.ceil(msLeft / 86_400_000))
          setBanner({ variant: "trial", daysLeft })
        } else if ((data.plan === "free" || data.plan === "starter") && data.pipelines_limit != null) {
          const used = data.pipelines_used ?? 0
          const limit = data.pipelines_limit
          // Free: always nudge. Starter (paid): only nudge once at/over the cap,
          // so a paying customer with headroom isn't nagged every page load.
          if (data.plan === "free" || used >= limit) {
            setBanner({ variant: "plan_limit", plan: data.plan, used, limit })
          }
        }
      } catch {
        // non-critical
      }
    }
    void checkPlan()
  }, [])

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
