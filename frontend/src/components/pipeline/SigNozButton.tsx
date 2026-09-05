"use client"

import { useSyncExternalStore } from "react"
import { Activity, ExternalLink, BarChart2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { isLeakedLocalhostUrl } from "@/lib/config/api"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"

const SIGNOZ_URL = process.env.NEXT_PUBLIC_SIGNOZ_UI_URL

// Hydration-safe "are we on the client?" — false during SSR and the first client
// render (so hydration matches), true afterwards — without a setState-in-effect.
const noopSubscribe = () => () => {}
function useHydrated(): boolean {
  return useSyncExternalStore(noopSubscribe, () => true, () => false)
}

function buildTraceSearchURL(base: string, pipelineId: string): string {
  const tags = JSON.stringify([
    { id: "pipeline_id", key: "pipeline_id", value: pipelineId, operator: "EQUALS", type: "tag" },
  ])
  return `${base}/trace?selectedTags=${encodeURIComponent(tags)}`
}

export function SigNozButton({ pipelineId }: { pipelineId: string }) {
  // Hide the button when NEXT_PUBLIC_SIGNOZ_UI_URL was mis-baked to a localhost
  // value on a real origin (its links would be dead localhost links). Gated on
  // hydration so SSR / first client render match. SigNoz is a separate host, so
  // origin-rebasing (rebaseLeakedLocalhost) is deliberately NOT used here.
  const hydrated = useHydrated()

  if (!SIGNOZ_URL) return null
  if (hydrated && isLeakedLocalhostUrl(SIGNOZ_URL)) return null

  const tracesUrl = buildTraceSearchURL(SIGNOZ_URL, pipelineId)
  const dashboardUrl = `${SIGNOZ_URL}/dashboard`

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" title="View in SigNoz">
          <Activity className="h-4 w-4 mr-2" />
          SigNoz
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="z-50">
        <DropdownMenuItem asChild>
          <a href={tracesUrl} target="_blank" rel="noopener noreferrer" className="flex items-center gap-2 cursor-pointer">
            <Activity className="h-4 w-4" />
            View Traces
            <ExternalLink className="h-3 w-3 ml-auto opacity-50" />
          </a>
        </DropdownMenuItem>
        <DropdownMenuItem asChild>
          <a href={dashboardUrl} target="_blank" rel="noopener noreferrer" className="flex items-center gap-2 cursor-pointer">
            <BarChart2 className="h-4 w-4" />
            Metrics Dashboard
            <ExternalLink className="h-3 w-3 ml-auto opacity-50" />
          </a>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
