"use client"

import { useState } from "react"
import { RefreshCw } from "lucide-react"
import { PageHeader } from "@/components/layout/PageHeader"
import { Button } from "@/components/ui/button"
import { AssetLineageView } from "@/components/explorer/AssetLineageView"

export default function LineagePage() {
  const [reloadTick, setReloadTick] = useState(0)
  return (
    <div className="space-y-6">
      <PageHeader
        heading="Lineage"
        description="Which pipelines write which tables, which models read them, and which models are refreshed out of step with what they read."
      >
        <Button variant="outline" size="sm" onClick={() => setReloadTick((t) => t + 1)}>
          <RefreshCw className="mr-2 h-4 w-4" />
          Refresh
        </Button>
      </PageHeader>
      <AssetLineageView reloadTick={reloadTick} />
    </div>
  )
}
