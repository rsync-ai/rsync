import { Badge } from "@/components/ui/badge"
import type { LoadStatus, LoadStatusTone } from "@/lib/pipeline/loadStatus"

const VARIANT: Record<LoadStatusTone, "success" | "info" | "warning" | "destructive"> = {
  ok: "success",
  info: "info",
  warn: "warning",
  error: "destructive",
}

/** "Full load in progress · 3 / 5 tables" / "Load completed, replication ongoing". */
export function LoadStatusBadge({ status, testId }: { status: LoadStatus; testId?: string }) {
  return (
    <Badge variant={VARIANT[status.tone]} title={status.title} data-testid={testId} className="font-medium">
      {status.text}
    </Badge>
  )
}
