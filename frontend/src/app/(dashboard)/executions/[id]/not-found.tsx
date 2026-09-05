import Link from "next/link"
import { SearchX } from "lucide-react"
import { Button } from "@/components/ui/button"

/**
 * Rendered when ExecutionDetailPage calls notFound() — the api-gateway returned
 * 404 for this id under the caller's active workspace, which covers both a
 * deleted execution and one belonging to another tenant (the gateway returns
 * 404, not 403, so it never confirms a foreign resource exists).
 *
 * Without this boundary the route fell through to Next.js's bare default 404,
 * outside the dashboard chrome and with no way back. Switching workspace no
 * longer lands here — the header switcher redirects off detail routes (see
 * lib/workspace/scoped-routes.ts) — so reaching this page now means a stale
 * link, a bookmark, or a genuinely deleted execution, which is exactly when an
 * explanation beats a silent redirect.
 */
export default function ExecutionNotFound() {
  return (
    <div className="flex min-h-[50vh] flex-col items-center justify-center gap-4 text-center">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-zinc-100 dark:bg-zinc-800">
        <SearchX className="h-6 w-6 text-zinc-500 dark:text-zinc-400" />
      </div>
      <div className="space-y-1">
        <h1 className="text-xl font-semibold text-zinc-900 dark:text-white">Execution not found</h1>
        <p className="max-w-md text-sm text-zinc-500 dark:text-zinc-400">
          This run may have been deleted, or it belongs to a different workspace.
          Switch workspaces from the header, or head back to your executions.
        </p>
      </div>
      <Link href="/executions">
        <Button variant="default" size="sm">
          Back to executions
        </Button>
      </Link>
    </div>
  )
}
