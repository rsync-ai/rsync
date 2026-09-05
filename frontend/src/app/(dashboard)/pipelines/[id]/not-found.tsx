import Link from "next/link"
import { SearchX } from "lucide-react"
import { Button } from "@/components/ui/button"

/**
 * Rendered when PipelineDetailPage calls notFound() — i.e. the api-gateway
 * returned 404 for this id under the caller's active workspace. That happens
 * both when the pipeline was deleted AND when it belongs to a different
 * workspace: the gateway returns 404 (not 403) so it never reveals whether a
 * resource in another tenant exists. This boundary preserves that privacy
 * guarantee while replacing the bare default 404 with a page that explains the
 * likely cause and routes the user back — most often the id was opened after
 * the active workspace was switched. (R7)
 */
export default function PipelineNotFound() {
  return (
    <div className="flex min-h-[50vh] flex-col items-center justify-center gap-4 text-center">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-zinc-100 dark:bg-zinc-800">
        <SearchX className="h-6 w-6 text-zinc-500 dark:text-zinc-400" />
      </div>
      <div className="space-y-1">
        <h1 className="text-xl font-semibold text-zinc-900 dark:text-white">Pipeline not found</h1>
        <p className="max-w-md text-sm text-zinc-500 dark:text-zinc-400">
          This pipeline may have been deleted, or it belongs to a different workspace.
          Switch workspaces from the header, or head back to your pipelines.
        </p>
      </div>
      <Link href="/pipelines">
        <Button variant="default" size="sm">
          Back to pipelines
        </Button>
      </Link>
    </div>
  )
}
