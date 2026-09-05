import Link from "next/link"
import { SearchX } from "lucide-react"
import { Button } from "@/components/ui/button"

/**
 * Rendered when ConnectionDetailPage calls notFound() — i.e. the api-gateway
 * returned 404 for this id under the caller's active workspace. As with
 * pipelines and executions, that covers both "deleted" and "belongs to another
 * workspace": the gateway answers 404 rather than 403 so it never reveals
 * whether a resource in another tenant exists.
 *
 * Before this boundary existed, the page bounced to /connections behind a
 * "Could not load connections" toast — which was simply untrue (the list loads
 * fine; one resource was out of scope) and gave the user nothing to act on.
 */
export default function ConnectionNotFound() {
  return (
    <div className="flex min-h-[50vh] flex-col items-center justify-center gap-4 text-center">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-zinc-100 dark:bg-zinc-800">
        <SearchX className="h-6 w-6 text-zinc-500 dark:text-zinc-400" />
      </div>
      <div className="space-y-1">
        <h1 className="text-xl font-semibold text-zinc-900 dark:text-white">Connection not found</h1>
        <p className="max-w-md text-sm text-zinc-500 dark:text-zinc-400">
          This connection may have been deleted, or it belongs to a different workspace.
          Switch workspaces from the header, or head back to your connections.
        </p>
      </div>
      <Link href="/connections">
        <Button variant="default" size="sm">
          Back to connections
        </Button>
      </Link>
    </div>
  )
}
