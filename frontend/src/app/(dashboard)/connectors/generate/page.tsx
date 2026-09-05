"use client"

import { Suspense } from "react"
import Link from "next/link"
import { Loader2, Sparkles, Lock } from "lucide-react"
import { Button } from "@/components/ui/button"
import { DiscoveryFlow } from "@/components/connectors/discovery/DiscoveryFlow"
import { canGenerateConnectors } from "@/contexts/CurrentUserContext"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"

/**
 * Generate-connector page.
 *
 * Phase 13c consolidation: the legacy single-form flow has been removed.
 * Every user goes through the discovery flow which:
 *   - For known vendors (vendor_apis.yaml): pre-fills protocol/endpoint/auth
 *     from the registry, auto-discovers operations from docs, ships a
 *     READY contract on first POST → user just clicks Generate.
 *   - For unknown vendors: walks a guided wizard that refuses to ship a
 *     hallucinated connector when critical dimensions are unconfirmed.
 *
 * The `?discover=1` query param is no longer required (kept silently for
 * back-compat with any old bookmarks).
 */
export default function GenerateConnectorPage() {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center min-h-screen">
        <Loader2 className="h-8 w-8 animate-spin text-violet-500" />
      </div>
    }>
      <GeneratePageContent />
    </Suspense>
  )
}

function GeneratePageContent() {
  const { role: workspaceRole, isLoading } = useWorkspaceRole()
  const allowed = canGenerateConnectors(workspaceRole)
  return (
    <div className="container mx-auto px-6 py-8 max-w-6xl">
      {/* Hero */}
      <div className="relative overflow-hidden rounded-2xl border border-violet-200/70 dark:border-violet-900/40 bg-gradient-to-br from-violet-50 via-indigo-50 to-white dark:from-violet-950/40 dark:via-indigo-950/30 dark:to-zinc-950 p-6 mb-6">
        <div className="absolute -top-10 -right-10 h-40 w-40 rounded-full bg-violet-200/40 blur-3xl pointer-events-none" />
        <div className="relative flex items-start gap-3">
          <div className="rounded-xl bg-white dark:bg-zinc-900 border border-violet-200 dark:border-violet-900/50 p-2 shadow-sm">
            <Sparkles className="h-5 w-5 text-violet-600" />
          </div>
          <div className="flex-1">
            <h1 className="text-xl font-semibold text-zinc-900 dark:text-zinc-50">
              Generate a connector
            </h1>
            <p className="text-sm text-zinc-600 dark:text-zinc-400 mt-1 max-w-2xl">
              From any API name to a running MCP connector. We discover protocol, auth,
              and operations from public docs — and refuse to ship anything we couldn&apos;t verify.
            </p>
          </div>
        </div>

        {/* Stepper */}
        <ol className="relative mt-5 flex items-center gap-2 text-xs">
          <Step n={1} label="Discover" active />
          <StepConnector />
          <Step n={2} label="Confirm" />
          <StepConnector />
          <Step n={3} label="Generate" />
        </ol>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center py-16">
          <Loader2 className="h-6 w-6 animate-spin text-violet-500" />
        </div>
      ) : allowed ? (
        <DiscoveryFlow
          onGenerate={async (contract) => {
            console.info("[discovery] generation complete", contract.session_id)
          }}
        />
      ) : (
        <GenerationAccessRequired />
      )}
    </div>
  )
}

// Connector generation is gated to workspace owner/admin at the api-gateway
// (WorkspaceGeneratorMiddleware). Members/viewers used to reach the wizard and
// hit an opaque 403 on Generate; instead, tell them why and where to go. The
// gateway remains the source of truth — this is UX, not enforcement.
function GenerationAccessRequired() {
  return (
    <div className="rounded-2xl border border-amber-200 dark:border-amber-900/40 bg-amber-50/60 dark:bg-amber-950/20 p-8 text-center">
      <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-amber-100 dark:bg-amber-900/30">
        <Lock className="h-6 w-6 text-amber-600" />
      </div>
      <h2 className="text-lg font-semibold text-zinc-900 dark:text-zinc-50">
        Connector generation needs workspace admin access
      </h2>
      <p className="mx-auto mt-2 max-w-md text-sm text-zinc-600 dark:text-zinc-400">
        Generating a new MCP connector requires the <span className="font-medium">Owner</span> or{" "}
        <span className="font-medium">Admin</span> role in this workspace. Ask your workspace owner to
        generate it or to raise your role — or browse the connectors that are already available.
      </p>
      <div className="mt-5 flex items-center justify-center gap-2">
        <Link href="/connectors">
          <Button variant="outline">Back to connectors</Button>
        </Link>
      </div>
    </div>
  )
}

function Step({ n, label, active = false }: { n: number; label: string; active?: boolean }) {
  return (
    <li className="flex items-center gap-1.5">
      <span
        className={
          active
            ? "inline-flex h-5 w-5 items-center justify-center rounded-full bg-violet-600 text-white text-[10px] font-bold"
            : "inline-flex h-5 w-5 items-center justify-center rounded-full bg-white dark:bg-zinc-900 border border-zinc-300 dark:border-zinc-700 text-zinc-500 text-[10px] font-bold"
        }
      >
        {n}
      </span>
      <span className={active ? "text-violet-700 dark:text-violet-300 font-medium" : "text-zinc-500"}>
        {label}
      </span>
    </li>
  )
}

function StepConnector() {
  return <span className="h-px w-6 bg-zinc-300 dark:bg-zinc-700" />
}
