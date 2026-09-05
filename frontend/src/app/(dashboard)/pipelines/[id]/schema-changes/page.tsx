import { notFound } from "next/navigation"
import Link from "next/link"
import { cookies } from "next/headers"
import { activeWorkspaceCookieHeader } from "@/lib/workspace/server-workspace"
import { ArrowLeft } from "lucide-react"
import { Button } from "@/components/ui/button"
import { PageHeader } from "@/components/layout/PageHeader"
import { SchemaDriftApprovalList } from "@/components/pipeline/SchemaDriftApprovalList"
import { API_ENDPOINTS } from "@/lib/config/api"

export const dynamic = "force-dynamic"

interface Props {
  params: Promise<{ id: string }>
}

// Server-side fetch must use the internal gateway URL (localhost inside the
// container is NOT the API gateway). A non-owner gets a non-ok response here,
// so notFound() reuses the gateway's owner-gate — same pattern as the sibling
// pipeline detail page.
async function getPipeline(id: string) {
  try {
    const cookieStore = await cookies()
    const authToken = cookieStore.get("auth_token")?.value
    // Forward the active-workspace selection (cookie mirror) so the owner-gated
    // fetch scopes to the chosen workspace, not the caller's personal one.
    const authHeaders: Record<string, string> = {
      ...(authToken ? { Authorization: authToken } : {}),
      ...activeWorkspaceCookieHeader(cookieStore),
    }
    const res = await fetch(API_ENDPOINTS.PIPELINES.GET_INTERNAL(id), {
      cache: "no-store",
      signal: AbortSignal.timeout(5000),
      headers: authHeaders,
    })
    if (!res.ok) return null
    const raw = await res.json()
    if (!raw) return null
    return {
      id: raw.id as string,
      name: (raw.name as string) || "this pipeline",
      destConnectorType: (raw.destination_connection?.connector_type as string | undefined) ?? null,
      // Same CDC test the pipeline detail page uses (sync_mode, or a persisted
      // cdc_mode for pipelines created before sync_mode was written). Batch and CDC
      // have genuinely different approval policies, and this page has to say which
      // one the user is looking at.
      isCDC: raw.sync_mode === "cdc" || Boolean(raw.cdc_mode),
    }
  } catch {
    return null
  }
}

export default async function SchemaChangesPage({ params }: Props) {
  const { id } = await params
  const pipeline = await getPipeline(id)
  if (!pipeline) notFound()

  return (
    <div className="space-y-6">
      <Link href={`/pipelines/${id}`}>
        <Button variant="ghost" size="sm">
          <ArrowLeft className="h-4 w-4 mr-2" />
          Back to pipeline
        </Button>
      </Link>
      <PageHeader
        heading="Schema changes"
        description={
          pipeline.isCDC
            ? `Schema-drift changes detected for “${pipeline.name}”. This is a CDC pipeline — new columns are applied as they stream in, so they appear here as history.`
            : `Review and approve schema-drift changes detected for “${pipeline.name}”.`
        }
      />
      <SchemaDriftApprovalList
        pipelineId={id}
        destConnectorType={pipeline.destConnectorType}
        isCDC={pipeline.isCDC}
      />
    </div>
  )
}
