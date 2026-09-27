import { redirect } from "next/navigation"

export const dynamic = "force-dynamic"

// Legacy route. CDC pipelines are created in `/chat`, which handles both batch
// and CDC; the standalone CDC chat that lived here called LLM-service routes
// (`/api/v1/cdc/v2/*`) that no service implements. Kept as a redirect so old
// links and bookmarks still land somewhere that works, prompt included.
export default async function CDCNewPipelinePage({
  searchParams,
}: {
  searchParams?: Promise<{ [key: string]: string | string[] | undefined }>
}) {
  const sp = await searchParams
  const prompt = typeof sp?.prompt === "string" ? sp.prompt.trim() : ""
  redirect(prompt ? `/chat?prompt=${encodeURIComponent(prompt)}` : "/chat")
}
