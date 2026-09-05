import { Suspense } from "react"
import { redirect } from "next/navigation"
import { PipelinesPageClient } from "./PipelinesPageClient"

export const dynamic = "force-dynamic"

export default async function PipelinesPage({
  searchParams,
}: {
  searchParams?: Promise<{ draft_id?: string; prompt?: string }>
}) {
  // Compatibility: old deep-links used `/pipelines?draft_id=...` for the deprecated sidebar panel.
  // Always hard-redirect to the full /chat experience.
  const sp = await searchParams
  if (sp?.draft_id) {
    const prompt = sp.prompt
    redirect(prompt ? `/chat?prompt=${encodeURIComponent(prompt)}` : "/chat")
  }

  return (
    <Suspense fallback={<div className="p-6 text-sm text-zinc-500">Loading pipelines…</div>}>
      <PipelinesPageClient />
    </Suspense>
  )
}
