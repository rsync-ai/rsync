import { PageHeader } from "@/components/layout/PageHeader"
import { CDCAgentChatWrapper } from "./CDCAgentChatWrapper"
import { redirect } from "next/navigation"

export const dynamic = "force-dynamic"

export default async function CDCNewPipelinePage({
  searchParams,
}: {
  searchParams?: Promise<{ [key: string]: string | string[] | undefined }>
}) {
  // Backwards-compat + safety net:
  // Older Home quick-prompts used to deep-link here with `?prompt=...`.
  // The canonical agentic flow is `/chat`, which supports both batch + CDC.
  const sp = await searchParams
  const prompt = typeof sp?.prompt === "string" ? sp.prompt.trim() : ""
  if (prompt) {
    redirect(`/chat?prompt=${encodeURIComponent(prompt)}`)
  }

  return (
    <div className="flex flex-col h-[calc(100vh-8rem)]">
      <PageHeader
        heading="Create CDC Pipeline"
        description="Use AI to set up Change Data Capture from your database"
      />
      <div className="flex-1 mt-6">
        <CDCAgentChatWrapper />
      </div>
    </div>
  )
}

