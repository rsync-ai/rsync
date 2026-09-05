import { redirect } from "next/navigation"

export default function NewPipelinePage() {
  // Draft/sidebar pipeline flow removed. Chat is the single pipeline creation entry point.
  redirect("/chat")
}

