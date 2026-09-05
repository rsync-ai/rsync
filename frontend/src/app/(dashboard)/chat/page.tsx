import { AgenticChatInterfaceV2 } from "@/components/chat/AgenticChatInterfaceV2"

export const dynamic = "force-dynamic"

export default function ChatPage() {
  return (
    <div className="flex flex-col h-[calc(100vh-8rem)]">
      {/* AgenticChatInterfaceV2 already renders a dedicated home/hero state. */}
      <AgenticChatInterfaceV2 />
    </div>
  )
}

