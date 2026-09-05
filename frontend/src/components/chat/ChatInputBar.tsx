"use client"

import { Send } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"

interface ChatInputBarProps {
  input: string
  onInputChange: (val: string) => void
  onSubmit: (e: React.FormEvent) => void
  isSendingMessage: boolean
  pipelineId: string | undefined
  isPipelineDone: boolean
  executionFailed: boolean
  onStartNew: () => void
  onViewPipeline: () => void
  onRetry: () => void
}

export function ChatInputBar({
  input,
  onInputChange,
  onSubmit,
  isSendingMessage,
  pipelineId,
  isPipelineDone,
  executionFailed,
  onStartNew,
  onViewPipeline,
  onRetry,
}: ChatInputBarProps) {
  const inputDisabled = (Boolean(pipelineId) && !isPipelineDone) || isSendingMessage

  if (isPipelineDone && pipelineId) {
    return (
      <div className="flex items-center gap-2 flex-wrap">
        <Button
          size="sm"
          variant="default"
          onClick={onStartNew}
          className="bg-violet-600 hover:bg-violet-700 text-white"
        >
          Start new pipeline
        </Button>
        <Button size="sm" variant="outline" onClick={onViewPipeline}>
          View pipeline details
        </Button>
        {executionFailed && (
          <Button
            size="sm"
            variant="outline"
            className="border-red-300 text-red-700 hover:bg-red-50"
            onClick={onRetry}
          >
            Retry with changes
          </Button>
        )}
      </div>
    )
  }

  return (
    <div className="flex gap-2">
      <Input
        value={input}
        onChange={(e) => onInputChange(e.target.value)}
        placeholder="Describe what data you want to move…"
        disabled={inputDisabled}
      />
      <Button type="submit" disabled={inputDisabled}>
        <Send className="h-4 w-4" />
      </Button>
    </div>
  )
}
