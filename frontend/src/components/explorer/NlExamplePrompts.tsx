"use client"

import { Sparkles } from "lucide-react"
import { getExamplePrompts, type ExampleTable } from "@/lib/explorer/examplePrompts"
import { cn } from "@/lib/utils"

export interface NlExamplePromptsProps {
  /** Known tables — examples are tailored to these when present. */
  tables?: ExampleTable[]
  /** Called with the full prompt text when a chip is clicked. */
  onSelect: (prompt: string) => void
  className?: string
}

/**
 * NlExamplePrompts — a row of clickable example chips below the NL input
 * that teach users how to phrase NL→SQL questions. Clicking a chip drops
 * its full prompt into the NL box (via `onSelect`). Examples come from the
 * pure `getExamplePrompts` helper, tailored to the connected schema.
 */
export function NlExamplePrompts({ tables = [], onSelect, className }: NlExamplePromptsProps) {
  const examples = getExamplePrompts(tables)
  if (examples.length === 0) return null

  return (
    <div className={cn("flex flex-wrap items-center gap-1.5", className)}>
      <span className="text-xs text-muted-foreground">Try:</span>
      {examples.map((ex) => (
        <button
          key={ex.label}
          type="button"
          onClick={() => onSelect(ex.prompt)}
          title={ex.prompt}
          className="inline-flex items-center gap-1 rounded-full border bg-background px-2.5 py-1 text-xs text-muted-foreground transition-colors hover:border-violet-400 hover:text-foreground"
        >
          <Sparkles className="h-3 w-3 text-violet-500" />
          {ex.label}
        </button>
      ))}
    </div>
  )
}
