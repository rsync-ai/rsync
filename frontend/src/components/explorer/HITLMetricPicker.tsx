"use client"

import { useEffect, useMemo, useState } from "react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { cn } from "@/lib/utils"
import { HelpCircle, Hash, Sigma } from "lucide-react"

export type MetricChoice = "count" | "sum_amount"

interface HITLMetricPickerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  question: string
  options?: Array<{
    id: MetricChoice
    title: string
    description: string
    default?: boolean
  }>
  onConfirm: (choice: MetricChoice) => void
  onCancel: () => void
}

const DEFAULT_OPTIONS: HITLMetricPickerProps["options"] = [
  {
    id: "count",
    title: "Count of orders",
    description: "How many orders happened per month (COUNT).",
    default: true,
  },
  {
    id: "sum_amount",
    title: "Sum of order amount",
    description: "Total order value per month (SUM of amount).",
  },
]

export function HITLMetricPicker({
  open,
  onOpenChange,
  question,
  options = DEFAULT_OPTIONS,
  onConfirm,
  onCancel,
}: HITLMetricPickerProps) {
  const defaultChoice = useMemo<MetricChoice>(() => {
    const hit = (options || []).find((o) => o.default)?.id
    return hit || "count"
  }, [options])

  const [selected, setSelected] = useState<MetricChoice>(defaultChoice)

  useEffect(() => {
    if (!open) return
    setSelected(defaultChoice)
  }, [open, defaultChoice])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <HelpCircle className="h-5 w-5 text-violet-500" />
            Quick clarification
          </DialogTitle>
          <DialogDescription>
            Your question could mean different metrics. Pick what you intended so the query is correct.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          <div className="p-3 bg-zinc-50 dark:bg-zinc-900 rounded-lg">
            <div className="text-[10px] text-zinc-500 uppercase tracking-wider mb-1">Your Question</div>
            <p className="text-sm text-zinc-700 dark:text-zinc-300 whitespace-pre-wrap">{question}</p>
          </div>

          <div className="space-y-2">
            {(options || []).map((opt) => {
              const active = selected === opt.id
              const Icon = opt.id === "count" ? Hash : Sigma
              return (
                <button
                  key={opt.id}
                  type="button"
                  onClick={() => setSelected(opt.id)}
                  className={cn(
                    "w-full p-3 rounded-lg border text-left transition-colors",
                    active
                      ? "border-violet-500 bg-violet-50 dark:bg-violet-950/20"
                      : "border-zinc-200 dark:border-zinc-700 hover:bg-zinc-50 dark:hover:bg-zinc-900"
                  )}
                >
                  <div className="flex items-start gap-3">
                    <div className="mt-0.5">
                      <Icon className={cn("h-4 w-4", active ? "text-violet-600" : "text-zinc-400")} />
                    </div>
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm">{opt.title}</span>
                        {opt.default ? (
                          <Badge variant="secondary" className="text-[10px]">
                            Recommended
                          </Badge>
                        ) : null}
                      </div>
                      <p className="text-xs text-zinc-500 mt-1">{opt.description}</p>
                    </div>
                  </div>
                </button>
              )
            })}
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onCancel}>
            Cancel
          </Button>
          <Button
            className="bg-gradient-to-r from-violet-600 to-indigo-600"
            onClick={() => {
              onConfirm(selected)
              onOpenChange(false)
            }}
          >
            Use this metric
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

