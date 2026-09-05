"use client"

import { useEffect, useState } from "react"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Table2,
  CheckCircle2,
  AlertTriangle,
  Database,
  Columns,
  Info,
} from "lucide-react"

export interface TableCandidate {
  table: string
  schema_name: string
  confidence: number
  reason: string
  columns?: { name: string; type: string; is_primary_key?: boolean }[]
  row_count?: number
}

interface HITLTablePickerProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  candidates: TableCandidate[]
  question: string
  onConfirm: (selectedTables: string[]) => void
  onCancel: () => void
}

export function HITLTablePicker({
  open,
  onOpenChange,
  candidates,
  question,
  onConfirm,
  onCancel,
}: HITLTablePickerProps) {
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set())

  // Reset default selections whenever the dialog opens or candidates change.
  // Without this, a previous selection can "stick" across different questions.
  //
  // Auto-select ONLY a single, unambiguous top candidate. Previously every
  // candidate at >=0.7 was pre-checked, which silently selected unrelated tables
  // (e.g. `demo_customers` @0.80 for a "rows in customers" question) and produced
  // a wrong answer if the user trusted the defaults. Require a clear winner: top
  // confidence >= 0.85 AND at least 0.15 ahead of the runner-up; otherwise select
  // none and let the user choose (Confirm is disabled while the selection is empty).
  useEffect(() => {
    if (!open) return
    const sorted = [...candidates].sort((a, b) => b.confidence - a.confidence)
    const top = sorted[0]
    const runnerUp = sorted[1]
    const clearWinner =
      top &&
      top.confidence >= 0.85 &&
      (!runnerUp || top.confidence - runnerUp.confidence >= 0.15)
    setSelectedTables(
      new Set(clearWinner ? [`${top.schema_name}.${top.table}`] : [])
    )
  }, [open, candidates])

  const toggleTable = (fullName: string) => {
    setSelectedTables((prev) => {
      const next = new Set(prev)
      if (next.has(fullName)) {
        next.delete(fullName)
      } else {
        next.add(fullName)
      }
      return next
    })
  }

  const handleConfirm = () => {
    onConfirm(Array.from(selectedTables))
    onOpenChange(false)
  }

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.8) return "text-emerald-600 bg-emerald-50 dark:bg-emerald-950/30"
    if (confidence >= 0.5) return "text-amber-600 bg-amber-50 dark:bg-amber-950/30"
    return "text-red-600 bg-red-50 dark:bg-red-950/30"
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl max-h-[90vh] flex flex-col overflow-hidden">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Table2 className="h-5 w-5 text-violet-500" />
            Select Tables
          </DialogTitle>
          <DialogDescription>
            The system identified multiple tables that might be relevant to your question.
            Please confirm which tables should be used.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-4 py-4 pr-1">
          {/* Question */}
          <div className="p-3 bg-zinc-50 dark:bg-zinc-900 rounded-lg">
            <div className="text-[10px] text-zinc-500 uppercase tracking-wider mb-1">
              Your Question
            </div>
            <p className="text-sm text-zinc-700 dark:text-zinc-300">{question}</p>
          </div>

          {/* Table Candidates */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">
                Candidate Tables ({candidates.length})
              </label>
              <div className="flex items-center gap-2 text-xs text-zinc-500">
                <Info className="h-3 w-3" />
                Higher confidence = better match
              </div>
            </div>

            <ScrollArea className="h-[40vh] max-h-[300px] min-h-[160px] border rounded-lg">
              <div className="p-2 space-y-2">
                {candidates.map((candidate) => {
                  const fullName = `${candidate.schema_name}.${candidate.table}`
                  const isSelected = selectedTables.has(fullName)

                  return (
                    <div
                      key={fullName}
                      className={cn(
                        "p-3 rounded-lg border cursor-pointer transition-colors",
                        isSelected
                          ? "border-violet-500 bg-violet-50 dark:bg-violet-950/20"
                          : "border-zinc-200 dark:border-zinc-700 hover:bg-zinc-50 dark:hover:bg-zinc-900"
                      )}
                      onClick={() => toggleTable(fullName)}
                    >
                      <div className="flex items-start gap-3">
                        <Checkbox
                          checked={isSelected}
                          onClick={(e) => e.stopPropagation()}
                          onCheckedChange={() => toggleTable(fullName)}
                          className="mt-1"
                        />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <Database className="h-4 w-4 text-zinc-400" />
                            <span className="font-medium text-sm">
                              {candidate.schema_name}.{candidate.table}
                            </span>
                            <Badge
                              variant="secondary"
                              className={cn("text-[10px]", getConfidenceColor(candidate.confidence))}
                            >
                              {Math.round(candidate.confidence * 100)}% match
                            </Badge>
                          </div>
                          <p className="text-xs text-zinc-500 mt-1">{candidate.reason}</p>
                          {candidate.columns && candidate.columns.length > 0 && (
                            <div className="flex items-center gap-1 mt-2 flex-wrap">
                              <Columns className="h-3 w-3 text-zinc-400" />
                              {candidate.columns.slice(0, 5).map((col) => (
                                <Badge
                                  key={col.name}
                                  variant="outline"
                                  className={cn(
                                    "text-[9px] py-0",
                                    col.is_primary_key && "border-violet-300 text-violet-600"
                                  )}
                                >
                                  {col.name}
                                </Badge>
                              ))}
                              {candidate.columns.length > 5 && (
                                <span className="text-[10px] text-zinc-400">
                                  +{candidate.columns.length - 5} more
                                </span>
                              )}
                            </div>
                          )}
                        </div>
                        {candidate.confidence >= 0.8 ? (
                          <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                        ) : candidate.confidence >= 0.5 ? (
                          <AlertTriangle className="h-4 w-4 text-amber-500" />
                        ) : null}
                      </div>
                    </div>
                  )
                })}
              </div>
            </ScrollArea>
          </div>

          {/* Selection Summary */}
          {selectedTables.size > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xs text-zinc-500">Selected:</span>
              {Array.from(selectedTables).map((t) => (
                <Badge key={t} variant="secondary" className="text-xs">
                  {t}
                </Badge>
              ))}
            </div>
          )}
        </div>

        <DialogFooter className="shrink-0">
          <Button variant="outline" onClick={onCancel}>
            Cancel
          </Button>
          <Button
            onClick={handleConfirm}
            disabled={selectedTables.size === 0}
            className="bg-gradient-to-r from-violet-600 to-indigo-600"
          >
            Confirm Selection ({selectedTables.size})
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
