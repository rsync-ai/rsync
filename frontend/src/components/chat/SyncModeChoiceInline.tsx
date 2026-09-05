"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  RefreshCw,
  Zap,
  Package,
  ArrowRight,
  Database,
  Cloud,
  X,
  CheckCircle2,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { displayConnectorName } from "@/lib/connector-display"

interface SyncModeOption {
  id: string
  label: string
}

interface SyncModeChoiceInlineProps {
  message: string
  choiceType: string
  options: SyncModeOption[]
  sourceType: string
  destType: string
  sourceConnectionId?: string
  destConnectionId?: string
  tables?: string[]
  initialChosenLabel?: string
  onChoice: (choiceId: string, context: any, label: string) => void
  onCancel?: () => void
}

export function SyncModeChoiceInline({
  message,
  choiceType,
  options,
  sourceType,
  destType,
  sourceConnectionId,
  destConnectionId,
  tables,
  initialChosenLabel,
  onChoice,
  onCancel,
}: SyncModeChoiceInlineProps) {
  // Do NOT pre-select a mode when the user has a real choice. Previously this
  // defaulted to options[0] (batch), so a user who explicitly asked for CDC could
  // click "Start pipeline" and silently launch a BATCH pipeline. Pre-select only
  // when there is a single option; otherwise force an explicit pick (the Start
  // button is disabled while nothing is selected).
  const [selectedOption, setSelectedOption] = useState<string | null>(
    () => (options.length === 1 ? options[0]?.id ?? null : null)
  )
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [chosenLabel, setChosenLabel] = useState<string | null>(initialChosenLabel ?? null)

  const handleSelect = (optionId: string) => {
    setSelectedOption(optionId)
  }

  const handleConfirm = async () => {
    if (!selectedOption) return
    setIsSubmitting(true)
    const label = options.find((o) => o.id === selectedOption)?.label ?? selectedOption
    try {
      await onChoice(
        selectedOption,
        {
          choice_type: choiceType,
          source_type: sourceType,
          dest_type: destType,
          source_connection_id: sourceConnectionId,
          dest_connection_id: destConnectionId,
          tables: tables,
        },
        label,
      )
      setChosenLabel(label)
    } finally {
      setIsSubmitting(false)
    }
  }

  // Get icon for option based on its ID
  const getOptionIcon = (optionId: string) => {
    if (optionId.includes('batch') || optionId.includes('one_time')) {
      return <Package className="h-5 w-5" />
    }
    if (optionId.includes('cdc') || optionId.includes('continuous') || optionId.includes('stream')) {
      return <Zap className="h-5 w-5" />
    }
    if (optionId.includes('initial')) {
      return <RefreshCw className="h-5 w-5" />
    }
    return <ArrowRight className="h-5 w-5" />
  }

  // Get color for option based on its ID
  const getOptionColor = (optionId: string) => {
    if (optionId.includes('batch') || optionId.includes('one_time')) {
      return "from-blue-500 to-blue-600"
    }
    if (optionId.includes('cdc') || optionId.includes('continuous')) {
      return "from-green-500 to-emerald-600"
    }
    if (optionId.includes('initial')) {
      return "from-violet-500 to-purple-600"
    }
    return "from-zinc-500 to-zinc-600"
  }

  // Get description for option
  const getOptionDescription = (optionId: string) => {
    if (optionId === 'batch' || optionId === 'use_batch' || optionId === 'one_time') {
      return "Copy selected tables in batches (snapshot now; incremental resume on reruns)"
    }
    if (optionId === 'cdc' || optionId === 'enable_cdc' || optionId === 'continuous') {
      return "Capture and stream changes in real-time using CDC"
    }
    if (optionId === 'initial_plus_cdc') {
      return "First export all existing data, then stream ongoing changes"
    }
    if (optionId === 'cdc_changes_only' || optionId === 'streaming_only' || optionId === 'cdc_streaming_only') {
      return "Stream new INSERT/UPDATE/DELETE only (no historical backfill)"
    }
    if (optionId === 'create_cdc') {
      return "Create a new CDC-enabled connection for streaming"
    }
    return ""
  }

  if (chosenLabel !== null) {
    return (
      <div className="mt-3 flex items-center gap-2 text-sm text-zinc-500 dark:text-zinc-400">
        <CheckCircle2 className="h-4 w-4 shrink-0 text-green-500" />
        <span>
          Sync mode: <span className="font-medium text-zinc-700 dark:text-zinc-200">{chosenLabel}</span>
        </span>
      </div>
    )
  }

  return (
    <Card className="mt-4 border-violet-200 dark:border-violet-900 bg-gradient-to-br from-white to-violet-50 dark:from-zinc-900 dark:to-violet-950/20">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-gradient-to-br from-violet-500 to-indigo-600">
              <RefreshCw className="h-5 w-5 text-white" />
            </div>
            <div>
              <CardTitle className="text-lg">
                {options.length > 1 ? "Choose Sync Mode" : "Confirm Pipeline"}
              </CardTitle>
              <p className="text-sm text-zinc-500 dark:text-zinc-400">
                {message}
              </p>
            </div>
          </div>
          {onCancel && (
            <Button variant="ghost" size="icon" onClick={onCancel}>
              <X className="h-4 w-4" />
            </Button>
          )}
        </div>
        
        {/* Source to Destination indicator */}
        <div className="flex items-center gap-2 mt-4 p-3 bg-white dark:bg-zinc-800 rounded-lg border border-zinc-200 dark:border-zinc-700">
          <div className="flex items-center gap-2">
            <Database className="h-4 w-4 text-blue-500" />
            <span className="font-medium">{displayConnectorName(sourceType)}</span>
          </div>
          <ArrowRight className="h-4 w-4 text-zinc-400" />
          <div className="flex items-center gap-2">
            <Cloud className="h-4 w-4 text-orange-500" />
            <span className="font-medium">{displayConnectorName(destType)}</span>
          </div>
          {tables && tables.length > 0 && (
            <Badge variant="secondary" className="ml-2">
              {tables.length} table{tables.length > 1 ? 's' : ''}
            </Badge>
          )}
        </div>
      </CardHeader>
      
      <CardContent className="space-y-3">
        {options.map((option) => (
          <button
            key={option.id}
            onClick={() => handleSelect(option.id)}
            disabled={isSubmitting}
            className={cn(
              "w-full flex items-center gap-4 p-4 rounded-xl border-2 transition-all text-left",
              selectedOption === option.id
                ? "border-violet-500 bg-violet-50 dark:bg-violet-950/30"
                : "border-zinc-200 dark:border-zinc-700 hover:border-violet-300 hover:bg-violet-50/50 dark:hover:border-violet-800 dark:hover:bg-violet-950/20",
              isSubmitting && "opacity-50 cursor-not-allowed"
            )}
          >
            <div className={cn(
              "flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br text-white shrink-0",
              getOptionColor(option.id)
            )}>
              {getOptionIcon(option.id)}
            </div>
            <div className="flex-1">
              <p className="font-medium text-zinc-900 dark:text-white">
                {option.label}
              </p>
              <p className="text-sm text-zinc-500 dark:text-zinc-400 mt-0.5">
                {getOptionDescription(option.id)}
              </p>
            </div>
            {selectedOption === option.id && isSubmitting && (
              <div className="animate-spin h-5 w-5 border-2 border-violet-500 border-t-transparent rounded-full" />
            )}
          </button>
        ))}

        <div className="pt-2 flex items-center justify-end gap-2">
          <Button
            type="button"
            variant="outline"
            onClick={onCancel}
            disabled={isSubmitting}
          >
            Cancel
          </Button>
          <Button
            type="button"
            onClick={() => void handleConfirm()}
            disabled={!selectedOption || isSubmitting}
            className="bg-gradient-to-r from-violet-600 to-indigo-600"
          >
            Start pipeline
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
