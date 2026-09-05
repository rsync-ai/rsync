"use client"

import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import { RefreshCw, Zap, Download, CheckCircle2 } from "lucide-react"

export type SyncMode = "full_cdc" | "incremental" | "full_stop"

interface SyncModeSelectorProps {
  value: SyncMode
  onChange: (mode: SyncMode) => void
  disabled?: boolean
  className?: string
}

const syncModeConfig: Record<SyncMode, {
  label: string
  description: string
  details: string[]
  icon: typeof RefreshCw
  color: string
  bgColor: string
  recommended?: boolean
}> = {
  full_cdc: {
    label: "Full Sync + CDC",
    description: "Initial snapshot of all data, then continuous replication",
    details: [
      "Captures all existing data first",
      "Then syncs INSERT, UPDATE, DELETE in real-time",
      "Best for new pipelines"
    ],
    icon: RefreshCw,
    color: "text-emerald-600",
    bgColor: "bg-emerald-50 border-emerald-200 dark:bg-emerald-950/30 dark:border-emerald-800",
    recommended: true,
  },
  incremental: {
    label: "Incremental Only",
    description: "No historical data, only captures changes from now",
    details: [
      "Skips existing data",
      "Only captures new changes",
      "Best for real-time events"
    ],
    icon: Zap,
    color: "text-blue-600",
    bgColor: "bg-blue-50 border-blue-200 dark:bg-blue-950/30 dark:border-blue-800",
  },
  full_stop: {
    label: "Full Sync & Stop",
    description: "One-time export, pipeline stops after completion",
    details: [
      "Exports all current data",
      "Auto-cleans up after completion",
      "Best for migrations or backups"
    ],
    icon: Download,
    color: "text-violet-600",
    bgColor: "bg-violet-50 border-violet-200 dark:bg-violet-950/30 dark:border-violet-800",
  },
}

export function SyncModeSelector({
  value,
  onChange,
  disabled = false,
  className,
}: SyncModeSelectorProps) {
  return (
    <div className={cn("space-y-3", className)}>
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-medium text-zinc-900 dark:text-white">
          Sync Mode
        </h3>
        {value && (
          <Badge variant="outline" className={syncModeConfig[value].color}>
            {syncModeConfig[value].label}
          </Badge>
        )}
      </div>
      
      <div className="grid gap-3 md:grid-cols-3">
        {(Object.entries(syncModeConfig) as [SyncMode, typeof syncModeConfig[SyncMode]][]).map(
          ([mode, config]) => {
            const Icon = config.icon
            const isSelected = value === mode
            
            return (
              <button
                key={mode}
                type="button"
                onClick={() => !disabled && onChange(mode)}
                disabled={disabled}
                className={cn(
                  "relative flex flex-col p-4 rounded-xl border-2 text-left transition-all",
                  isSelected
                    ? config.bgColor
                    : "bg-white border-zinc-200 dark:bg-zinc-900 dark:border-zinc-800",
                  !disabled && !isSelected && "hover:border-zinc-300 dark:hover:border-zinc-700",
                  disabled && "opacity-50 cursor-not-allowed"
                )}
              >
                {/* Recommended badge */}
                {config.recommended && (
                  <Badge 
                    className="absolute -top-2 -right-2 bg-emerald-500 text-white text-xs"
                  >
                    Default
                  </Badge>
                )}
                
                {/* Selection indicator */}
                {isSelected && (
                  <div className="absolute top-3 right-3">
                    <CheckCircle2 className={cn("h-5 w-5", config.color)} />
                  </div>
                )}
                
                {/* Icon */}
                <div className={cn(
                  "flex h-10 w-10 items-center justify-center rounded-lg mb-3",
                  isSelected ? config.color : "text-zinc-400",
                  isSelected ? "bg-white/50 dark:bg-black/20" : "bg-zinc-100 dark:bg-zinc-800"
                )}>
                  <Icon className="h-5 w-5" />
                </div>
                
                {/* Content */}
                <h4 className={cn(
                  "font-medium mb-1",
                  isSelected ? config.color : "text-zinc-900 dark:text-white"
                )}>
                  {config.label}
                </h4>
                <p className="text-xs text-zinc-500 dark:text-zinc-400 mb-3">
                  {config.description}
                </p>
                
                {/* Details list */}
                <ul className="space-y-1">
                  {config.details.map((detail, idx) => (
                    <li 
                      key={idx} 
                      className="text-xs text-zinc-500 dark:text-zinc-400 flex items-start gap-1.5"
                    >
                      <span className={cn(
                        "h-1.5 w-1.5 rounded-full mt-1.5 flex-shrink-0",
                        isSelected ? config.color.replace("text-", "bg-") : "bg-zinc-300 dark:bg-zinc-600"
                      )} />
                      {detail}
                    </li>
                  ))}
                </ul>
              </button>
            )
          }
        )}
      </div>
    </div>
  )
}

