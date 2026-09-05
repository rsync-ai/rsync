"use client"

import { useState } from "react"
import { motion, AnimatePresence } from "framer-motion"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import {
  Sparkles,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Flame,
  Database,
  Clock,
  AlertTriangle,
  Zap,
  Settings,
  Layers,
  SkipForward,
} from "lucide-react"
import { SyncModeSelector, type SyncMode } from "./SyncModeSelector"

// =============================================================================
// TYPES
// =============================================================================

interface TableRecommendation {
  name: string
  full_name?: string
  priority: "high" | "medium" | "low"
  reason: string
  estimated_rows?: number
  has_primary_key?: boolean
}

interface SmartRecommendationsCardProps {
  recommendations: {
    recommended_tables?: TableRecommendation[]
    recommended_sync_mode?: string
    warnings?: string[]
    tips?: string[]
    total_tables?: number
    skipped_count?: number
  }
  syncMode?: SyncMode
  onSyncModeChange?: (mode: SyncMode) => void
  onApprove?: () => void
  onModify?: () => void
  onViewAll?: () => void
  className?: string
}

// =============================================================================
// HELPERS
// =============================================================================

function formatNumber(num: number | undefined): string {
  if (!num) return "0"
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`
  return num.toString()
}

const priorityConfig = {
  high: {
    label: "High Priority",
    icon: Flame,
    color: "text-orange-600 dark:text-orange-400",
    bg: "bg-orange-100 dark:bg-orange-900/30",
    border: "border-orange-200 dark:border-orange-800",
    description: "Critical for real-time operations",
  },
  medium: {
    label: "Standard",
    icon: Database,
    color: "text-blue-600 dark:text-blue-400",
    bg: "bg-blue-100 dark:bg-blue-900/30",
    border: "border-blue-200 dark:border-blue-800",
    description: "Regular data tables",
  },
  low: {
    label: "Optional",
    icon: Clock,
    color: "text-zinc-500 dark:text-zinc-400",
    bg: "bg-zinc-100 dark:bg-zinc-800",
    border: "border-zinc-200 dark:border-zinc-700",
    description: "Can be synced later",
  },
}

const syncModeDescriptions: Record<string, { label: string; description: string; icon: typeof Zap }> = {
  full_sync: {
    label: "Full Sync",
    description: "Load existing data, then stream changes",
    icon: Layers,
  },
  incremental: {
    label: "Incremental",
    description: "Only sync new/changed records",
    icon: Zap,
  },
  cdc: {
    label: "CDC Only",
    description: "Stream changes without initial load",
    icon: Zap,
  },
}

// =============================================================================
// TABLE GROUP COMPONENT
// =============================================================================

function TableGroup({
  priority,
  tables,
  expanded,
  onToggle,
}: {
  priority: "high" | "medium" | "low"
  tables: TableRecommendation[]
  expanded: boolean
  onToggle: () => void
}) {
  const config = priorityConfig[priority]
  const Icon = config.icon
  const totalRows = tables.reduce((sum, t) => sum + (t.estimated_rows || 0), 0)

  return (
    <div className={cn("rounded-lg border", config.border)}>
      {/* Group Header */}
      <button
        onClick={onToggle}
        className={cn(
          "w-full flex items-center justify-between p-3 text-left transition-colors",
          config.bg
        )}
      >
        <div className="flex items-center gap-3">
          <div className={cn("p-1.5 rounded", config.bg)}>
            <Icon className={cn("h-4 w-4", config.color)} />
          </div>
          <div>
            <span className={cn("font-medium text-sm", config.color)}>
              {config.label}
            </span>
            <span className="text-xs text-zinc-500 ml-2">
              {tables.length} tables • {formatNumber(totalRows)} rows
            </span>
          </div>
        </div>
        {expanded ? (
          <ChevronDown className="h-4 w-4 text-zinc-400" />
        ) : (
          <ChevronRight className="h-4 w-4 text-zinc-400" />
        )}
      </button>

      {/* Expanded Table List */}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="overflow-hidden"
          >
            <div className="p-3 pt-0 space-y-1">
              {tables.slice(0, 10).map((table) => (
                <div
                  key={table.name}
                  className="flex items-center justify-between py-2 px-3 rounded-lg hover:bg-zinc-50 dark:hover:bg-zinc-900/50"
                >
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-sm text-zinc-900 dark:text-white">
                      {table.name}
                    </span>
                    {table.has_primary_key && (
                      <Badge variant="outline" className="text-xs h-5">
                        PK
                      </Badge>
                    )}
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-zinc-500">
                      {formatNumber(table.estimated_rows)} rows
                    </span>
                    <span className="text-xs text-zinc-400 max-w-[150px] truncate">
                      {table.reason}
                    </span>
                  </div>
                </div>
              ))}
              {tables.length > 10 && (
                <div className="text-center py-2 text-sm text-zinc-500">
                  +{tables.length - 10} more tables
                </div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

// =============================================================================
// MAIN COMPONENT
// =============================================================================

export function SmartRecommendationsCard({
  recommendations,
  syncMode: initialSyncMode = "full_cdc",
  onSyncModeChange,
  onApprove,
  onModify,
  onViewAll,
  className,
}: SmartRecommendationsCardProps) {
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set(["high"]))
  const [localSyncMode, setLocalSyncMode] = useState<SyncMode>(initialSyncMode)
  
  const handleSyncModeChange = (mode: SyncMode) => {
    setLocalSyncMode(mode)
    onSyncModeChange?.(mode)
  }

  const tables = recommendations.recommended_tables || []
  const recommendedSyncMode = recommendations.recommended_sync_mode || "full_sync"
  const syncModeInfo = syncModeDescriptions[recommendedSyncMode] || syncModeDescriptions.full_sync
  const SyncIcon = syncModeInfo.icon

  // Group tables by priority
  const highPriority = tables.filter((t) => t.priority === "high")
  const mediumPriority = tables.filter((t) => t.priority === "medium")
  const lowPriority = tables.filter((t) => t.priority === "low")

  const totalSelected = tables.length
  const totalRows = tables.reduce((sum, t) => sum + (t.estimated_rows || 0), 0)
  const skippedCount = recommendations.skipped_count || 0

  const toggleGroup = (group: string) => {
    const newExpanded = new Set(expandedGroups)
    if (newExpanded.has(group)) {
      newExpanded.delete(group)
    } else {
      newExpanded.add(group)
    }
    setExpandedGroups(newExpanded)
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className={className}
    >
      <Card className="border-zinc-200 dark:border-zinc-800 overflow-hidden">
        {/* Gradient Header */}
        <div className="h-1.5 bg-gradient-to-r from-amber-500 to-orange-500" />

        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg flex items-center gap-2">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-amber-500 to-orange-500">
                <Sparkles className="h-4 w-4 text-white" />
              </div>
              AI Recommendations
            </CardTitle>
            <Badge variant="secondary" className="font-mono">
              {totalSelected} tables selected
            </Badge>
          </div>
        </CardHeader>

        <CardContent className="space-y-4">
          {/* Summary */}
          <div className="flex items-center gap-4 p-3 rounded-lg bg-zinc-50 dark:bg-zinc-900/50">
            <div className="flex items-center gap-2">
              <SyncIcon className="h-5 w-5 text-violet-600" />
              <div>
                <p className="text-sm font-medium">{syncModeInfo.label}</p>
                <p className="text-xs text-zinc-500">{syncModeInfo.description}</p>
              </div>
            </div>
            <div className="ml-auto text-right">
              <p className="text-lg font-bold">{formatNumber(totalRows)}</p>
              <p className="text-xs text-zinc-500">total rows</p>
            </div>
          </div>

          {/* Priority Groups */}
          <div className="space-y-2">
            {highPriority.length > 0 && (
              <TableGroup
                priority="high"
                tables={highPriority}
                expanded={expandedGroups.has("high")}
                onToggle={() => toggleGroup("high")}
              />
            )}
            {mediumPriority.length > 0 && (
              <TableGroup
                priority="medium"
                tables={mediumPriority}
                expanded={expandedGroups.has("medium")}
                onToggle={() => toggleGroup("medium")}
              />
            )}
            {lowPriority.length > 0 && (
              <TableGroup
                priority="low"
                tables={lowPriority}
                expanded={expandedGroups.has("low")}
                onToggle={() => toggleGroup("low")}
              />
            )}
          </div>

          {/* Sync Mode Selector - Always visible */}
          <div className="pt-4 border-t border-zinc-200 dark:border-zinc-800">
            <SyncModeSelector
              value={localSyncMode}
              onChange={handleSyncModeChange}
            />
          </div>

          {/* Skipped Tables Info */}
          {skippedCount > 0 && (
            <div className="flex items-center gap-2 p-3 rounded-lg bg-zinc-100 dark:bg-zinc-800">
              <SkipForward className="h-4 w-4 text-zinc-400" />
              <span className="text-sm text-zinc-500">
                <strong>{skippedCount}</strong> tables skipped (empty, system, or audit tables)
              </span>
              {onViewAll && (
                <Button variant="link" size="sm" onClick={onViewAll} className="ml-auto">
                  View all tables
                </Button>
              )}
            </div>
          )}

          {/* Warnings */}
          {recommendations.warnings && recommendations.warnings.length > 0 && (
            <div className="space-y-1">
              {recommendations.warnings.slice(0, 2).map((warning, i) => (
                <div
                  key={i}
                  className="flex items-start gap-2 p-2 rounded-lg bg-amber-50 dark:bg-amber-900/20"
                >
                  <AlertTriangle className="h-4 w-4 text-amber-500 mt-0.5 shrink-0" />
                  <span className="text-xs text-amber-700 dark:text-amber-300">{warning}</span>
                </div>
              ))}
            </div>
          )}

          {/* Actions */}
          <div className="flex gap-3 pt-2">
            <Button
              onClick={onApprove}
              className="flex-1 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700"
            >
              <CheckCircle2 className="h-4 w-4 mr-2" />
              Approve & Continue
            </Button>
            <Button variant="outline" onClick={onModify}>
              <Settings className="h-4 w-4 mr-2" />
              Modify Selection
            </Button>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

