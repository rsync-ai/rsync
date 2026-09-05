"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { cn } from "@/lib/utils"
import { motion, AnimatePresence } from "framer-motion"
import {
  Shield,
  EyeOff,
  Filter,
  Columns,
  Hash,
  Trash2,
  Plus,
  ChevronDown,
  ChevronRight,
  AlertTriangle,
  CheckCircle2,
  Sparkles,
  Settings,
  X,
} from "lucide-react"
import { SyncModeSelector, type SyncMode } from "./SyncModeSelector"
import { RowFilterBuilder, type RowFilter } from "./RowFilterBuilder"

// =============================================================================
// TYPES
// =============================================================================

export type TransformType = "mask" | "exclude" | "filter" | "hash" | "rename" | "computed"

export interface Transform {
  id: string
  type: TransformType
  table: string
  column?: string
  config: Record<string, any>
  description: string
}

export interface SensitivityItem {
  table: string
  column: string
  type: string
  recommendation: string
}

export interface SensitivityAnalysis {
  high_risk: SensitivityItem[]
  medium_risk: SensitivityItem[]
  low_risk: SensitivityItem[]
}

export interface SuggestedTransform {
  type: TransformType
  table: string
  column: string
  pattern?: string
  description: string
}

interface TransformationConfiguratorProps {
  sensitivityAnalysis?: SensitivityAnalysis
  suggestedTransforms?: SuggestedTransform[]
  tables?: { name: string; columns: string[] }[]
  onApply: (config: {
    transforms: Transform[]
    syncMode: SyncMode
    rowFilters: RowFilter[]
  }) => void
  onSkip: () => void
  className?: string
}

// =============================================================================
// TRANSFORM TYPE CONFIG
// =============================================================================

const transformTypeConfig: Record<TransformType, {
  label: string
  icon: typeof Shield
  color: string
  description: string
}> = {
  mask: {
    label: "Mask",
    icon: EyeOff,
    color: "bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300",
    description: "Partially hide sensitive data (e.g., j***@e*****.com)",
  },
  exclude: {
    label: "Exclude",
    icon: Trash2,
    color: "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300",
    description: "Completely remove column from sync",
  },
  filter: {
    label: "Filter Rows",
    icon: Filter,
    color: "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300",
    description: "Only sync rows matching a condition",
  },
  hash: {
    label: "Hash",
    icon: Hash,
    color: "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300",
    description: "Replace with SHA-256 hash (consistent, one-way)",
  },
  rename: {
    label: "Rename",
    icon: Columns,
    color: "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300",
    description: "Rename column in destination",
  },
  computed: {
    label: "Computed",
    icon: Sparkles,
    color: "bg-pink-100 text-pink-700 dark:bg-pink-900/30 dark:text-pink-300",
    description: "Add a computed column (e.g., total = price * qty)",
  },
}

// =============================================================================
// COMPONENTS
// =============================================================================

function TransformTypeBadge({ type }: { type: TransformType }) {
  const config = transformTypeConfig[type]
  const Icon = config.icon

  return (
    <Badge className={cn("gap-1", config.color)}>
      <Icon className="h-3 w-3" />
      {config.label}
    </Badge>
  )
}

function SensitivityCard({
  level,
  items,
  onAddTransform,
}: {
  level: "high" | "medium" | "low"
  items: SensitivityItem[]
  onAddTransform: (item: SensitivityItem, type: TransformType) => void
}) {
  const [expanded, setExpanded] = useState(level === "high")

  if (items.length === 0) return null

  const config = {
    high: {
      label: "High Risk",
      icon: AlertTriangle,
      color: "text-red-600 dark:text-red-400",
      bg: "bg-red-50 dark:bg-red-950/30",
      border: "border-red-200 dark:border-red-800",
    },
    medium: {
      label: "Medium Risk",
      icon: AlertTriangle,
      color: "text-amber-600 dark:text-amber-400",
      bg: "bg-amber-50 dark:bg-amber-950/30",
      border: "border-amber-200 dark:border-amber-800",
    },
    low: {
      label: "Low Risk",
      icon: CheckCircle2,
      color: "text-zinc-500 dark:text-zinc-400",
      bg: "bg-zinc-50 dark:bg-zinc-900/50",
      border: "border-zinc-200 dark:border-zinc-800",
    },
  }

  const { label, icon: Icon, color, bg, border } = config[level]

  return (
    <div className={cn("rounded-lg border", border)}>
      <button
        onClick={() => setExpanded(!expanded)}
        className={cn("w-full flex items-center justify-between p-3 text-left", bg)}
      >
        <div className="flex items-center gap-2">
          <Icon className={cn("h-4 w-4", color)} />
          <span className={cn("font-medium text-sm", color)}>{label}</span>
          <span className="text-xs text-zinc-500">({items.length} columns)</span>
        </div>
        {expanded ? (
          <ChevronDown className="h-4 w-4 text-zinc-400" />
        ) : (
          <ChevronRight className="h-4 w-4 text-zinc-400" />
        )}
      </button>

      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="overflow-hidden"
          >
            <div className="p-3 space-y-2">
              {items.map((item, idx) => (
                <div
                  key={`${item.table}-${item.column}-${idx}`}
                  className="flex items-center justify-between p-2 rounded-lg bg-white dark:bg-zinc-900"
                >
                  <div>
                    <span className="font-mono text-sm">
                      {item.table}.{item.column}
                    </span>
                    <p className="text-xs text-zinc-500">{item.type.replace("_", " ")}</p>
                  </div>
                  <div className="flex items-center gap-1">
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => onAddTransform(item, "mask")}
                    >
                      <EyeOff className="h-3 w-3 mr-1" />
                      Mask
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => onAddTransform(item, "exclude")}
                    >
                      <Trash2 className="h-3 w-3 mr-1" />
                      Exclude
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

function TransformCard({
  transform,
  onRemove,
  onUpdate,
}: {
  transform: Transform
  onRemove: () => void
  onUpdate: (config: Record<string, any>) => void
}) {
  const config = transformTypeConfig[transform.type]
  const Icon = config.icon

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -10 }}
      className="p-3 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900"
    >
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-3">
          <div className={cn("p-2 rounded-lg", config.color)}>
            <Icon className="h-4 w-4" />
          </div>
          <div>
            <p className="font-medium text-sm text-zinc-900 dark:text-white">
              {transform.description}
            </p>
            <p className="text-xs text-zinc-500 font-mono">
              {transform.table}.{transform.column}
            </p>
          </div>
        </div>
        <Button variant="ghost" size="sm" onClick={onRemove} className="h-7 w-7 p-0">
          <X className="h-4 w-4 text-zinc-400" />
        </Button>
      </div>

      {/* Config options based on type */}
      {transform.type === "mask" && (
        <div className="mt-3 flex items-center gap-2 pl-11">
          <Label className="text-xs text-zinc-500">Pattern:</Label>
          <select
            value={transform.config.pattern || "partial"}
            onChange={(e) => onUpdate({ ...transform.config, pattern: e.target.value })}
            className="text-xs border rounded px-2 py-1 bg-white dark:bg-zinc-800"
          >
            <option value="partial">Partial (j***@e*****.com)</option>
            <option value="full">Full (************)</option>
            <option value="first3">First 3 chars (joh***)</option>
          </select>
        </div>
      )}

      {transform.type === "filter" && (
        <div className="mt-3 pl-11">
          <Label className="text-xs text-zinc-500">Condition:</Label>
          <Input
            value={transform.config.predicate || ""}
            onChange={(e) => onUpdate({ ...transform.config, predicate: e.target.value })}
            placeholder="e.g., created_at > NOW() - INTERVAL '30 days'"
            className="mt-1 text-xs h-8"
          />
        </div>
      )}

      {transform.type === "rename" && (
        <div className="mt-3 pl-11 flex items-center gap-2">
          <Label className="text-xs text-zinc-500">New name:</Label>
          <Input
            value={transform.config.newName || ""}
            onChange={(e) => onUpdate({ ...transform.config, newName: e.target.value })}
            placeholder="new_column_name"
            className="text-xs h-8 w-40"
          />
        </div>
      )}

      {transform.type === "computed" && (
        <div className="mt-3 pl-11 space-y-2">
          <div className="flex items-center gap-2">
            <Label className="text-xs text-zinc-500">Column name:</Label>
            <Input
              value={transform.config.columnName || ""}
              onChange={(e) => onUpdate({ ...transform.config, columnName: e.target.value })}
              placeholder="order_total"
              className="text-xs h-8 w-32"
            />
          </div>
          <div className="flex items-center gap-2">
            <Label className="text-xs text-zinc-500">Expression:</Label>
            <Input
              value={transform.config.expression || ""}
              onChange={(e) => onUpdate({ ...transform.config, expression: e.target.value })}
              placeholder="quantity * unit_price"
              className="text-xs h-8 flex-1"
            />
          </div>
        </div>
      )}
    </motion.div>
  )
}

// =============================================================================
// MAIN COMPONENT
// =============================================================================

export function TransformationConfigurator({
  sensitivityAnalysis,
  suggestedTransforms,
  tables = [],
  onApply,
  onSkip,
  className,
}: TransformationConfiguratorProps) {
  const [transforms, setTransforms] = useState<Transform[]>([])
  const [syncMode, setSyncMode] = useState<SyncMode>("full_cdc")
  const [rowFilters, setRowFilters] = useState<RowFilter[]>([])
  const [showAddForm, setShowAddForm] = useState(false)

  const addTransform = (item: SensitivityItem, type: TransformType) => {
    const config = transformTypeConfig[type]
    const newTransform: Transform = {
      id: `${Date.now()}`,
      type,
      table: item.table,
      column: item.column,
      config: type === "mask" ? { pattern: "partial" } : {},
      description: `${config.label} ${item.column} in ${item.table}`,
    }
    setTransforms([...transforms, newTransform])
  }

  const addSuggestedTransform = (suggested: SuggestedTransform) => {
    const config = transformTypeConfig[suggested.type]
    const newTransform: Transform = {
      id: `${Date.now()}`,
      type: suggested.type,
      table: suggested.table,
      column: suggested.column,
      config: suggested.pattern ? { pattern: suggested.pattern } : {},
      description: suggested.description,
    }
    setTransforms([...transforms, newTransform])
  }

  const removeTransform = (id: string) => {
    setTransforms(transforms.filter((t) => t.id !== id))
  }

  const updateTransform = (id: string, config: Record<string, any>) => {
    setTransforms(
      transforms.map((t) => (t.id === id ? { ...t, config } : t))
    )
  }

  const applyAllSuggested = () => {
    if (!suggestedTransforms) return
    const newTransforms = suggestedTransforms.map((suggested, idx) => ({
      id: `suggested-${idx}`,
      type: suggested.type,
      table: suggested.table,
      column: suggested.column,
      config: suggested.pattern ? { pattern: suggested.pattern } : {},
      description: suggested.description,
    }))
    setTransforms([...transforms, ...newTransforms])
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className={className}
    >
      <Card className="border-zinc-200 dark:border-zinc-800 overflow-hidden">
        {/* Gradient Header */}
        <div className="h-1.5 bg-gradient-to-r from-violet-500 to-purple-500" />

        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg flex items-center gap-2">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-violet-500 to-purple-500">
                <Shield className="h-4 w-4 text-white" />
              </div>
              Data Transformations
            </CardTitle>
            {suggestedTransforms && suggestedTransforms.length > 0 && (
              <Button
                variant="outline"
                size="sm"
                onClick={applyAllSuggested}
                className="gap-1"
              >
                <Sparkles className="h-3 w-3" />
                Apply All Suggested
              </Button>
            )}
          </div>
          <p className="text-sm text-zinc-500 mt-2">
            Configure data transformations before syncing to destination
          </p>
        </CardHeader>

        <CardContent className="space-y-4">
          {/* Sensitivity Analysis */}
          {sensitivityAnalysis && (
            <div className="space-y-2">
              <p className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
                Sensitive Data Detected
              </p>
              <SensitivityCard
                level="high"
                items={sensitivityAnalysis.high_risk}
                onAddTransform={addTransform}
              />
              <SensitivityCard
                level="medium"
                items={sensitivityAnalysis.medium_risk}
                onAddTransform={addTransform}
              />
            </div>
          )}

          {/* Suggested Transforms */}
          {suggestedTransforms && suggestedTransforms.length > 0 && (
            <div className="space-y-2">
              <p className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
                AI Suggested Transforms
              </p>
              <div className="flex flex-wrap gap-2">
                {suggestedTransforms.map((suggested, idx) => (
                  <Button
                    key={idx}
                    variant="outline"
                    size="sm"
                    onClick={() => addSuggestedTransform(suggested)}
                    className="gap-1"
                  >
                    <Plus className="h-3 w-3" />
                    {suggested.description}
                  </Button>
                ))}
              </div>
            </div>
          )}

          {/* Active Transforms */}
          {transforms.length > 0 && (
            <div className="space-y-2">
              <p className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
                Active Transforms ({transforms.length})
              </p>
              <AnimatePresence>
                {transforms.map((transform) => (
                  <TransformCard
                    key={transform.id}
                    transform={transform}
                    onRemove={() => removeTransform(transform.id)}
                    onUpdate={(config) => updateTransform(transform.id, config)}
                  />
                ))}
              </AnimatePresence>
            </div>
          )}

          {/* Empty State */}
          {transforms.length === 0 && !sensitivityAnalysis && (
            <div className="text-center py-8 text-zinc-500">
              <Settings className="h-12 w-12 mx-auto mb-3 text-zinc-300" />
              <p>No transformations configured</p>
              <p className="text-xs mt-1">
                Add transformations to mask, filter, or modify data before syncing
              </p>
            </div>
          )}

          {/* Sync Mode Selector */}
          <div className="pt-4 border-t border-zinc-200 dark:border-zinc-800">
            <SyncModeSelector
              value={syncMode}
              onChange={setSyncMode}
            />
          </div>

          {/* Row Filters */}
          {tables.length > 0 && (
            <div className="pt-4 border-t border-zinc-200 dark:border-zinc-800">
              <RowFilterBuilder
                tables={tables}
                filters={rowFilters}
                onChange={setRowFilters}
              />
            </div>
          )}

          {/* Actions */}
          <div className="flex gap-3 pt-4 border-t border-zinc-200 dark:border-zinc-800">
            <Button
              onClick={() => onApply({ transforms, syncMode, rowFilters })}
              className="flex-1 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700"
            >
              <CheckCircle2 className="h-4 w-4 mr-2" />
              {transforms.length > 0 || rowFilters.length > 0
                ? `Apply ${transforms.length + rowFilters.length} Configuration${transforms.length + rowFilters.length > 1 ? "s" : ""}`
                : "Continue Without Transforms"}
            </Button>
            <Button variant="outline" onClick={onSkip}>
              Skip
            </Button>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

