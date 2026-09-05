"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { cn } from "@/lib/utils"
import { Plus, X, Filter, ChevronDown, ChevronRight } from "lucide-react"
import { motion, AnimatePresence } from "framer-motion"

// =============================================================================
// TYPES
// =============================================================================

export type FilterOperator = 
  | "=" 
  | "!=" 
  | ">" 
  | "<" 
  | ">=" 
  | "<=" 
  | "IN" 
  | "NOT IN" 
  | "LIKE" 
  | "IS NULL" 
  | "IS NOT NULL"

export type FilterConjunction = "AND" | "OR"

export interface RowFilter {
  id: string
  table: string
  column: string
  operator: FilterOperator
  value: string
  conjunction: FilterConjunction
}

interface RowFilterBuilderProps {
  tables: { name: string; columns: string[] }[]
  filters: RowFilter[]
  onChange: (filters: RowFilter[]) => void
  disabled?: boolean
  className?: string
}

// =============================================================================
// CONSTANTS
// =============================================================================

const operators: { value: FilterOperator; label: string; needsValue: boolean }[] = [
  { value: "=", label: "equals", needsValue: true },
  { value: "!=", label: "not equals", needsValue: true },
  { value: ">", label: "greater than", needsValue: true },
  { value: "<", label: "less than", needsValue: true },
  { value: ">=", label: "greater or equal", needsValue: true },
  { value: "<=", label: "less or equal", needsValue: true },
  { value: "IN", label: "in list", needsValue: true },
  { value: "NOT IN", label: "not in list", needsValue: true },
  { value: "LIKE", label: "like pattern", needsValue: true },
  { value: "IS NULL", label: "is null", needsValue: false },
  { value: "IS NOT NULL", label: "is not null", needsValue: false },
]

// =============================================================================
// COMPONENT
// =============================================================================

export function RowFilterBuilder({
  tables,
  filters,
  onChange,
  disabled = false,
  className,
}: RowFilterBuilderProps) {
  const [isExpanded, setIsExpanded] = useState(filters.length > 0)

  const addFilter = () => {
    const firstTable = tables[0]
    const newFilter: RowFilter = {
      id: `filter-${Date.now()}`,
      table: firstTable?.name || "",
      column: firstTable?.columns[0] || "",
      operator: "=",
      value: "",
      conjunction: "AND",
    }
    onChange([...filters, newFilter])
    setIsExpanded(true)
  }

  const updateFilter = (id: string, updates: Partial<RowFilter>) => {
    onChange(
      filters.map((f) => (f.id === id ? { ...f, ...updates } : f))
    )
  }

  const removeFilter = (id: string) => {
    onChange(filters.filter((f) => f.id !== id))
  }

  const getColumnsForTable = (tableName: string): string[] => {
    const table = tables.find((t) => t.name === tableName)
    return table?.columns || []
  }

  const needsValue = (operator: FilterOperator): boolean => {
    return operators.find((o) => o.value === operator)?.needsValue ?? true
  }

  return (
    <div className={cn("space-y-3", className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <button
          type="button"
          onClick={() => setIsExpanded(!isExpanded)}
          className="flex items-center gap-2 text-sm font-medium text-zinc-900 dark:text-white hover:text-violet-600 transition-colors"
          disabled={disabled}
        >
          {isExpanded ? (
            <ChevronDown className="h-4 w-4" />
          ) : (
            <ChevronRight className="h-4 w-4" />
          )}
          <Filter className="h-4 w-4" />
          Row Filters
          {filters.length > 0 && (
            <Badge variant="secondary" className="ml-2">
              {filters.length}
            </Badge>
          )}
        </button>
        
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={addFilter}
          disabled={disabled || tables.length === 0}
          className="gap-1"
        >
          <Plus className="h-3.5 w-3.5" />
          Add Filter
        </Button>
      </div>

      {/* Filter list */}
      <AnimatePresence>
        {isExpanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            className="space-y-2 overflow-hidden"
          >
            {filters.length === 0 ? (
              <p className="text-sm text-zinc-500 dark:text-zinc-400 italic py-2">
                No filters configured. All rows will be synced.
              </p>
            ) : (
              filters.map((filter, index) => (
                <motion.div
                  key={filter.id}
                  initial={{ opacity: 0, y: -10 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -10 }}
                  className="flex flex-wrap items-center gap-2 p-3 rounded-lg border border-zinc-200 bg-zinc-50 dark:border-zinc-800 dark:bg-zinc-900"
                >
                  {/* Conjunction (for 2nd+ filters) */}
                  {index > 0 && (
                    <Select
                      value={filter.conjunction}
                      onValueChange={(v) =>
                        updateFilter(filter.id, { conjunction: v as FilterConjunction })
                      }
                      disabled={disabled}
                    >
                      <SelectTrigger className="w-20 h-8 text-xs">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="AND">AND</SelectItem>
                        <SelectItem value="OR">OR</SelectItem>
                      </SelectContent>
                    </Select>
                  )}
                  
                  {/* Table selector */}
                  <Select
                    value={filter.table}
                    onValueChange={(v) =>
                      updateFilter(filter.id, {
                        table: v,
                        column: getColumnsForTable(v)[0] || "",
                      })
                    }
                    disabled={disabled}
                  >
                    <SelectTrigger className="w-32 h-8 text-xs">
                      <SelectValue placeholder="Table" />
                    </SelectTrigger>
                    <SelectContent>
                      {tables.map((table) => (
                        <SelectItem key={table.name} value={table.name}>
                          {table.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>

                  {/* Column selector */}
                  <Select
                    value={filter.column}
                    onValueChange={(v) => updateFilter(filter.id, { column: v })}
                    disabled={disabled}
                  >
                    <SelectTrigger className="w-32 h-8 text-xs">
                      <SelectValue placeholder="Column" />
                    </SelectTrigger>
                    <SelectContent>
                      {getColumnsForTable(filter.table).map((col) => (
                        <SelectItem key={col} value={col}>
                          {col}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>

                  {/* Operator selector */}
                  <Select
                    value={filter.operator}
                    onValueChange={(v) =>
                      updateFilter(filter.id, { operator: v as FilterOperator })
                    }
                    disabled={disabled}
                  >
                    <SelectTrigger className="w-32 h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {operators.map((op) => (
                        <SelectItem key={op.value} value={op.value}>
                          {op.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>

                  {/* Value input */}
                  {needsValue(filter.operator) && (
                    <Input
                      type="text"
                      value={filter.value}
                      onChange={(e) =>
                        updateFilter(filter.id, { value: e.target.value })
                      }
                      placeholder={
                        filter.operator === "IN" || filter.operator === "NOT IN"
                          ? "a, b, c"
                          : filter.operator === "LIKE"
                          ? "%pattern%"
                          : "value"
                      }
                      disabled={disabled}
                      className="w-32 h-8 text-xs"
                    />
                  )}

                  {/* Remove button */}
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => removeFilter(filter.id)}
                    disabled={disabled}
                    className="h-8 w-8 p-0 text-zinc-400 hover:text-red-500"
                  >
                    <X className="h-4 w-4" />
                  </Button>
                </motion.div>
              ))
            )}

            {/* Preview */}
            {filters.length > 0 && (
              <div className="pt-2 border-t border-zinc-200 dark:border-zinc-800">
                <Label className="text-xs text-zinc-500 dark:text-zinc-400">
                  Filter Preview:
                </Label>
                <code className="block mt-1 text-xs text-violet-600 dark:text-violet-400 font-mono">
                  WHERE{" "}
                  {filters
                    .map((f, i) => {
                      const condition =
                        needsValue(f.operator)
                          ? `${f.table}.${f.column} ${f.operator} '${f.value}'`
                          : `${f.table}.${f.column} ${f.operator}`
                      return i === 0 ? condition : `${f.conjunction} ${condition}`
                    })
                    .join(" ")}
                </code>
              </div>
            )}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

