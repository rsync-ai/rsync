"use client"

import { useState } from "react"
import { motion, AnimatePresence } from "framer-motion"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import {
  Database,
  CheckCircle2,
  AlertTriangle,
  ChevronDown,
  ChevronUp,
  Key,
  Layers,
  HardDrive,
  ArrowRight,
  Eye,
} from "lucide-react"

// =============================================================================
// TYPES
// =============================================================================

interface TableInfo {
  name: string
  full_name?: string
  schema?: string
  estimated_rows?: number
  size_bytes?: number
  size_human?: string
  has_primary_key?: boolean
  primary_keys?: string[]
  columns?: Array<{ name: string; type: string }>
}

interface DiscoveredSchema {
  tables: TableInfo[]
  relationships?: Array<{ from: string; to: string; type: string }>
  total_tables?: number
  total_rows?: number
  total_size_human?: string
}

interface DiscoverySummaryCardProps {
  schema: DiscoveredSchema
  cdcReady?: boolean
  onContinue?: () => void
  onViewDetails?: () => void
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

function calculateTotalRows(tables: TableInfo[]): number {
  return tables.reduce((sum, t) => sum + (t.estimated_rows || 0), 0)
}

// =============================================================================
// STAT CARD COMPONENT
// =============================================================================

function StatCard({
  icon: Icon,
  label,
  value,
  subValue,
  color,
}: {
  icon: typeof Database
  label: string
  value: string | number
  subValue?: string
  color: string
}) {
  return (
    <div className="flex items-center gap-3 p-4 rounded-xl bg-zinc-50 dark:bg-zinc-900/50">
      <div className={cn("p-2.5 rounded-lg", color)}>
        <Icon className="h-5 w-5 text-white" />
      </div>
      <div>
        <p className="text-2xl font-bold text-zinc-900 dark:text-white">{value}</p>
        <p className="text-xs text-zinc-500 dark:text-zinc-400">{label}</p>
        {subValue && (
          <p className="text-xs text-zinc-400 dark:text-zinc-500">{subValue}</p>
        )}
      </div>
    </div>
  )
}

// =============================================================================
// MAIN COMPONENT
// =============================================================================

export function DiscoverySummaryCard({
  schema,
  cdcReady = true,
  onContinue,
  onViewDetails,
  className,
}: DiscoverySummaryCardProps) {
  const [showDetails, setShowDetails] = useState(false)

  const tables = schema.tables || []
  const totalTables = schema.total_tables || tables.length
  const totalRows = schema.total_rows || calculateTotalRows(tables)
  const tablesWithPK = tables.filter((t) => t.has_primary_key !== false).length
  const tablesWithoutPK = totalTables - tablesWithPK

  // Categorize tables
  const systemTables = tables.filter((t) =>
    t.name.startsWith("pg_") || t.name.startsWith("_") || t.schema === "information_schema"
  )
  const emptyTables = tables.filter((t) => (t.estimated_rows || 0) === 0)
  const largeTables = tables.filter((t) => (t.estimated_rows || 0) > 100000)

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className={className}
    >
      <Card className="border-zinc-200 dark:border-zinc-800 overflow-hidden">
        {/* Success Header */}
        <div className="h-1.5 bg-gradient-to-r from-emerald-500 to-teal-500" />

        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <CardTitle className="text-lg flex items-center gap-2">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-emerald-100 dark:bg-emerald-900/30">
                <CheckCircle2 className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
              </div>
              Discovery Complete
            </CardTitle>
            {cdcReady ? (
              <Badge className="bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300">
                CDC Ready
              </Badge>
            ) : (
              <Badge className="bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300">
                <AlertTriangle className="h-3 w-3 mr-1" />
                Requires Setup
              </Badge>
            )}
          </div>
        </CardHeader>

        <CardContent className="space-y-4">
          {/* Stats Grid */}
          <div className="grid grid-cols-3 gap-3">
            <StatCard
              icon={Database}
              label="Tables Found"
              value={totalTables}
              color="bg-violet-600"
            />
            <StatCard
              icon={Key}
              label="CDC-Ready"
              value={tablesWithPK}
              subValue={tablesWithoutPK > 0 ? `${tablesWithoutPK} without PK` : undefined}
              color="bg-emerald-600"
            />
            <StatCard
              icon={Layers}
              label="Total Rows"
              value={formatNumber(totalRows)}
              subValue={schema.total_size_human}
              color="bg-blue-600"
            />
          </div>

          {/* Insights */}
          <div className="space-y-2">
            {tablesWithoutPK > 0 && (
              <div className="flex items-start gap-2 p-3 rounded-lg bg-amber-50 dark:bg-amber-900/20">
                <AlertTriangle className="h-4 w-4 text-amber-600 dark:text-amber-400 mt-0.5 shrink-0" />
                <div className="text-sm">
                  <span className="font-medium text-amber-700 dark:text-amber-300">
                    {tablesWithoutPK} tables without primary key
                  </span>
                  <span className="text-amber-600 dark:text-amber-400">
                    {" "}— will use full table sync instead of CDC
                  </span>
                </div>
              </div>
            )}

            {largeTables.length > 0 && (
              <div className="flex items-start gap-2 p-3 rounded-lg bg-blue-50 dark:bg-blue-900/20">
                <HardDrive className="h-4 w-4 text-blue-600 dark:text-blue-400 mt-0.5 shrink-0" />
                <div className="text-sm">
                  <span className="font-medium text-blue-700 dark:text-blue-300">
                    {largeTables.length} large tables (100K+ rows)
                  </span>
                  <span className="text-blue-600 dark:text-blue-400">
                    {" "}— initial sync may take longer
                  </span>
                </div>
              </div>
            )}
          </div>

          {/* Expandable Details */}
          <AnimatePresence>
            {showDetails && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: "auto", opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                className="overflow-hidden"
              >
                <div className="pt-2 border-t border-zinc-200 dark:border-zinc-800 space-y-3">
                  <p className="text-sm font-medium text-zinc-700 dark:text-zinc-300">
                    Table Categories
                  </p>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="flex justify-between p-2 rounded bg-zinc-50 dark:bg-zinc-900/50">
                      <span className="text-zinc-500">With Primary Key</span>
                      <span className="font-medium">{tablesWithPK}</span>
                    </div>
                    <div className="flex justify-between p-2 rounded bg-zinc-50 dark:bg-zinc-900/50">
                      <span className="text-zinc-500">Without Primary Key</span>
                      <span className="font-medium">{tablesWithoutPK}</span>
                    </div>
                    <div className="flex justify-between p-2 rounded bg-zinc-50 dark:bg-zinc-900/50">
                      <span className="text-zinc-500">Empty Tables</span>
                      <span className="font-medium">{emptyTables.length}</span>
                    </div>
                    <div className="flex justify-between p-2 rounded bg-zinc-50 dark:bg-zinc-900/50">
                      <span className="text-zinc-500">Large Tables (100K+)</span>
                      <span className="font-medium">{largeTables.length}</span>
                    </div>
                  </div>

                  {/* Sample Tables */}
                  <div className="space-y-1">
                    <p className="text-xs text-zinc-500">Sample Tables:</p>
                    <div className="flex flex-wrap gap-1">
                      {tables.slice(0, 8).map((t) => (
                        <Badge key={t.name} variant="secondary" className="text-xs font-mono">
                          {t.name}
                        </Badge>
                      ))}
                      {tables.length > 8 && (
                        <Badge variant="outline" className="text-xs">
                          +{tables.length - 8} more
                        </Badge>
                      )}
                    </div>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>

          {/* Actions */}
          <div className="flex items-center gap-3 pt-2">
            <Button
              onClick={onContinue}
              className="flex-1 bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700"
            >
              Continue to Recommendations
              <ArrowRight className="h-4 w-4 ml-2" />
            </Button>

            <Button
              variant="outline"
              onClick={() => setShowDetails(!showDetails)}
              className="gap-1"
            >
              {showDetails ? (
                <>
                  <ChevronUp className="h-4 w-4" />
                  Less
                </>
              ) : (
                <>
                  <Eye className="h-4 w-4" />
                  Details
                </>
              )}
            </Button>
          </div>
        </CardContent>
      </Card>
    </motion.div>
  )
}

