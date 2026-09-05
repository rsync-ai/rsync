"use client"

import { BaseEdge, EdgeLabelRenderer, getBezierPath, type EdgeProps } from "@xyflow/react"

export type SchemaField = { name: string; type: string; nullable?: boolean }

export type AnimatedEdgeData = {
  active: boolean
  color: string
  sourceLabel?: string
  targetLabel?: string
  sourceStatus?: string
  sourceDuration?: number | null
  outputSchema?: SchemaField[] | null
}

export function AnimatedEdge(props: EdgeProps) {
  const { sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, markerEnd, id, data } = props
  const ed = (data ?? {}) as AnimatedEdgeData
  const stroke = ed.color ?? "#94a3b8"

  const [path, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  })

  return (
    <>
      {/* Underlay (subtle wider stroke for depth) */}
      <path
        d={path}
        fill="none"
        stroke={stroke}
        strokeOpacity={0.18}
        strokeWidth={6}
      />

      {/* Main edge */}
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        style={{
          stroke,
          strokeWidth: 2,
          strokeDasharray: ed.active ? "6 6" : undefined,
          animation: ed.active ? "dash-flow 0.8s linear infinite" : undefined,
          fill: "none",
        }}
      />

      {/* Particles (only when active) */}
      {ed.active && (
        <>
          <circle r={3} fill={stroke} opacity={0.95}>
            <animateMotion dur="1.6s" repeatCount="indefinite" path={path} />
          </circle>
          <circle r={2.5} fill={stroke} opacity={0.7}>
            <animateMotion dur="1.6s" begin="0.4s" repeatCount="indefinite" path={path} />
          </circle>
          <circle r={2} fill={stroke} opacity={0.5}>
            <animateMotion dur="1.6s" begin="0.8s" repeatCount="indefinite" path={path} />
          </circle>
        </>
      )}

      {/* Edge labels: row count badge + PII shield + hover popover */}
      <EdgeLabelRenderer>
        <div
          className="absolute pointer-events-auto group"
          style={{
            transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
          }}
        >
          <div className="flex items-center gap-1">
            {/* Hover hit target. The badges that used to sit here reported a row
                count and a masked-field count; nothing on the wire carries
                either (see the note atop `dagHelpers.ts`), so every edge always
                took this branch. */}
            <div className="w-12 h-4 -my-1 rounded-full opacity-0 cursor-help" aria-hidden />
          </div>

          {/* Hover popover — shows full flow detail */}
          <div className="absolute top-full left-1/2 -translate-x-1/2 mt-2 z-50 hidden group-hover:block pointer-events-none">
            <div className="rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 shadow-lg p-2.5 min-w-[180px] max-w-[240px]">
              <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">
                Data flow
              </div>
              {ed.sourceLabel && ed.targetLabel && (
                <div className="text-[11px] text-zinc-700 dark:text-zinc-200 font-medium mb-1.5 leading-tight">
                  {ed.sourceLabel} → {ed.targetLabel}
                </div>
              )}
              <div className="space-y-1 text-[10px]">
                {ed.sourceStatus && (
                  <Row label="Source status" value={ed.sourceStatus} />
                )}
                {typeof ed.sourceDuration === "number" && ed.sourceDuration > 0 && (
                  <Row label="Source took" value={formatMs(ed.sourceDuration)} />
                )}
              </div>
              {ed.outputSchema && ed.outputSchema.length > 0 ? (
                <div className="mt-2">
                  <div className="text-[9px] uppercase tracking-wide text-zinc-400 mb-1">
                    Output schema
                  </div>
                  <div className="space-y-0.5 max-h-[120px] overflow-y-auto">
                    {ed.outputSchema.slice(0, 10).map((f) => (
                      <div key={f.name} className="flex items-center justify-between gap-2 text-[9px]">
                        <span className="text-zinc-700 dark:text-zinc-200 font-mono truncate">{f.name}</span>
                        <span className="text-zinc-400 font-mono shrink-0">{f.type}{f.nullable === false ? " !" : ""}</span>
                      </div>
                    ))}
                    {ed.outputSchema.length > 10 && (
                      <div className="text-[9px] text-zinc-400 italic">
                        +{ed.outputSchema.length - 10} more columns
                      </div>
                    )}
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </EdgeLabelRenderer>
    </>
  )
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-zinc-500">{label}</span>
      <span className="text-zinc-800 dark:text-zinc-100 font-medium">{value}</span>
    </div>
  )
}

function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`
}
