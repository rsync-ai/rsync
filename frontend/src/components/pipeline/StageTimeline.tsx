/**
 * Stage Timeline Component
 * 
 * Vertical timeline showing all pipeline stages
 * Primary mental model for users to understand execution flow
 */

import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { 
  CheckCircle2, 
  XCircle, 
  Loader2, 
  Circle,
  Pause,
  RefreshCw,
  Square,
} from 'lucide-react'
import type { StageExecution, StageStatus } from '@/lib/pipeline/stageDefinitions'
import { cn } from '@/lib/utils'

export interface StageTimelineProps {
  stages: StageExecution[]
  currentStageKey?: string
  className?: string
  title?: string
}

const statusIcons: Record<StageStatus, React.ReactNode> = {
  'pending': <Circle className="h-4 w-4 text-zinc-300" />,
  'running': <Loader2 className="h-4 w-4 animate-spin text-blue-600" />,
  'waiting': <Pause className="h-4 w-4 text-orange-600" />,
  'retrying': <RefreshCw className="h-4 w-4 animate-spin text-yellow-600" />,
  'completed': <CheckCircle2 className="h-4 w-4 text-green-600" />,
  'failed': <XCircle className="h-4 w-4 text-red-600" />,
  'cancelled': <Square className="h-4 w-4 text-zinc-500" />,
}

const statusColors: Record<StageStatus, string> = {
  'pending': 'bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400',
  'running': 'bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-950 dark:text-blue-300',
  'waiting': 'bg-orange-50 text-orange-700 border-orange-200 dark:bg-orange-950 dark:text-orange-300',
  'retrying': 'bg-yellow-50 text-yellow-700 border-yellow-200 dark:bg-yellow-950 dark:text-yellow-300',
  'completed': 'bg-green-50 text-green-700 dark:bg-green-950 dark:text-green-300',
  'failed': 'bg-red-50 text-red-700 dark:bg-red-950 dark:text-red-300',
  'cancelled': 'bg-zinc-50 text-zinc-700 border-zinc-200 dark:bg-zinc-900/40 dark:text-zinc-300',
}

function formatTimestamp(timestamp?: number): string {
  if (!timestamp) return ''
  const date = new Date(timestamp)
  // Avoid locale-dependent formatting during SSR/hydration.
  // Use a deterministic UTC time string (HH:MM:SS).
  return date.toISOString().slice(11, 19)
}

function formatDuration(start?: number, end?: number): string {
  if (!start) return ''
  const endTime = end || Date.now()
  const seconds = Math.floor((endTime - start) / 1000)
  if (seconds < 60) return `${seconds}s`
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`
}

export function StageTimeline({ stages, currentStageKey, className, title }: StageTimelineProps) {
  if (stages.length === 0) {
    return null
  }

  return (
    <Card className={cn("p-4 border-zinc-200 dark:border-zinc-800", className)}>
      <div className="space-y-1">
        <h3 className="text-sm font-semibold text-zinc-900 dark:text-white mb-3">
          {title || "Pipeline Stages"}
        </h3>
        
        <div className="space-y-0">
          {stages.map((stage, index) => {
            const isActive = stage.stage === currentStageKey
            const isLast = index === stages.length - 1
            
            return (
              <div key={stage.stage} className="relative">
                {/* Connector line to next stage */}
                {!isLast && (
                  <div 
                    className={cn(
                      "absolute left-[15px] top-[32px] w-0.5 h-[calc(100%-16px)]",
                      stage.status === 'completed' 
                        ? "bg-green-300 dark:bg-green-700"
                        : "bg-zinc-200 dark:bg-zinc-700"
                    )}
                  />
                )}
                
                {/* Stage Row */}
                <div 
                  className={cn(
                    "relative flex items-start gap-3 p-2 rounded-lg transition-all",
                    isActive && "ring-2 ring-blue-200 dark:ring-blue-800",
                    statusColors[stage.status]
                  )}
                >
                  {/* Icon */}
                  <div className="relative z-10 flex-shrink-0 mt-0.5">
                    {statusIcons[stage.status]}
                  </div>

                  {/* Content */}
                  <div className="flex-1 min-w-0 space-y-1">
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex-1">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-medium">
                            {stage.icon} {stage.label}
                          </span>
                          
                          {/* Retry badge */}
                          {stage.currentAttempt !== undefined && stage.currentAttempt > 1 && (
                            <Badge variant="outline" className="text-xs">
                              {stage.maxAttempts !== undefined
                                ? `Retry ${stage.currentAttempt}/${stage.maxAttempts}`
                                : `Retry ${stage.currentAttempt}`}
                            </Badge>
                          )}
                        </div>
                        
                        <p className="text-xs text-zinc-600 dark:text-zinc-400 mt-0.5">
                          {stage.description}
                        </p>
                      </div>

                      {/* Timestamp and Duration */}
                      <div className="text-xs text-zinc-500 text-right whitespace-nowrap">
                        {stage.startedAt && (
                          <div>{formatTimestamp(stage.startedAt)}</div>
                        )}
                        {stage.startedAt && stage.completedAt && (
                          <div className="font-medium">
                            {formatDuration(stage.startedAt, stage.completedAt)}
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Progress bar for running stage */}
                    {(stage.status === 'running' || stage.status === 'retrying') && stage.progress !== undefined && (
                      <Progress value={stage.progress} className="h-1" />
                    )}

                    {/* Message for waiting stage */}
                    {stage.status === 'waiting' && stage.attempts.length > 0 && (
                      <div className="text-xs text-orange-800 dark:text-orange-200 mt-1 p-2 bg-orange-50 dark:bg-orange-950/40 rounded">
                        {stage.attempts[stage.attempts.length - 1].message || 'Waiting for input'}
                      </div>
                    )}

                    {/* Error message for failed stage */}
                    {stage.status === 'failed' && stage.attempts.length > 0 && (
                      <div className="text-xs text-red-600 dark:text-red-400 mt-1 p-2 bg-red-50 dark:bg-red-950/50 rounded">
                        {stage.attempts[stage.attempts.length - 1].error || 'Stage failed'}
                      </div>
                    )}

                    {/* Message for cancelled stage */}
                    {stage.status === 'cancelled' && stage.attempts.length > 0 && (
                      <div className="text-xs text-zinc-600 dark:text-zinc-300 mt-1 p-2 bg-zinc-100 dark:bg-zinc-900/50 rounded">
                        {stage.attempts[stage.attempts.length - 1].message || 'Stopped'}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </Card>
  )
}
