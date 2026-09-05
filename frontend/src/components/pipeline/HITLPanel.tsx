/**
 * HITL Panel Component
 * 
 * BIG, OBVIOUS panel that appears when human input is required
 * Makes it crystal clear: "The pipeline is STOPPED. Action required."
 */

import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { AlertTriangle, Clock, Info } from 'lucide-react'
import type { HITLState } from '@/lib/pipeline/stageDefinitions'
import { cn } from '@/lib/utils'

export interface HITLPanelProps {
  hitl: HITLState
  children: React.ReactNode // Action form/buttons go here
  className?: string
}

const riskLevels = {
  low: {
    icon: <Info className="h-5 w-5" />,
    color: 'border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-950/20',
    badgeColor: 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300',
    textColor: 'text-blue-900 dark:text-blue-100',
  },
  medium: {
    icon: <AlertTriangle className="h-5 w-5" />,
    color: 'border-orange-200 bg-orange-50 dark:border-orange-800 dark:bg-orange-950/20',
    badgeColor: 'bg-orange-100 text-orange-700 dark:bg-orange-900 dark:text-orange-300',
    textColor: 'text-orange-900 dark:text-orange-100',
  },
  high: {
    icon: <AlertTriangle className="h-5 w-5" />,
    color: 'border-red-200 bg-red-50 dark:border-red-800 dark:bg-red-950/20',
    badgeColor: 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300',
    textColor: 'text-red-900 dark:text-red-100',
  },
}

function inferRiskLevel(blockingType: string): 'low' | 'medium' | 'high' {
  const type = blockingType.toLowerCase()
  
  if (type.includes('connector_generation') || type.includes('policy')) {
    return 'medium'
  }
  
  if (type.includes('approval') || type.includes('confirm')) {
    return 'high'
  }
  
  return 'low' // connection_config, etc.
}

function formatEstimatedTime(seconds?: number): string {
  if (!seconds) return 'a few moments'
  if (seconds < 60) return `~${seconds} seconds`
  if (seconds < 300) return `~${Math.ceil(seconds / 60)} minutes`
  return 'several minutes'
}

export function HITLPanel({ hitl, children, className }: HITLPanelProps) {
  const riskLevel = inferRiskLevel(hitl.blockingReason.type)
  const config = riskLevels[riskLevel]

  return (
    <Card 
      className={cn(
        "border-2",
        config.color,
        className
      )}
    >
      {/* Header: Grab attention */}
      <div className="p-4 border-b border-current/10">
        <div className="flex items-start gap-3">
          <div className={config.textColor}>
            {config.icon}
          </div>
          
          <div className="flex-1 space-y-2">
            <div className="flex items-center gap-2 flex-wrap">
              <h3 className={cn("text-lg font-bold", config.textColor)}>
                ⏸️ Pipeline Paused - Action Required
              </h3>
              <Badge className={config.badgeColor}>
                Waiting for Input
              </Badge>
            </div>

            <p className={cn("text-sm", config.textColor)}>
              {hitl.blockingReason.description}
            </p>

            {hitl.blockingReason.estimatedSeconds && (
              <div className={cn("flex items-center gap-2 text-xs", config.textColor)}>
                <Clock className="h-3 w-3" />
                <span>
                  This should take {formatEstimatedTime(hitl.blockingReason.estimatedSeconds)}
                </span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Body: Action form */}
      <div className="p-4 space-y-4">
        {children}
        
        {/* Footer: What happens next */}
        <div className="text-xs text-zinc-600 dark:text-zinc-400 p-3 bg-zinc-50 dark:bg-zinc-900/50 rounded border border-zinc-200 dark:border-zinc-800">
          <span className="font-medium">What happens next:</span>
          <br />
          Once you submit, the pipeline will resume automatically from where it left off.
        </div>
      </div>
    </Card>
  )
}
