'use client';

import { ArrowRight, Plus, Zap } from 'lucide-react';
import { ConnectionLogo } from '@/components/connectors/ConnectionLogo';
import { cn } from '@/lib/utils';
import { normalizeConnectorForLogo, type QuickPipeline } from './suggestions';

interface QuickPipelinesProps {
  pipelines: QuickPipeline[];
  onPick: (source: string, destination: string) => void;
  onCustom: () => void;
}

const cardBase =
  'flex items-center gap-3 p-4 rounded-xl text-left transition-all hover:-translate-y-0.5 hover:shadow-md focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-violet-500';

export function QuickPipelines({ pipelines, onPick, onCustom }: QuickPipelinesProps) {
  const allExamples = pipelines.length > 0 && pipelines.every((p) => p.needsConnection);

  return (
    <section aria-labelledby="quick-pipelines-heading" className="w-full">
      <div className="flex items-center gap-2 mb-3">
        <Zap className="w-4 h-4 text-amber-500" aria-hidden />
        <h2 id="quick-pipelines-heading" className="text-sm font-semibold text-gray-700 dark:text-gray-300">
          {allExamples ? 'Example Pipelines' : 'Quick Pipelines'}
        </h2>
        {allExamples && (
          <span className="text-xs text-gray-400 dark:text-gray-500">— add a connection to run these</span>
        )}
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {pipelines.map((pipeline) => (
          <button
            key={`${pipeline.source}>${pipeline.destination}`}
            type="button"
            onClick={() => onPick(pipeline.source, pipeline.destination)}
            className={cn(
              cardBase,
              'group bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 hover:border-violet-300 dark:hover:border-violet-600',
              pipeline.needsConnection && 'opacity-75 hover:opacity-100'
            )}
          >
            <div className="flex items-center gap-1 shrink-0">
              <ConnectionLogo connectorType={normalizeConnectorForLogo(pipeline.source)} size="sm" />
              <ArrowRight className="w-3 h-3 text-gray-400 group-hover:text-violet-500" aria-hidden />
              <ConnectionLogo connectorType={normalizeConnectorForLogo(pipeline.destination)} size="sm" />
            </div>
            <div className="flex-1 min-w-0">
              <p className="font-medium text-gray-900 dark:text-white text-sm truncate">{pipeline.label}</p>
              <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
                {pipeline.needsConnection ? 'Needs connection' : pipeline.description}
              </p>
            </div>
          </button>
        ))}

        <button
          type="button"
          onClick={onCustom}
          className={cn(
            cardBase,
            'bg-gray-50 dark:bg-gray-800/50 border-2 border-dashed border-gray-200 dark:border-gray-700 hover:border-violet-300 dark:hover:border-violet-600 hover:bg-violet-50 dark:hover:bg-violet-900/20'
          )}
        >
          <div className="w-10 h-10 rounded-lg bg-gray-200 dark:bg-gray-700 flex items-center justify-center shrink-0">
            <Plus className="w-5 h-5 text-gray-500 dark:text-gray-400" aria-hidden />
          </div>
          <div>
            <p className="font-medium text-gray-700 dark:text-gray-300 text-sm">Custom Pipeline</p>
            <p className="text-xs text-gray-500 dark:text-gray-400">Describe any pipeline</p>
          </div>
        </button>
      </div>
    </section>
  );
}
