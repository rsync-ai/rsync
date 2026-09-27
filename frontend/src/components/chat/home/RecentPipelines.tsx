'use client';

import Link from 'next/link';
import { ArrowRight, ChevronRight, Clock } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ConnectionLogo } from '@/components/connectors/ConnectionLogo';
import { getPipelineType, getStatusConfig, type PipelineListItem } from '@/components/pipeline/PipelinesTable';
import { cn, formatRelativeTime } from '@/lib/utils';
import { normalizeConnectorForLogo, shortConnectorName } from './suggestions';
import type { HomePipeline } from './useHomeData';

interface RecentPipelinesProps {
  pipelines: HomePipeline[];
  total: number | null;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
}

function endpoints(p: HomePipeline) {
  const src = p.source_connection?.connector_type || p.source_connector || '';
  const dst = p.destination_connection?.connector_type || p.destination_connector || '';
  return { src, dst };
}

function timeLine(p: HomePipeline): string {
  const lastRun = p.last_execution?.started_at;
  if (lastRun) return `Last run ${formatRelativeTime(lastRun)}`;
  return p.updated_at ? `Updated ${formatRelativeTime(p.updated_at)}` : 'Never run';
}

function StatusPill({ pipeline }: { pipeline: HomePipeline }) {
  if (!pipeline.derived_status) return null;
  const cfg = getStatusConfig(pipeline as PipelineListItem);
  const Icon = cfg.icon;
  return (
    <span className={cn('inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-medium', cfg.bgColor, cfg.color)}>
      <Icon className={cn('h-3 w-3', pipeline.derived_status === 'running' && 'animate-spin')} aria-hidden />
      {cfg.label}
    </span>
  );
}

export function RecentPipelines({ pipelines, total, hasMore, loadingMore, onLoadMore }: RecentPipelinesProps) {
  return (
    <section aria-labelledby="recent-pipelines-heading" className="w-full">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Clock className="w-4 h-4 text-gray-500 dark:text-gray-400" aria-hidden />
          <h2 id="recent-pipelines-heading" className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            Recent Pipelines
            {total !== null && <span className="ml-1 font-normal text-gray-400 dark:text-gray-500">({total})</span>}
          </h2>
        </div>
        <Link
          href="/pipelines"
          className="inline-flex items-center rounded-md px-2 py-1 text-xs text-gray-500 hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-gray-200"
        >
          View all
          <ChevronRight className="w-3 h-3 ml-1" aria-hidden />
        </Link>
      </div>

      <ul className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 divide-y divide-gray-100 dark:divide-gray-700 overflow-hidden">
        {pipelines.map((pipeline) => {
          const { src, dst } = endpoints(pipeline);
          const hasEndpoints = Boolean(src && dst);
          return (
            <li key={pipeline.id}>
              <Link
                href={`/pipelines/${pipeline.id}`}
                data-testid="recent-pipeline-row"
                className="group flex items-center gap-3 p-4 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors focus-visible:outline-none focus-visible:bg-violet-50 dark:focus-visible:bg-violet-900/20"
              >
                <div className="flex items-center gap-1 shrink-0">
                  <ConnectionLogo connectorType={normalizeConnectorForLogo(src || 'unknown')} size="sm" />
                  <ArrowRight className="w-3 h-3 text-gray-400" aria-hidden />
                  <ConnectionLogo connectorType={normalizeConnectorForLogo(dst || 'unknown')} size="sm" />
                </div>
                <div className="flex-1 min-w-0">
                  {/* Pill wraps under the name on narrow screens instead of squeezing it */}
                  <div className="flex flex-wrap items-center gap-x-2 gap-y-1 min-w-0">
                    <p className="max-w-full font-medium text-gray-900 dark:text-white text-sm truncate">
                      {pipeline.name || `Pipeline ${pipeline.id.slice(0, 8)}`}
                    </p>
                    <StatusPill pipeline={pipeline} />
                  </div>
                  <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
                    {hasEndpoints && (
                      <>
                        {pipeline.source_connection?.name || shortConnectorName(src)}
                        {' → '}
                        {pipeline.destination_connection?.name || shortConnectorName(dst)}
                        {' · '}
                      </>
                    )}
                    {pipeline.derived_status !== undefined && (
                      <>{getPipelineType(pipeline as PipelineListItem)} · </>
                    )}
                    {timeLine(pipeline)}
                  </p>
                </div>
                <span className="hidden sm:inline-flex items-center text-xs font-medium text-violet-600 dark:text-violet-400 opacity-0 group-hover:opacity-100 group-focus-visible:opacity-100 transition-opacity">
                  Open
                  <ChevronRight className="w-3.5 h-3.5" aria-hidden />
                </span>
              </Link>
            </li>
          );
        })}
      </ul>

      {hasMore && (
        <div className="mt-3 flex justify-center">
          <Button variant="outline" size="sm" onClick={onLoadMore} disabled={loadingMore} className="min-w-[140px]">
            {loadingMore ? 'Loading…' : 'Load more'}
          </Button>
        </div>
      )}
    </section>
  );
}
