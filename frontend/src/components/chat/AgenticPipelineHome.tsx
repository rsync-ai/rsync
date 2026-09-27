'use client';

import { useRef } from 'react';
import Link from 'next/link';
import { motion } from 'framer-motion';
import { Database, Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { useHomeData } from '@/components/chat/home/useHomeData';
import { buildExamplePrompts, buildQuickPipelines } from '@/components/chat/home/suggestions';
import { HeroPrompt } from '@/components/chat/home/HeroPrompt';
import { QuickPipelines } from '@/components/chat/home/QuickPipelines';
import { ConnectionsPanel } from '@/components/chat/home/ConnectionsPanel';
import { RecentPipelines } from '@/components/chat/home/RecentPipelines';
import { HomeSkeleton } from '@/components/chat/home/HomeSkeleton';

interface AgenticPipelineHomeProps {
  onSubmit: (intent: string) => void;
  // Kept for callers; recent pipelines now open their detail page instead.
  onRunPipeline?: (pipelineId: string) => void;
}

const fadeUp = (delay: number) => ({
  initial: { opacity: 0, y: 16 },
  animate: { opacity: 1, y: 0 },
  transition: { delay },
});

export function AgenticPipelineHome({ onSubmit }: AgenticPipelineHomeProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const {
    connections,
    loading,
    loadError,
    fetchData,
    visiblePipelines,
    pipelinesTotal,
    hasMorePipelines,
    loadingMorePipelines,
    loadMorePipelines,
  } = useHomeData();

  const quickPipelines = buildQuickPipelines(connections);
  const examplePrompts = buildExamplePrompts(quickPipelines);
  const showConnections = !loading && connections.length > 0;
  const showRecent = !loading && visiblePipelines.length > 0;

  return (
    <div className="flex flex-col items-center min-h-[80vh] px-4 py-10 sm:py-14">
      <div className="w-full max-w-5xl space-y-10">
        <motion.div {...fadeUp(0)}>
          <HeroPrompt inputRef={inputRef} examples={examplePrompts} onSubmit={onSubmit} />
        </motion.div>

        <motion.div {...fadeUp(0.1)}>
          <QuickPipelines
            pipelines={quickPipelines}
            onPick={(source, dest) => onSubmit(`sync ${source} to ${dest}`)}
            onCustom={() => inputRef.current?.focus()}
          />
        </motion.div>

        {loading && <HomeSkeleton />}

        {(showConnections || showRecent) && (
          <motion.div
            {...fadeUp(0.2)}
            className={cn(
              'grid grid-cols-1 gap-6 items-start',
              showConnections && showRecent && 'xl:grid-cols-[minmax(0,1fr)_minmax(0,2fr)]'
            )}
          >
            {showConnections && <ConnectionsPanel connections={connections} />}
            {showRecent && (
              <RecentPipelines
                pipelines={visiblePipelines}
                total={pipelinesTotal}
                hasMore={hasMorePipelines}
                loadingMore={loadingMorePipelines}
                onLoadMore={() => void loadMorePipelines()}
              />
            )}
          </motion.div>
        )}

        {/* Read failed: unknown, not empty */}
        {!loading && loadError && (
          <motion.div
            {...fadeUp(0.2)}
            className="w-full max-w-md mx-auto text-center p-8 bg-amber-50 dark:bg-amber-950/30 rounded-2xl border-2 border-dashed border-amber-300 dark:border-amber-900"
          >
            <Database className="w-12 h-12 text-amber-400 mx-auto mb-4" aria-hidden />
            <h3 className="font-semibold text-gray-900 dark:text-white mb-2">Could not load your workspace</h3>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              {loadError}. Your connections and pipelines are unknown right now — this is not an empty workspace.
            </p>
            <Button variant="outline" onClick={() => void fetchData()}>
              Retry
            </Button>
          </motion.div>
        )}

        {/* Empty State - No Connections */}
        {!loading && !loadError && connections.length === 0 && (
          <motion.div
            {...fadeUp(0.2)}
            className="w-full max-w-md mx-auto text-center p-8 bg-gray-50 dark:bg-gray-800/50 rounded-2xl border-2 border-dashed border-gray-200 dark:border-gray-700"
          >
            <Database className="w-12 h-12 text-gray-300 dark:text-gray-600 mx-auto mb-4" aria-hidden />
            <h3 className="font-semibold text-gray-900 dark:text-white mb-2">No connections yet</h3>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Set up your first source and destination to start building pipelines
            </p>
            <Button asChild className="bg-violet-600 hover:bg-violet-700">
              <Link href="/connections/new">
                <Plus className="w-4 h-4 mr-2" aria-hidden />
                Create Connection
              </Link>
            </Button>
          </motion.div>
        )}
      </div>
    </div>
  );
}
