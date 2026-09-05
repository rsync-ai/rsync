'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { 
  Search, ArrowRight, Database, Cloud, 
  Play, Clock, Plus, Settings, Sparkles, Zap,
  ChevronRight
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { API_ENDPOINTS } from '@/lib/config/api';
import { authFetch } from "@/lib/api/auth-fetch"
import { captureWorkspace, onActiveWorkspaceChange } from '@/lib/workspace/active-workspace';
import { ConnectionLogo } from '@/components/connectors/ConnectionLogo';

function normalizeConnectorForLogo(type: string): string {
  const normalized = String(type || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/_/g, '-');

  // Semantic aliases so logo endpoints resolve reliably
  if (normalized === 's3' || normalized === 'amazon-s3') return 'aws-s3';
  if (normalized === 'postgres') return 'postgresql';

  return normalized || 'unknown';
}

// Quick pipeline templates
const QUICK_PIPELINES = [
  { source: 'mysql', destination: 's3', label: 'MySQL → S3', description: 'Database backup to cloud storage' },
  { source: 'postgresql', destination: 'snowflake', label: 'Postgres → Snowflake', description: 'Analytics warehouse sync' },
  { source: 'mongodb', destination: 'bigquery', label: 'MongoDB → BigQuery', description: 'NoSQL to analytics' },
  { source: 'mysql', destination: 'postgresql', label: 'MySQL → Postgres', description: 'Database migration' },
  { source: 's3', destination: 'snowflake', label: 'S3 → Snowflake', description: 'Data lake to warehouse' },
];

interface Connection {
  id: string;
  name: string;
  connector_type: string;
  type: 'source' | 'destination';
  status: string;
  // OAuth token expiry surfaced by api-gateway (BUG-10).
  is_expired?: boolean;
}

interface RecentPipeline {
  id: string;
  name: string;
  source_connector?: string;
  destination_connector?: string;
  updated_at: string;
  status: string;
}

interface AgenticPipelineHomeProps {
  onSubmit: (intent: string) => void;
  onRunPipeline?: (pipelineId: string) => void;
}

export function AgenticPipelineHome({
  onSubmit,
  onRunPipeline,
}: AgenticPipelineHomeProps) {
  const [intent, setIntent] = useState('');
  const [connections, setConnections] = useState<Connection[]>([]);
  const [recentPipelines, setRecentPipelines] = useState<RecentPipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMorePipelines, setLoadingMorePipelines] = useState(false);
  const [pipelinesTotal, setPipelinesTotal] = useState<number | null>(null);
  const [isInputFocused, setIsInputFocused] = useState(false);
  const [visiblePipelinesCount, setVisiblePipelinesCount] = useState(10);

  const PIPELINES_PAGE_SIZE = 10;

  // Load on mount, and reload on every active-workspace change. Both lists are
  // workspace-scoped, so a switch invalidates them: without this the hero kept
  // rendering the PREVIOUS workspace's connections and recent pipelines — each
  // with a live Run button — under the new workspace's name in the header.
  // Same-tab and cross-tab, matching every other list surface in the app.
  useEffect(() => {
    void fetchData();
    return onActiveWorkspaceChange(() => {
      // Clear first: the refetch is async, and showing the old tenant's rows
      // until it lands is the bug, not a loading state.
      setConnections([]);
      setRecentPipelines([]);
      setPipelinesTotal(null);
      setVisiblePipelinesCount(PIPELINES_PAGE_SIZE);
      void fetchData();
    });
  }, []);

  const fetchData = async () => {
    // Drop both responses if a switch overtook the request, or the previous
    // workspace's rows land under the new one.
    const isStale = captureWorkspace();
    setLoading(true);
    try {
      // Fetch connections
      const connRes = await authFetch(API_ENDPOINTS.CONNECTIONS.LIST, { cache: "no-store" });
      if (isStale()) return;
      if (connRes.ok) {
        const connData = await connRes.json();
        if (isStale()) return;
        setConnections(Array.isArray(connData) ? connData : connData.connections || []);
      }

      // Fetch recent pipelines
      const pipeRes = await authFetch(`${API_ENDPOINTS.PIPELINES.LIST}?limit=${PIPELINES_PAGE_SIZE}&offset=0`, {
        cache: "no-store",
      });
      if (isStale()) return;
      if (pipeRes.ok) {
        const pipeData = await pipeRes.json();
        if (isStale()) return;
        const list = Array.isArray(pipeData) ? pipeData : pipeData.pipelines || [];
        setRecentPipelines(list);
        setPipelinesTotal(typeof pipeData?.total === 'number' ? pipeData.total : null);
        setVisiblePipelinesCount(PIPELINES_PAGE_SIZE);
      }
    } catch (error) {
      console.error('Failed to fetch data:', error);
    } finally {
      if (!isStale()) setLoading(false);
    }
  };

  const visiblePipelines = recentPipelines.slice(0, visiblePipelinesCount);

  const hasMorePipelines = pipelinesTotal !== null
    ? visiblePipelinesCount < pipelinesTotal
    : visiblePipelinesCount < recentPipelines.length; // best-effort fallback

  const loadMorePipelines = async () => {
    if (loadingMorePipelines) return;

    // If we already have more items in memory (e.g. backend returned 50), just reveal them.
    if (visiblePipelinesCount < recentPipelines.length) {
      setVisiblePipelinesCount((v) => Math.min(recentPipelines.length, v + PIPELINES_PAGE_SIZE));
      return;
    }

    // This page APPENDS, so a switch landing mid-flight would splice the new
    // workspace's page 2 onto the old workspace's page 1.
    const isStale = captureWorkspace();
    setLoadingMorePipelines(true);
    try {
      const offset = recentPipelines.length;
      const pipeRes = await authFetch(
        `${API_ENDPOINTS.PIPELINES.LIST}?limit=${PIPELINES_PAGE_SIZE}&offset=${offset}`,
        { cache: "no-store" }
      );
      if (!pipeRes.ok || isStale()) return;
      const pipeData = await pipeRes.json();
      if (isStale()) return;
      const next = Array.isArray(pipeData) ? pipeData : pipeData.pipelines || [];
      setRecentPipelines((prev) => {
        // Dedupe by id (defensive against servers that ignore offset)
        const seen = new Set(prev.map((p) => p.id));
        const merged = [...prev];
        for (const p of next) {
          if (p?.id && !seen.has(p.id)) {
            merged.push(p);
            seen.add(p.id);
          }
        }
        return merged;
      });
      if (typeof pipeData?.total === 'number') setPipelinesTotal(pipeData.total);
      setVisiblePipelinesCount((v) => v + PIPELINES_PAGE_SIZE);
    } catch (error) {
      console.error('Failed to load more pipelines:', error);
    } finally {
      setLoadingMorePipelines(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (intent.trim()) {
      onSubmit(intent.trim());
    }
  };

  const handleQuickPipeline = (source: string, dest: string) => {
    const intentText = `sync ${source} to ${dest}`;
    onSubmit(intentText);
  };

  const renderConnectorLogo = (type: string, size: 'sm' | 'md' = 'sm') => {
    const id = normalizeConnectorForLogo(type);
    return <ConnectionLogo connectorType={id} size={size} />;
  };

  const sourceConnections = connections.filter(c => c.type === 'source');
  const destConnections = connections.filter(c => c.type === 'destination');

  // Only suggest quick pipelines whose source AND destination connector types the
  // user actually has connected. Normalize both sides so aliases (s3/amazon-s3,
  // postgres/postgresql) match. Fall back to the full list when the user has too
  // few connectors to match anything, so the section is never blank.
  const availableConnectorTypes = new Set(
    connections.map(c => normalizeConnectorForLogo(c.connector_type))
  );
  const filteredQuickPipelines = QUICK_PIPELINES.filter(
    p =>
      availableConnectorTypes.has(normalizeConnectorForLogo(p.source)) &&
      availableConnectorTypes.has(normalizeConnectorForLogo(p.destination))
  );
  const displayedQuickPipelines =
    filteredQuickPipelines.length > 0 ? filteredQuickPipelines : QUICK_PIPELINES;

  return (
    <div className="flex flex-col items-center justify-center min-h-[80vh] px-4 py-8">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="text-center mb-8"
      >
        <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-violet-500 to-purple-600 text-white mb-4 shadow-lg">
          <Sparkles className="w-8 h-8" />
        </div>
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          Agentic Data Pipeline
        </h1>
        <p className="text-gray-500 dark:text-gray-400 max-w-md">
          Describe your data pipeline in plain English, or choose a quick action below
        </p>
      </motion.div>

      {/* Main Input */}
      <motion.form
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        onSubmit={handleSubmit}
        className="w-full max-w-2xl mb-8"
      >
        <div className={cn(
          "relative flex items-center rounded-2xl border-2 bg-white dark:bg-gray-900 shadow-sm transition-all",
          isInputFocused 
            ? "border-violet-500 shadow-lg shadow-violet-100 dark:shadow-violet-900/20" 
            : "border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600"
        )}>
          <Search className="absolute left-4 w-5 h-5 text-gray-400" />
          <Input
            type="text"
            value={intent}
            onChange={(e) => setIntent(e.target.value)}
            onFocus={() => setIsInputFocused(true)}
            onBlur={() => setIsInputFocused(false)}
            placeholder='Try "sync mysql to s3" or "backup postgres daily"'
            className="pl-12 pr-24 py-6 text-lg border-0 focus:ring-0 rounded-2xl bg-transparent"
          />
          <Button
            type="submit"
            disabled={!intent.trim()}
            className={cn(
              "absolute right-2 rounded-xl px-4 py-2 transition-all",
              intent.trim()
                ? "bg-violet-600 hover:bg-violet-700 text-white"
                : "bg-gray-100 dark:bg-gray-800 text-gray-400"
            )}
          >
            <ArrowRight className="w-5 h-5" />
          </Button>
        </div>
      </motion.form>

      {/* Quick Pipelines */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="w-full max-w-3xl mb-8"
      >
        <div className="flex items-center gap-2 mb-4">
          <Zap className="w-4 h-4 text-amber-500" />
          <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Quick Pipelines</h2>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          {displayedQuickPipelines.map((pipeline, index) => (
            <motion.button
              key={index}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={() => handleQuickPipeline(pipeline.source, pipeline.destination)}
              className="flex items-center gap-3 p-4 bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 hover:border-violet-300 dark:hover:border-violet-600 hover:shadow-md transition-all text-left group"
            >
              <div className="flex items-center gap-1">
                {renderConnectorLogo(pipeline.source, 'sm')}
                <ArrowRight className="w-3 h-3 text-gray-400 group-hover:text-violet-500" />
                {renderConnectorLogo(pipeline.destination, 'sm')}
              </div>
              <div className="flex-1 min-w-0">
                <p className="font-medium text-gray-900 dark:text-white text-sm truncate">
                  {pipeline.label}
                </p>
                <p className="text-xs text-gray-500 dark:text-gray-400 truncate">
                  {pipeline.description}
                </p>
              </div>
            </motion.button>
          ))}
          
          {/* Custom Pipeline */}
          <motion.button
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
            onClick={() => {
              const input = document.querySelector('input[type="text"]') as HTMLInputElement;
              input?.focus();
            }}
            className="flex items-center gap-3 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-xl border-2 border-dashed border-gray-200 dark:border-gray-700 hover:border-violet-300 dark:hover:border-violet-600 hover:bg-violet-50 dark:hover:bg-violet-900/20 transition-all text-left"
          >
            <div className="w-10 h-10 rounded-lg bg-gray-200 dark:bg-gray-700 flex items-center justify-center">
              <Plus className="w-5 h-5 text-gray-500 dark:text-gray-400" />
            </div>
            <div>
              <p className="font-medium text-gray-700 dark:text-gray-300 text-sm">Custom Pipeline</p>
              <p className="text-xs text-gray-500 dark:text-gray-400">Describe any pipeline</p>
            </div>
          </motion.button>
        </div>
      </motion.div>

      {/* Your Connections */}
      {!loading && connections.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="w-full max-w-3xl mb-8"
        >
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Database className="w-4 h-4 text-blue-500" />
              <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Your Connections</h2>
            </div>
            <Button 
              variant="ghost" 
              size="sm" 
              className="text-xs text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              onClick={() => window.location.href = '/connections'}
            >
              <Settings className="w-3 h-3 mr-1" />
              Manage
            </Button>
          </div>
          
          <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
            <div className="grid grid-cols-2 gap-4">
              {/* Sources */}
              <div>
                <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">Sources</p>
                <div className="flex flex-wrap gap-2">
                  {sourceConnections.length > 0 ? (
                    sourceConnections.slice(0, 3).map((conn) => (
                      <span
                        key={conn.id}
                        className={cn(
                          "inline-flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-medium",
                          conn.is_expired
                            ? "bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400"
                            : conn.status === 'active'
                            ? "bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-400"
                            : "bg-gray-50 dark:bg-gray-700 text-gray-600 dark:text-gray-300"
                        )}
                      >
                        {renderConnectorLogo(conn.connector_type, 'sm')}
                        {conn.name}
                        {conn.is_expired
                          ? <span className="w-1.5 h-1.5 rounded-full bg-red-500" title="Token expired" />
                          : conn.status === 'active' && <span className="w-1.5 h-1.5 rounded-full bg-green-500" />}
                      </span>
                    ))
                  ) : (
                    <span className="text-xs text-gray-400">No sources configured</span>
                  )}
                  {sourceConnections.length > 3 && (
                    <span className="text-xs text-gray-500 dark:text-gray-400">+{sourceConnections.length - 3} more</span>
                  )}
                </div>
              </div>
              
              {/* Destinations */}
              <div>
                <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">Destinations</p>
                <div className="flex flex-wrap gap-2">
                  {destConnections.length > 0 ? (
                    destConnections.slice(0, 3).map((conn) => (
                      <span
                        key={conn.id}
                        className={cn(
                          "inline-flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-medium",
                          conn.is_expired
                            ? "bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400"
                            : conn.status === 'active'
                            ? "bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-400"
                            : "bg-gray-50 dark:bg-gray-700 text-gray-600 dark:text-gray-300"
                        )}
                      >
                        {renderConnectorLogo(conn.connector_type, 'sm')}
                        {conn.name}
                        {conn.is_expired
                          ? <span className="w-1.5 h-1.5 rounded-full bg-red-500" title="Token expired" />
                          : conn.status === 'active' && <span className="w-1.5 h-1.5 rounded-full bg-green-500" />}
                      </span>
                    ))
                  ) : (
                    <span className="text-xs text-gray-400">No destinations configured</span>
                  )}
                  {destConnections.length > 3 && (
                    <span className="text-xs text-gray-500 dark:text-gray-400">+{destConnections.length - 3} more</span>
                  )}
                </div>
              </div>
            </div>
          </div>
        </motion.div>
      )}

      {/* Recent Pipelines */}
      {!loading && recentPipelines.length > 0 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="w-full max-w-3xl"
        >
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Clock className="w-4 h-4 text-gray-500 dark:text-gray-400" />
              <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Recent Pipelines</h2>
            </div>
            <Button 
              variant="ghost" 
              size="sm" 
              className="text-xs text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200"
              onClick={() => window.location.href = '/pipelines'}
            >
              View All
              <ChevronRight className="w-3 h-3 ml-1" />
            </Button>
          </div>
          
          <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 divide-y divide-gray-100 dark:divide-gray-700">
            {visiblePipelines.map((pipeline) => (
              <div
                key={pipeline.id}
                className="flex items-center justify-between p-4 hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors"
              >
                <div className="flex items-center gap-3">
                  <div className="flex items-center gap-1">
                    {renderConnectorLogo(pipeline.source_connector || 'unknown', 'sm')}
                    <ArrowRight className="w-3 h-3 text-gray-400" />
                    {renderConnectorLogo(pipeline.destination_connector || 'unknown', 'sm')}
                  </div>
                  <div>
                    <p className="font-medium text-gray-900 dark:text-white text-sm">{pipeline.name || `Pipeline ${pipeline.id.slice(0, 8)}`}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      Last run: {formatRelativeTime(pipeline.updated_at)}
                    </p>
                  </div>
                </div>
                {onRunPipeline && (
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => onRunPipeline(pipeline.id)}
                    className="text-violet-600 hover:text-violet-700 hover:bg-violet-50 dark:text-violet-400 dark:hover:text-violet-300 dark:hover:bg-violet-900/20"
                  >
                    <Play className="w-4 h-4 mr-1" />
                    Run
                  </Button>
                )}
              </div>
            ))}
          </div>

          {hasMorePipelines && (
            <div className="mt-3 flex justify-center">
              <Button
                variant="outline"
                size="sm"
                onClick={loadMorePipelines}
                disabled={loadingMorePipelines}
                className="min-w-[140px]"
              >
                {loadingMorePipelines ? "Loading…" : "Load more"}
              </Button>
            </div>
          )}
        </motion.div>
      )}

      {/* Empty State - No Connections */}
      {!loading && connections.length === 0 && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="w-full max-w-md text-center p-8 bg-gray-50 dark:bg-gray-800/50 rounded-2xl border-2 border-dashed border-gray-200 dark:border-gray-700"
        >
          <Database className="w-12 h-12 text-gray-300 dark:text-gray-600 mx-auto mb-4" />
          <h3 className="font-semibold text-gray-900 dark:text-white mb-2">No connections yet</h3>
          <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
            Set up your first source and destination to start building pipelines
          </p>
          <Button
            onClick={() => window.location.href = '/connections/new'}
            className="bg-violet-600 hover:bg-violet-700"
          >
            <Plus className="w-4 h-4 mr-2" />
            Create Connection
          </Button>
        </motion.div>
      )}
    </div>
  );
}

// Helper function
function formatRelativeTime(dateString: string): string {
  if (!dateString) return 'Unknown';
  
  try {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
  } catch {
    return 'Unknown';
  }
}

