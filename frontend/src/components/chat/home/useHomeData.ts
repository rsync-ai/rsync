'use client';

import { useCallback, useEffect, useState } from 'react';
import { API_ENDPOINTS } from '@/lib/config/api';
import { authFetch } from '@/lib/api/auth-fetch';
import { captureWorkspace, onActiveWorkspaceChange } from '@/lib/workspace/active-workspace';
import type { PipelineListItem } from '@/components/pipeline/PipelinesTable';

export interface HomeConnection {
  id: string;
  name: string;
  connector_type: string;
  type: 'source' | 'destination';
  status: string;
  // OAuth token expiry surfaced by api-gateway (BUG-10).
  is_expired?: boolean;
}

// The list endpoint returns the nested `source_connection`/`destination_connection`
// shape; the flat `*_connector` fields are kept only for older gateways.
export type HomePipeline = Partial<PipelineListItem> & {
  id: string;
  name: string;
  updated_at: string;
  source_connector?: string;
  destination_connector?: string;
};

export const PIPELINES_PAGE_SIZE = 10;

export function useHomeData() {
  const [connections, setConnections] = useState<HomeConnection[]>([]);
  const [recentPipelines, setRecentPipelines] = useState<HomePipeline[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMorePipelines, setLoadingMorePipelines] = useState(false);
  const [pipelinesTotal, setPipelinesTotal] = useState<number | null>(null);
  const [visiblePipelinesCount, setVisiblePipelinesCount] = useState(PIPELINES_PAGE_SIZE);
  // Both reads below used to fall through to [] on failure, so an outage
  // rendered as "No connections yet" with a Create button, and the recent
  // pipelines section simply vanished.
  const [loadError, setLoadError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    // Drop both responses if a switch overtook the request, or the previous
    // workspace's rows land under the new one.
    const isStale = captureWorkspace();
    setLoading(true);
    setLoadError(null);
    try {
      const connRes = await authFetch(API_ENDPOINTS.CONNECTIONS.LIST, { cache: 'no-store' });
      if (isStale()) return;
      if (connRes.ok) {
        const connData = await connRes.json();
        if (isStale()) return;
        setConnections(Array.isArray(connData) ? connData : connData.connections || []);
      } else {
        setLoadError(`The server answered ${connRes.status} for your connections`);
      }

      const pipeRes = await authFetch(`${API_ENDPOINTS.PIPELINES.LIST}?limit=${PIPELINES_PAGE_SIZE}&offset=0`, {
        cache: 'no-store',
      });
      if (isStale()) return;
      if (pipeRes.ok) {
        const pipeData = await pipeRes.json();
        if (isStale()) return;
        const list = Array.isArray(pipeData) ? pipeData : pipeData.pipelines || [];
        setRecentPipelines(list);
        setPipelinesTotal(typeof pipeData?.total === 'number' ? pipeData.total : null);
        setVisiblePipelinesCount(PIPELINES_PAGE_SIZE);
      } else {
        setLoadError(`The server answered ${pipeRes.status} for your pipelines`);
      }
    } catch (error) {
      console.error('Failed to fetch data:', error);
      if (!isStale()) setLoadError('Could not reach the server');
    } finally {
      if (!isStale()) setLoading(false);
    }
  }, []);

  // Load on mount, and reload on every active-workspace change. Both lists are
  // workspace-scoped, so a switch invalidates them: without this the hero kept
  // rendering the PREVIOUS workspace's connections and recent pipelines under
  // the new workspace's name in the header. Same-tab and cross-tab.
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
  }, [fetchData]);

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
        { cache: 'no-store' }
      );
      if (!pipeRes.ok || isStale()) return;
      const pipeData = await pipeRes.json();
      if (isStale()) return;
      const next: HomePipeline[] = Array.isArray(pipeData) ? pipeData : pipeData.pipelines || [];
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

  return {
    connections,
    loading,
    loadError,
    fetchData,
    visiblePipelines,
    pipelinesTotal,
    hasMorePipelines,
    loadingMorePipelines,
    loadMorePipelines,
  };
}
