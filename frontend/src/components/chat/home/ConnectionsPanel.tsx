'use client';

import Link from 'next/link';
import { Database, Plus, Settings } from 'lucide-react';
import { ConnectionLogo } from '@/components/connectors/ConnectionLogo';
import { cn } from '@/lib/utils';
import { normalizeConnectorForLogo } from './suggestions';
import type { HomeConnection } from './useHomeData';

const MAX_CHIPS = 4;

function connectionState(conn: HomeConnection): { label: string; chip: string; dot: string } {
  if (conn.is_expired) {
    return {
      label: 'Token expired',
      chip: 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400',
      dot: 'bg-red-500',
    };
  }
  if (conn.status === 'active') {
    return {
      label: 'Active',
      chip: 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-400',
      dot: 'bg-green-500',
    };
  }
  return {
    label: conn.status || 'Unknown',
    chip: 'bg-gray-50 dark:bg-gray-700 text-gray-600 dark:text-gray-300',
    dot: 'bg-gray-400',
  };
}

function ConnectionChipList({ title, connections }: { title: string; connections: HomeConnection[] }) {
  return (
    <div>
      <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">
        {title} <span className="text-gray-400 dark:text-gray-500">({connections.length})</span>
      </p>
      <ul className="flex flex-wrap gap-2">
        {connections.length === 0 && (
          <li className="text-xs text-gray-400">No {title.toLowerCase()} configured</li>
        )}
        {connections.slice(0, MAX_CHIPS).map((conn) => {
          const state = connectionState(conn);
          return (
            <li
              key={conn.id}
              title={`${conn.name} — ${state.label}`}
              className={cn('inline-flex max-w-full items-center gap-1.5 px-2 py-1 rounded-lg text-xs font-medium', state.chip)}
            >
              <ConnectionLogo connectorType={normalizeConnectorForLogo(conn.connector_type)} size="sm" />
              <span className="truncate">{conn.name}</span>
              <span className={cn('w-1.5 h-1.5 rounded-full shrink-0', state.dot)} aria-hidden />
              <span className="sr-only">{state.label}</span>
            </li>
          );
        })}
        {connections.length > MAX_CHIPS && (
          <li className="self-center text-xs text-gray-500 dark:text-gray-400">
            +{connections.length - MAX_CHIPS} more
          </li>
        )}
      </ul>
    </div>
  );
}

export function ConnectionsPanel({ connections }: { connections: HomeConnection[] }) {
  const sources = connections.filter((c) => c.type === 'source');
  const destinations = connections.filter((c) => c.type === 'destination');

  return (
    <section aria-labelledby="connections-heading" className="w-full">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Database className="w-4 h-4 text-blue-500" aria-hidden />
          <h2 id="connections-heading" className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            Your Connections
          </h2>
        </div>
        <Link
          href="/connections"
          className="inline-flex items-center rounded-md px-2 py-1 text-xs text-gray-500 hover:bg-gray-100 hover:text-gray-700 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-gray-200"
        >
          <Settings className="w-3 h-3 mr-1" aria-hidden />
          Manage
        </Link>
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-4">
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-1 gap-4">
          <ConnectionChipList title="Sources" connections={sources} />
          <ConnectionChipList title="Destinations" connections={destinations} />
        </div>
        <Link
          href="/connections/new"
          className="flex items-center justify-center gap-1.5 rounded-lg border border-dashed border-gray-200 dark:border-gray-700 py-2 text-xs font-medium text-gray-500 hover:border-violet-300 hover:text-violet-700 dark:text-gray-400 dark:hover:border-violet-600 dark:hover:text-violet-300 transition-colors"
        >
          <Plus className="w-3.5 h-3.5" aria-hidden />
          Add connection
        </Link>
      </div>
    </section>
  );
}
