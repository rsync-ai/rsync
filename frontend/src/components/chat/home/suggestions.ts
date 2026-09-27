import { getConnectorDisplayName } from '@/lib/types/mcp-connector';
import type { HomeConnection } from './useHomeData';

export function normalizeConnectorForLogo(type: string): string {
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

// Short names for chips and cards; getConnectorDisplayName is too long for them
// ("Google Cloud Storage").
const SHORT_NAMES: Record<string, string> = {
  postgresql: 'Postgres',
  'aws-s3': 'S3',
  gcs: 'GCS',
  'azure-blob': 'Azure Blob',
  'sample-data': 'Sample data',
};

export function shortConnectorName(type: string): string {
  const id = normalizeConnectorForLogo(type);
  return SHORT_NAMES[id] ?? getConnectorDisplayName(id);
}

export interface QuickPipeline {
  source: string;
  destination: string;
  label: string;
  description: string;
  demo?: boolean;
  // A generic example shown because nothing matched the user's connections.
  needsConnection?: boolean;
}

// Curated templates. Offered first when both sides are connected, and as
// generic examples when nothing matches.
const QUICK_PIPELINES: QuickPipeline[] = [
  { source: 'mysql', destination: 's3', label: 'MySQL → S3', description: 'Database backup to cloud storage' },
  { source: 'postgresql', destination: 'snowflake', label: 'Postgres → Snowflake', description: 'Analytics warehouse sync' },
  { source: 'mongodb', destination: 'bigquery', label: 'MongoDB → BigQuery', description: 'NoSQL to analytics' },
  { source: 'mysql', destination: 'postgresql', label: 'MySQL → Postgres', description: 'Database migration' },
  { source: 's3', destination: 'snowflake', label: 'S3 → Snowflake', description: 'Data lake to warehouse' },
  // The zero-credential demo pair ("Start with sample data" seeds both). Offered
  // only once both connections exist: as a generic example it would point a new
  // user at a source they have not added.
  { source: 'sample-data', destination: 'postgresql', label: 'Sample data → Postgres', description: 'Zero-credential demo', demo: true },
];

const MAX_SUGGESTIONS = 6;

const pairKey = (source: string, destination: string) =>
  `${normalizeConnectorForLogo(source)}>${normalizeConnectorForLogo(destination)}`;

/**
 * Suggestions built from what the user can actually run: matching templates
 * first, then every other connected source → destination type pair. Falls back
 * to the generic templates (minus the demo pair) flagged `needsConnection`, so
 * the section is never blank and never passes an example off as ready.
 */
export function buildQuickPipelines(connections: HomeConnection[]): QuickPipeline[] {
  const sourceTypes = new Set<string>();
  const destTypes = new Set<string>();
  for (const c of connections) {
    const id = normalizeConnectorForLogo(c.connector_type);
    if (c.type === 'source') sourceTypes.add(id);
    else if (c.type === 'destination') destTypes.add(id);
  }
  // Templates predate the source/destination split in the matcher; either role
  // counts, as before.
  const anyType = new Set([...sourceTypes, ...destTypes]);

  const out: QuickPipeline[] = QUICK_PIPELINES.filter(
    (p) => anyType.has(normalizeConnectorForLogo(p.source)) && anyType.has(normalizeConnectorForLogo(p.destination))
  );
  const seen = new Set(out.map((p) => pairKey(p.source, p.destination)));

  for (const src of sourceTypes) {
    for (const dst of destTypes) {
      if (out.length >= MAX_SUGGESTIONS) break;
      const key = pairKey(src, dst);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        source: src,
        destination: dst,
        label: `${shortConnectorName(src)} → ${shortConnectorName(dst)}`,
        description: 'From your connections',
      });
    }
  }

  if (out.length > 0) return out.slice(0, MAX_SUGGESTIONS);
  return QUICK_PIPELINES.filter((p) => !p.demo).map((p) => ({ ...p, needsConnection: true }));
}

/** Example prompts for the hero input, grounded in the first suggestion the user can run. */
export function buildExamplePrompts(suggestions: QuickPipeline[]): string[] {
  const ready = suggestions.find((p) => !p.needsConnection && !p.demo);
  if (!ready) return ['sync mysql to s3', 'backup postgres daily', 'stream mongodb changes to bigquery'];
  const src = shortConnectorName(ready.source).toLowerCase();
  const dst = shortConnectorName(ready.destination).toLowerCase();
  return [`sync ${src} to ${dst}`, `stream ${src} changes to ${dst}`, `copy ${src} to ${dst} every night`];
}
