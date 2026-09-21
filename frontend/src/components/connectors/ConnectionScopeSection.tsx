"use client"

import { useState } from "react"
import { Loader2, AlertCircle } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  applyNamespaceFilter,
  NamespaceFilterError,
  parseNamespaceFilter,
  NAMESPACE_FILTER_MODE_KEY,
  NAMESPACE_FILTER_PATTERNS_KEY,
  type NamespaceFilterMode,
  type NamespaceFilterResult,
} from "@/lib/pipeline/namespaceFilter"

// The connection modal's Scope step (#31): which databases or schemas a source
// connection reads. It writes namespace_filter_mode / namespace_filter_patterns
// into the connection config; discovery, pipelines and CDC apply them on the
// server. The preview runs the same filter here over the names the connector
// lists, so what it shows is what the server will keep.

export type ScopeValue = { mode: NamespaceFilterMode; patterns: string }

// scopeFromConfig reads the Scope a config holds. An unknown mode reads as
// written so the section can show it as invalid instead of silently "all".
export function scopeFromConfig(config: Record<string, unknown>): ScopeValue {
  const mode = String(config[NAMESPACE_FILTER_MODE_KEY] ?? "").trim().toLowerCase()
  const patterns = config[NAMESPACE_FILTER_PATTERNS_KEY]
  return {
    mode: (mode || "all") as NamespaceFilterMode,
    patterns: typeof patterns === "string" ? patterns : "",
  }
}

// scopeConfigError is the message a save must stop on, "" when the Scope is
// valid. The server refuses the same configs (fail closed).
export function scopeConfigError(config: Record<string, unknown>): string {
  try {
    parseNamespaceFilter(config)
    return ""
  } catch (e) {
    return e instanceof NamespaceFilterError ? e.message : String(e)
  }
}

// The listing the section previews: GET /connections/:id/namespaces for a saved
// connection, POST /connections/namespaces with the form's config for a new one.
export type NamespaceListing = { namespaces: string[]; current?: string }

const PREVIEW_LIMIT = 50

const MODES: Array<{ mode: NamespaceFilterMode; label: string }> = [
  { mode: "all", label: "All" },
  { mode: "include", label: "Only these" },
  { mode: "exclude", label: "All except" },
]

function plural(kind: string): string {
  if (kind === "database") return "databases"
  if (kind === "dataset") return "datasets"
  return "schemas"
}

function NameList({ names, testId }: { names: string[]; testId: string }) {
  const shown = names.slice(0, PREVIEW_LIMIT)
  return (
    <p data-testid={testId} className="text-xs font-mono break-words text-zinc-600 dark:text-zinc-400">
      {shown.join(", ")}
      {names.length > shown.length && ` … +${names.length - shown.length} more`}
    </p>
  )
}

export function ConnectionScopeSection({
  namespaceKind,
  value,
  onChange,
  pinnedDatabase,
  loadNamespaces,
}: {
  // namespace_model table_namespace: "database", "schema" or "dataset".
  namespaceKind: string
  value: ScopeValue
  onChange: (v: ScopeValue) => void
  // The database a database-namespace connection names: it is then pinned to
  // it and the Scope does not apply (the connector ignores it too).
  pinnedDatabase?: string
  loadNamespaces: () => Promise<NamespaceListing>
}) {
  const kinds = plural(namespaceKind)
  const [loading, setLoading] = useState(false)
  const [listing, setListing] = useState<NamespaceListing | null>(null)
  const [loadError, setLoadError] = useState("")

  if (pinnedDatabase) {
    return (
      <div data-testid="connection-scope" className="space-y-2 rounded-lg border p-4">
        <h3 className="text-sm font-semibold">Scope</h3>
        <p data-testid="connection-scope-pinned" className="text-xs text-zinc-500 dark:text-zinc-400">
          This connection reads one database, <span className="font-mono">{pinnedDatabase}</span>. Leave the
          database empty to reach every database on the server and choose which ones here.
        </p>
      </div>
    )
  }

  const config = { [NAMESPACE_FILTER_MODE_KEY]: value.mode, [NAMESPACE_FILTER_PATTERNS_KEY]: value.patterns }
  const error = scopeConfigError(config)
  let preview: NamespaceFilterResult | null = null
  if (listing && !error) {
    preview = applyNamespaceFilter(listing.namespaces, parseNamespaceFilter(config))
  }

  const runPreview = async () => {
    setLoading(true)
    setLoadError("")
    try {
      setListing(await loadNamespaces())
    } catch (e) {
      setListing(null)
      setLoadError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div data-testid="connection-scope" className="space-y-3 rounded-lg border p-4">
      <div className="space-y-1">
        <h3 className="text-sm font-semibold">Scope</h3>
        <p className="text-xs text-zinc-500 dark:text-zinc-400">
          Which {kinds} this connection reads. Discovery, pipelines and CDC see only these; system {kinds} are
          always left out.
        </p>
      </div>

      <div role="radiogroup" aria-label="Scope" className="grid grid-cols-3 gap-2">
        {MODES.map(({ mode, label }) => (
          <button
            key={mode}
            type="button"
            role="radio"
            aria-checked={value.mode === mode}
            onClick={() => onChange({ ...value, mode })}
            className={`px-3 py-2 rounded-md border-2 text-sm transition-all ${
              value.mode === mode
                ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20 text-violet-700 dark:text-violet-300"
                : "border-zinc-200 dark:border-zinc-700 hover:border-zinc-300 dark:hover:border-zinc-600"
            }`}
          >
            {label === "All" ? `All ${kinds}` : label}
          </button>
        ))}
      </div>

      {value.mode !== "all" && (
        <div className="space-y-1">
          <Label htmlFor="namespace_filter_patterns">
            {value.mode === "include" ? `${kinds[0].toUpperCase()}${kinds.slice(1)} to read` : `${kinds[0].toUpperCase()}${kinds.slice(1)} to leave out`}
          </Label>
          <Input
            id="namespace_filter_patterns"
            value={value.patterns}
            placeholder="sales, crm_*, tenant_*"
            onChange={(e) => onChange({ ...value, patterns: e.target.value })}
          />
          <p className="text-xs text-zinc-500 dark:text-zinc-400">
            Comma-separated. <span className="font-mono">*</span> matches any characters; a name must match a whole
            pattern, ignoring case.
          </p>
        </div>
      )}

      {error && (
        <p role="alert" data-testid="connection-scope-error" className="text-xs text-red-600 dark:text-red-400">
          {error}
        </p>
      )}

      <div className="space-y-2">
        <Button type="button" variant="outline" size="sm" onClick={runPreview} disabled={loading}>
          {loading && <Loader2 className="h-3 w-3 mr-2 animate-spin" />}
          Preview {kinds}
        </Button>
        {loadError && (
          <p data-testid="connection-scope-load-error" className="text-xs text-red-600 dark:text-red-400 flex gap-1">
            <AlertCircle className="h-3 w-3 mt-0.5 flex-shrink-0" />
            Could not list {kinds}: {loadError}
          </p>
        )}
        {preview && (
          <div data-testid="connection-scope-preview" className="space-y-1">
            <p className="text-xs font-medium">
              Reads {preview.kept.length} of {listing?.namespaces.length ?? 0} {kinds}
            </p>
            {preview.kept.length > 0 && <NameList names={preview.kept} testId="connection-scope-kept" />}
            {preview.excluded.length > 0 && (
              <>
                <p className="text-xs font-medium">Leaves out</p>
                <NameList names={preview.excluded} testId="connection-scope-excluded" />
              </>
            )}
            {preview.warning && (
              <p data-testid="connection-scope-warning" className="text-xs text-amber-700 dark:text-amber-400">
                {preview.warning} Pipelines on this connection will find no tables until one exists.
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
