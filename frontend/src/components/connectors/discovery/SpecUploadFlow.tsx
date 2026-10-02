"use client"

import { useState, type ReactNode } from "react"
import Link from "next/link"
import { useSearchParams } from "next/navigation"
import { AlertTriangle, FileUp, Link2, Loader2, ClipboardPaste, Play } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  DiscoveryError,
  generateFromSpec,
  type GenerateFromSpecResponse,
} from "@/lib/api/discovery"

type Source = "file" | "url" | "paste"

const INPUT_CLASS =
  "w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"

/** Same rule as the gateway's normalizeConnectorName: lowercase kebab-case. */
export function toConnectorSlug(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
}

/**
 * `info.title` from a JSON or YAML document, to prefill the name. A best
 * guess only -- the generator parses the document properly.
 */
export function specTitle(text: string): string {
  try {
    const doc = JSON.parse(text)
    return typeof doc?.info?.title === "string" ? doc.info.title : ""
  } catch {
    const m = text.match(/^info:[ \t]*\r?\n(?:[ \t]+.*\r?\n)*?[ \t]+title:[ \t]*(['"]?)(.+?)\1[ \t]*\r?$/m)
    return m ? m[2] : ""
  }
}

interface Failure {
  message: string
  suggestions: string[]
}

/**
 * Generate a connector from an OpenAPI / Swagger document.
 *
 * Shown instead of DiscoveryFlow where the discovery service is not served
 * (the community image strips it). It posts to the same guarded
 * /api/v1/connectors/generate as the wizard, with the document inline.
 */
export function SpecUploadFlow() {
  const searchParams = useSearchParams()
  const prefilledName = toConnectorSlug(searchParams?.get("name") || "")

  const [source, setSource] = useState<Source>("file")
  const [specText, setSpecText] = useState("")
  const [specLabel, setSpecLabel] = useState("")
  const [specUrl, setSpecUrl] = useState("")
  const [fetchingUrl, setFetchingUrl] = useState(false)

  const [name, setName] = useState(prefilledName)
  const [nameEdited, setNameEdited] = useState(prefilledName !== "")
  const [baseUrl, setBaseUrl] = useState("")
  const [replace, setReplace] = useState(false)

  const [generating, setGenerating] = useState(false)
  const [result, setResult] = useState<GenerateFromSpecResponse | null>(null)
  const [failure, setFailure] = useState<Failure | null>(null)

  const takeSpec = (text: string, label: string) => {
    setSpecText(text)
    setSpecLabel(label)
    setResult(null)
    setFailure(null)
    if (!nameEdited) setName(toConnectorSlug(specTitle(text)))
  }

  const onFile = async (file: File | undefined) => {
    if (!file) return
    takeSpec(await file.text(), file.name)
  }

  // The browser fetches the document, not the server: the generator refuses
  // server-side URL fetches, and this request carries no rsync.ai credentials.
  const onFetchUrl = async () => {
    const url = specUrl.trim()
    if (!/^https?:\/\//i.test(url)) {
      setFailure({ message: "Enter an http:// or https:// URL.", suggestions: [] })
      return
    }
    setFetchingUrl(true)
    setFailure(null)
    try {
      const res = await fetch(url, { credentials: "omit" })
      if (!res.ok) {
        setFailure({ message: `${url} answered ${res.status} ${res.statusText}`.trim(), suggestions: [] })
        return
      }
      takeSpec(await res.text(), url)
    } catch {
      setFailure({
        message: `Could not fetch ${url} from your browser. The host may not allow cross-origin requests.`,
        suggestions: ["Download the file and upload it instead."],
      })
    } finally {
      setFetchingUrl(false)
    }
  }

  const slug = toConnectorSlug(name)
  const canGenerate = specText.trim() !== "" && slug !== "" && !generating && !fetchingUrl

  const onGenerate = async () => {
    setGenerating(true)
    setResult(null)
    setFailure(null)
    try {
      const res = await generateFromSpec(slug, specText, { baseUrl, forceRegenerate: replace })
      if (res.success) {
        setResult(res)
      } else {
        setFailure({
          message: res.error_message || "Generation failed.",
          suggestions: res.suggestions ?? [],
        })
      }
    } catch (e) {
      if (e instanceof DiscoveryError) {
        setFailure({ message: e.message, suggestions: e.suggestions })
      } else {
        setFailure({ message: e instanceof Error ? e.message : String(e), suggestions: [] })
      }
    } finally {
      setGenerating(false)
    }
  }

  return (
    <div
      className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,22rem)]"
      data-testid="spec-upload-flow"
    >
      <div className="rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shadow-sm p-5 space-y-5">
        <div>
          <div className="text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-2">
            OpenAPI or Swagger document (JSON or YAML)
          </div>
          <div className="inline-flex rounded-md border border-zinc-200 dark:border-zinc-800 p-0.5 text-xs">
            <SourceTab id="file" current={source} onSelect={setSource} icon={<FileUp className="h-3.5 w-3.5" />} label="Upload file" />
            <SourceTab id="url" current={source} onSelect={setSource} icon={<Link2 className="h-3.5 w-3.5" />} label="From URL" />
            <SourceTab id="paste" current={source} onSelect={setSource} icon={<ClipboardPaste className="h-3.5 w-3.5" />} label="Paste" />
          </div>

          <div className="mt-3">
            {source === "file" && (
              <input
                type="file"
                accept=".json,.yaml,.yml,application/json,application/yaml,text/yaml"
                aria-label="OpenAPI document file"
                data-testid="spec-file-input"
                onChange={(e) => onFile(e.target.files?.[0])}
                className="block w-full text-sm text-zinc-700 dark:text-zinc-300 file:mr-3 file:rounded-md file:border-0 file:bg-violet-50 dark:file:bg-violet-950/40 file:px-3 file:py-1.5 file:text-xs file:font-medium file:text-violet-700 dark:file:text-violet-300"
              />
            )}
            {source === "url" && (
              <div className="flex gap-2">
                <input
                  type="url"
                  value={specUrl}
                  onChange={(e) => setSpecUrl(e.target.value)}
                  placeholder="https://example.com/openapi.json"
                  aria-label="OpenAPI document URL"
                  data-testid="spec-url-input"
                  className={INPUT_CLASS}
                />
                <Button
                  type="button"
                  variant="outline"
                  onClick={onFetchUrl}
                  disabled={fetchingUrl || specUrl.trim() === ""}
                  data-testid="spec-url-fetch"
                >
                  {fetchingUrl ? <Loader2 className="h-4 w-4 animate-spin" /> : "Fetch"}
                </Button>
              </div>
            )}
            {source === "paste" && (
              <textarea
                value={specLabel === "pasted" ? specText : ""}
                onChange={(e) => takeSpec(e.target.value, "pasted")}
                rows={10}
                placeholder='{"openapi": "3.0.0", "info": {"title": "My API"}, "paths": {...}}'
                aria-label="OpenAPI document text"
                data-testid="spec-paste-input"
                className={`${INPUT_CLASS} text-[11px] font-mono`}
              />
            )}
          </div>
          {specText && specLabel !== "pasted" && (
            <div className="mt-2 text-[11px] text-zinc-500 dark:text-zinc-400" data-testid="spec-loaded">
              Loaded <span className="font-mono break-all">{specLabel}</span> ({specText.length.toLocaleString()} characters)
            </div>
          )}
        </div>

        <div>
          <label htmlFor="spec-connector-name" className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
            Connector name
          </label>
          <input
            id="spec-connector-name"
            type="text"
            value={name}
            onChange={(e) => {
              setName(e.target.value)
              setNameEdited(true)
            }}
            placeholder="my-api"
            className={`${INPUT_CLASS} font-mono`}
          />
          <div className="text-[10px] text-zinc-500 dark:text-zinc-400 mt-1">
            {slug && slug !== name.trim() ? (
              <>Will be saved as <code>{slug}</code>.</>
            ) : (
              "Lowercase letters, digits and hyphens. Taken from the document's title until you edit it."
            )}
          </div>
        </div>

        <div>
          <label htmlFor="spec-base-url" className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
            Base URL <span className="text-zinc-400 font-normal">(optional)</span>
          </label>
          <input
            id="spec-base-url"
            type="url"
            value={baseUrl}
            onChange={(e) => setBaseUrl(e.target.value)}
            placeholder="https://api.example.com"
            className={INPUT_CLASS}
          />
          <div className="text-[10px] text-zinc-500 dark:text-zinc-400 mt-1">
            Overrides the document&apos;s <code>servers</code> entry. Needed when it has none.
          </div>
        </div>

        <label className="flex items-center gap-2 text-xs text-zinc-700 dark:text-zinc-300">
          <input
            type="checkbox"
            checked={replace}
            onChange={(e) => setReplace(e.target.checked)}
            data-testid="spec-replace"
          />
          Replace an existing connector with this name
        </label>

        <Button
          type="button"
          onClick={onGenerate}
          disabled={!canGenerate}
          className="w-full bg-gradient-to-r from-violet-600 to-indigo-600 text-white"
          data-testid="spec-generate"
        >
          {generating ? (
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <Play className="h-4 w-4 mr-2" />
          )}
          Generate connector
        </Button>
      </div>

      <aside
        className="rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shadow-sm lg:sticky lg:top-6 self-start"
        data-testid="generation-panel"
      >
        <div className="px-5 pt-5 pb-3 border-b border-zinc-100 dark:border-zinc-800">
          <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Generation result</h2>
        </div>
        <div className="px-5 py-5 text-xs">
          <SpecResult generating={generating} result={result} failure={failure} />
        </div>
      </aside>
    </div>
  )
}

function SourceTab({
  id,
  current,
  onSelect,
  icon,
  label,
}: {
  id: Source
  current: Source
  onSelect: (s: Source) => void
  icon: ReactNode
  label: string
}) {
  const active = id === current
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      data-testid={`spec-source-${id}`}
      onClick={() => onSelect(id)}
      className={
        active
          ? "inline-flex items-center gap-1.5 rounded px-2.5 py-1 bg-violet-600 text-white"
          : "inline-flex items-center gap-1.5 rounded px-2.5 py-1 text-zinc-600 dark:text-zinc-400 hover:bg-zinc-100 dark:hover:bg-zinc-900"
      }
    >
      {icon}
      {label}
    </button>
  )
}

function SpecResult({
  generating,
  result,
  failure,
}: {
  generating: boolean
  result: GenerateFromSpecResponse | null
  failure: Failure | null
}) {
  if (generating) {
    return (
      <div className="flex flex-col items-center py-8 text-center">
        <Loader2 className="h-8 w-8 text-violet-500 animate-spin mb-3" />
        <div className="text-sm font-medium text-zinc-800 dark:text-zinc-200">Generating connector…</div>
      </div>
    )
  }
  if (failure) {
    return (
      <div
        className="rounded-lg border border-red-300 dark:border-red-900 bg-red-50 dark:bg-red-950/30 p-3 text-red-800 dark:text-red-200"
        data-testid="spec-result-error"
        role="alert"
      >
        <div className="flex gap-2">
          <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
          <div className="break-words">{failure.message}</div>
        </div>
        {failure.suggestions.length > 0 && (
          <ul className="mt-2 ml-6 list-disc space-y-0.5">
            {failure.suggestions.map((s, i) => (
              <li key={i}>{s}</li>
            ))}
          </ul>
        )}
      </div>
    )
  }
  if (result?.status === "already_exists") {
    return (
      <div
        className="rounded-lg border border-amber-300 dark:border-amber-900 bg-amber-50 dark:bg-amber-950/30 p-3 text-amber-900 dark:text-amber-200"
        data-testid="spec-already-exists"
      >
        A connector named <code className="font-mono">{result.connector_name}</code> already exists, so nothing
        was generated. Tick <strong>Replace an existing connector</strong> and generate again to overwrite it, or
        choose another name.
      </div>
    )
  }
  if (result?.success) {
    const notes = result.metadata?.notes ?? []
    const warnings = result.draft_warnings ?? []
    return (
      <div className="space-y-3" data-testid="spec-result-success">
        <div className="rounded-lg border border-emerald-300 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-950/30 p-3">
          <div className="text-sm font-semibold text-emerald-900 dark:text-emerald-200">
            ✓ Generated <code className="font-mono">{result.connector_name}</code>
          </div>
          <div className="text-[11px] text-emerald-700 dark:text-emerald-300 mt-1 leading-relaxed">
            {result.operation_count != null && (
              <>operations: <strong>{result.operation_count}</strong><br /></>
            )}
            {result.version && (
              <>version: <strong>{result.version}</strong></>
            )}
          </div>
        </div>
        {result.metadata?.managed_connectors === false && (
          <div className="rounded-md border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40 px-3 py-2 text-zinc-600 dark:text-zinc-400">
            This install does not build or start generated connectors. The files were saved
            {result.output_path ? <> to <code className="font-mono break-all">{result.output_path}</code></> : null}.
          </div>
        )}
        {[...notes, ...warnings].length > 0 && (
          <ul className="list-disc ml-4 space-y-0.5 text-zinc-600 dark:text-zinc-400" data-testid="spec-result-notes">
            {[...notes, ...warnings].map((n, i) => (
              <li key={i}>{n}</li>
            ))}
          </ul>
        )}
        <Button asChild size="sm" className="w-full bg-gradient-to-r from-violet-600 to-indigo-600 text-white">
          <Link href="/connectors">View connectors</Link>
        </Button>
      </div>
    )
  }
  return (
    <div className="py-8 text-center text-zinc-500 dark:text-zinc-400">
      Load a document, check the name, then generate.
    </div>
  )
}
