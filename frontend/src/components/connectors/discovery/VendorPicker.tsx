"use client"

import { useEffect, useState } from "react"
import { Check, ChevronDown, Search, Sparkles } from "lucide-react"
import { cn } from "@/lib/utils"
import { listVendors, type Vendor, type VendorApi } from "@/lib/api/discovery"

interface Props {
  apiName: string
  onApiNameChange: (name: string) => void
  vendor: Vendor | null
  onVendorPick: (vendor: Vendor | null) => void
  variantId: string | null
  onVariantPick: (variantId: string | null) => void
}

/**
 * Type-ahead vendor input + variant card picker.
 *
 * Behaviour:
 *   - User types api_name; we debounce-call /v1/vendors?q=...
 *   - When ≥1 match, show suggestion list
 *   - When user picks a vendor, expand its variants as cards
 *   - Single-variant vendors auto-pick
 */
export function VendorPicker({
  apiName,
  onApiNameChange,
  vendor,
  onVendorPick,
  variantId,
  onVariantPick,
}: Props) {
  const [suggestions, setSuggestions] = useState<Vendor[]>([])
  const [loading, setLoading] = useState(false)
  const [showSuggestions, setShowSuggestions] = useState(false)

  useEffect(() => {
    const q = apiName.trim()
    if (!q || vendor?.id === q.toLowerCase()) {
      setSuggestions([])
      return
    }
    let cancelled = false
    const timer = setTimeout(async () => {
      setLoading(true)
      try {
        const results = await listVendors(q)
        if (!cancelled) setSuggestions(results.slice(0, 6))
      } catch {
        if (!cancelled) setSuggestions([])
      } finally {
        if (!cancelled) setLoading(false)
      }
    }, 220)
    return () => {
      cancelled = true
      clearTimeout(timer)
    }
  }, [apiName, vendor?.id])

  // Auto-pick single-variant vendors
  useEffect(() => {
    if (vendor && vendor.apis.length === 1 && !variantId) {
      onVariantPick(vendor.apis[0].id)
    }
  }, [vendor, variantId, onVariantPick])

  return (
    <div className="space-y-3">
      {/* API name input with type-ahead */}
      <div className="relative">
        <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
          API name
        </label>
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-400 pointer-events-none" />
          <input
            type="text"
            value={apiName}
            onChange={(e) => {
              onApiNameChange(e.target.value)
              onVendorPick(null)
              onVariantPick(null)
              setShowSuggestions(true)
            }}
            onFocus={() => setShowSuggestions(true)}
            onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
            placeholder="Search thousands of APIs (stripe, twilio, github…) or paste a spec"
            className="w-full pl-9 pr-3 py-2 text-sm rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
            autoComplete="off"
          />
        </div>
        {showSuggestions && !vendor && apiName.trim() && (
          <div className="absolute z-10 mt-1 w-full rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 shadow-lg overflow-hidden">
            {suggestions.map((v) => (
              <button
                key={v.id}
                type="button"
                onMouseDown={(e) => {
                  e.preventDefault()
                  onApiNameChange(v.id)
                  onVendorPick(v)
                  setShowSuggestions(false)
                }}
                className="w-full flex items-center justify-between gap-3 px-3 py-2 text-left text-sm hover:bg-zinc-50 dark:hover:bg-zinc-800"
              >
                <div>
                  <div className="font-medium text-zinc-900 dark:text-zinc-100">
                    {v.display_name}
                  </div>
                  <div className="text-[11px] text-zinc-500">
                    {v.apis.length} {v.apis.length === 1 ? "API" : "APIs"} · {v.category}
                  </div>
                </div>
                {v.source_tier === 1 && (
                  <span className="text-[9px] uppercase font-bold text-violet-600 dark:text-violet-400 tracking-wide">
                    Curated
                  </span>
                )}
                {v.source_tier === 3 && (
                  <span className="text-[9px] uppercase font-bold text-sky-600 dark:text-sky-400 tracking-wide">
                    Directory
                  </span>
                )}
              </button>
            ))}
            {/* Always offer the custom-API path so unlisted vendors
                (pipedrive, zoho, etc.) don't look unsupported. */}
            <button
              type="button"
              onMouseDown={(e) => {
                e.preventDefault()
                onVendorPick(null)
                onVariantPick(null)
                setShowSuggestions(false)
              }}
              className="w-full flex items-center justify-between gap-3 px-3 py-2 text-left text-sm bg-zinc-50 dark:bg-zinc-800/40 hover:bg-zinc-100 dark:hover:bg-zinc-800 border-t border-zinc-200 dark:border-zinc-700"
            >
              <div>
                <div className="font-medium text-zinc-900 dark:text-zinc-100">
                  Use “{apiName.trim()}” as a custom API
                </div>
                <div className="text-[11px] text-zinc-500">
                  We&apos;ll discover protocol, auth, and operations from the docs URL.
                </div>
              </div>
              <Sparkles className="h-3.5 w-3.5 text-violet-500 shrink-0" />
            </button>
          </div>
        )}
        {loading && apiName.trim() && (
          <div className="absolute right-3 top-9 text-[10px] text-zinc-400">searching…</div>
        )}
      </div>

      {/* Vendor variant picker */}
      {vendor && vendor.apis.length > 1 && (
        <div>
          <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
            {vendor.display_name} has {vendor.apis.length} APIs — pick one
          </label>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            {vendor.apis.map((api) => (
              <VariantCard
                key={api.id}
                api={api}
                selected={api.id === variantId}
                onPick={() => onVariantPick(api.id)}
              />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function VariantCard({
  api,
  selected,
  onPick,
}: {
  api: VendorApi
  selected: boolean
  onPick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onPick}
      className={cn(
        "text-left p-3 rounded-lg border transition-colors",
        selected
          ? "border-violet-500 bg-violet-50 dark:bg-violet-950/30 ring-1 ring-violet-500"
          : "border-zinc-200 dark:border-zinc-700 hover:bg-zinc-50 dark:hover:bg-zinc-800/50",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
              {api.name}
            </span>
            {api.recommended && (
              <span className="inline-flex items-center gap-1 text-[9px] font-bold uppercase tracking-wide text-violet-700 dark:text-violet-400 bg-violet-100 dark:bg-violet-900/40 px-1.5 py-0.5 rounded">
                <Sparkles className="h-2.5 w-2.5" />
                Recommended
              </span>
            )}
            <span className="text-[10px] uppercase tracking-wide text-zinc-500 font-mono">
              {api.protocol}
            </span>
          </div>
          {api.reason && (
            <div className="mt-1 text-[11px] text-zinc-600 dark:text-zinc-400 leading-snug">
              {api.reason}
            </div>
          )}
          <div className="mt-1 text-[10px] font-mono text-zinc-500 truncate">
            {api.endpoint_template}
          </div>
        </div>
        {selected && <Check className="h-4 w-4 text-violet-600 shrink-0 mt-0.5" />}
      </div>
    </button>
  )
}
