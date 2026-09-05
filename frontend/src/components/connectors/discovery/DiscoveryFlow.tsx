"use client"

import { useCallback, useEffect, useState } from "react"
import { useSearchParams } from "next/navigation"
import { AlertTriangle, Loader2, Play, RefreshCw, Sparkles } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  confirmDiscovery,
  deleteDiscovery,
  generateFromSession,
  startDiscovery,
  type Dimension,
  type DiscoveryError,
  type GenerateFromSessionResponse,
  type GenerationContract,
  type Vendor,
} from "@/lib/api/discovery"
import { fetchMCPConnector } from "@/lib/api/mcp-connectors"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS, API_GATEWAY_URL } from "@/lib/config/api"
import { VendorPicker } from "./VendorPicker"
import { ContractPreviewCard } from "./ContractPreviewCard"

interface Props {
  onGenerate?: (contract: GenerationContract) => void | Promise<void>
}

/**
 * Two-phase generation flow:
 *   1. User picks vendor + variant (or types unknown api_name + docs URL)
 *   2. Click "Discover" → start_discovery → contract preview
 *   3. Fill in any open questions → confirm → contract updates
 *   4. When contract.can_generate=true → "Generate connector" enabled
 */
export function DiscoveryFlow({ onGenerate }: Props) {
  // Accept `?name=X` so the chat "generate X" chip can deep-link here
  // with the API name pre-filled. The user still has to pick vendor /
  // docs URL / auth — we don't auto-discover from the URL hint.
  const searchParams = useSearchParams()
  const prefilledName = (searchParams?.get("name") || "").trim()

  const [apiName, setApiName] = useState(prefilledName)
  const [vendor, setVendor] = useState<Vendor | null>(null)
  const [variantId, setVariantId] = useState<string | null>(null)
  const [docsUrl, setDocsUrl] = useState("")
  const [protocolHint, setProtocolHint] = useState<string>("auto")

  const [contract, setContract] = useState<GenerationContract | null>(null)
  const [pendingAnswers, setPendingAnswers] = useState<
    Partial<Record<Dimension, unknown>>
  >({})

  const [discovering, setDiscovering] = useState(false)
  const [confirming, setConfirming] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [generationResult, setGenerationResult] = useState<GenerateFromSessionResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Phase 13c — advanced options ported from the classic form
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [forceRegenerate, setForceRegenerate] = useState(false)
  // P0 — collapse the spec-first inputs (OpenAPI / GraphQL / curl) for custom
  // APIs so a non-technical user sees only name + docs URL + type on first
  // paint. Separate from showAdvanced (Phase-2 force-regenerate) so the two
  // expanders toggle independently.
  const [showSpecFields, setShowSpecFields] = useState(false)

  // Proactively check if a connector with the same name already exists
  // once the contract becomes READY. null=unchecked, false=new, true=exists.
  const [connectorExists, setConnectorExists] = useState<boolean | null>(null)

  // Phase 13d: surface name-normalization + "did you mean" suggestions from
  // the api-gateway's ValidateConnector endpoint as the user types. The
  // backend already returns suggested correct name + similar connectors;
  // the wizard used to ignore the endpoint, so this is the wiring.
  interface NameCheck {
    suggestedName?: string
    similar?: string[]
    warning?: string
    isKnownAPI?: boolean
    alreadyExists?: boolean
    // Non-blocking advisory: existing connectors that are near-duplicates of this
    // NEW id (same vendor root like `stripe` vs `stripe-demo`, or same display
    // name under a different id). Distinct from `alreadyExists` — generation is
    // still allowed; we just warn that a SEPARATE connector will be created.
    nearDuplicates?: string[]
    // Hard gates: when the backend says valid=false (test value, malformed
    // slug, etc.) or can_generate=false (already-exists conflict),
    // Discover must be disabled. Without this the user can click through
    // a clearly-flagged-bad name and waste a discovery cycle producing
    // an honest refusal we already knew was coming.
    valid?: boolean
    canGenerate?: boolean
  }
  const [nameCheck, setNameCheck] = useState<NameCheck | null>(null)

  // Auto-detected category (POST /api/v1/connectors/detect-category).
  // Used to seed the api_category_hint we pass to generation so the
  // architect picks the right templates + capability defaults. Backend
  // already serves the endpoint; the wizard used to ignore it.
  interface CategoryDetect {
    category_hint?: string
    confidence?: number
    source?: string
  }
  const [categoryDetect, setCategoryDetect] = useState<CategoryDetect | null>(null)

  // Phase 13g — optional description so the user can override the
  // auto-derived "<api_name> connector" string. Stored in metadata and
  // shown on the connector card.
  const [description, setDescription] = useState("")

  // Editable connector slug. Auto-derived as "api_name" or
  // "api_name-variant_id" when a variant is selected. User can override it
  // to avoid collisions (e.g. shopify-admin-graphql vs shopify-admin-rest).
  const [connectorName, setConnectorName] = useState("")
  const [connectorNameEdited, setConnectorNameEdited] = useState(false)

  // Spec-first inputs. Paste an OpenAPI/Swagger spec or point at a GraphQL
  // endpoint to generate deterministically from the machine-readable contract
  // instead of LLM-scraping docs.
  const [openApiSpec, setOpenApiSpec] = useState("")
  const [graphqlEndpoint, setGraphqlEndpoint] = useState("")
  // REST base URL (e.g. https://api.github.com) — sent as base_url_hint so the
  // contract targets the right host. Also the endpoint for GraphQL when you paste
  // an introspection schema (which carries no endpoint of its own).
  const [baseUrl, setBaseUrl] = useState("")
  // Inline GraphQL introspection JSON — deterministic, no live endpoint needed.
  const [graphqlSchema, setGraphqlSchema] = useState("")
  // P0 — paste-a-curl escape hatch. A real example request from the docs is
  // converted server-side to a minimal OpenAPI spec (no LLM guessing).
  const [curlExample, setCurlExample] = useState("")

  const reset = useCallback(async () => {
    if (contract?.session_id) {
      try {
        await deleteDiscovery(contract.session_id)
      } catch {
        /* best-effort */
      }
    }
    setContract(null)
    setPendingAnswers({})
    setError(null)
  }, [contract?.session_id])

  const startFlow = useCallback(async () => {
    if (!apiName.trim()) {
      setError("API name is required")
      return
    }
    setDiscovering(true)
    setError(null)
    try {
      // Tier-3 directory (APIs.guru) entries aren't in the vendor registry —
      // they carry a spec URL instead. Feed that to the deterministic
      // spec-first path and skip the (unresolvable) vendor_id/variant_id.
      const selectedVariant =
        vendor?.apis.find((a) => a.id === variantId) ?? null
      const directorySpecUrl =
        vendor?.source_tier === 3 ? selectedVariant?.openapi_spec_url || null : null

      const res = await startDiscovery({
        api_name: apiName.trim(),
        docs_url: docsUrl.trim() || null,
        protocol_hint: protocolHint === "auto" ? null : (protocolHint as
          | "rest"
          | "graphql"
          | "openapi"),
        vendor_id: directorySpecUrl ? null : vendor?.id || null,
        api_variant_id: directorySpecUrl ? null : variantId,
        openapi_spec: openApiSpec.trim() || null,
        openapi_spec_url: directorySpecUrl,
        graphql_endpoint: graphqlEndpoint.trim() || null,
        graphql_schema: graphqlSchema.trim() || null,
        base_url_hint: baseUrl.trim() || null,
        curl_example: curlExample.trim() || null,
      })
      setContract(res.contract)
      setPendingAnswers({})
    } catch (e) {
      const err = e as DiscoveryError
      setError(`Discovery failed: ${err.message}`)
    } finally {
      setDiscovering(false)
    }
  }, [apiName, docsUrl, protocolHint, vendor?.id, variantId, openApiSpec, graphqlEndpoint, graphqlSchema, baseUrl, curlExample])

  const submitAnswers = useCallback(async () => {
    if (!contract) return
    setConfirming(true)
    setError(null)
    try {
      const res = await confirmDiscovery(contract.session_id, pendingAnswers)
      setContract(res.contract)
      setPendingAnswers({})
    } catch (e) {
      const err = e as DiscoveryError
      setError(`Confirm failed: ${err.message}`)
    } finally {
      setConfirming(false)
    }
  }, [contract, pendingAnswers])

  const handleGenerate = useCallback(async () => {
    if (!contract?.can_generate) return
    setGenerating(true)
    setError(null)
    setGenerationResult(null)
    try {
      // Phase 13a: actually generate the connector by handing the session_id
      // off to the api-gateway → tool-generator fast path.
      const result = await generateFromSession(connectorName || contract.api_name, contract.session_id, {
        description: description.trim() || `${connectorName || contract.api_name} connector`,
        forceRegenerate,
        apiCategoryHint: categoryDetect?.category_hint,
      })
      setGenerationResult(result)
      if (!result.success) {
        setError(result.error_message || `Generation failed at ${result.error_stage || "unknown"}`)
        return
      }
      // Optional caller hook (e.g., navigation)
      await onGenerate?.(contract)
    } catch (e) {
      const err = e as { status?: number; message?: string }
      // The api-gateway restricts generation to power_user/admin. The generate
      // page gates base users out before they reach here, but surface a clear
      // message if a 403 still slips through (e.g. a mid-session tier change)
      // instead of an opaque "Generation failed".
      if (err?.status === 403) {
        setError(
          "Connector generation requires power user or admin access. Contact an admin to upgrade your account.",
        )
      } else {
        setError(err?.message || "Generation failed")
      }
    } finally {
      setGenerating(false)
    }
  }, [contract, onGenerate, description, forceRegenerate, connectorName])

  // Auto-derive connector slug from api_name + variant unless user edited it.
  // Full slugify (not just whitespace→hyphen) so a natural-cased or punctuated
  // API name like "GitHub" or "Poke API" becomes a valid kebab slug
  // ("github" / "poke-api"). The slug is what /validate checks and what generate
  // uses, so this keeps capitalized/spaced names from being blocked at Discover.
  useEffect(() => {
    if (connectorNameEdited) return
    const base = apiName
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
    setConnectorName(variantId ? `${base}-${variantId}` : base)
  }, [apiName, variantId, connectorNameEdited])

  // Reset session when api_name changes meaningfully
  useEffect(() => {
    setConnectorNameEdited(false)
    if (contract && contract.api_name !== apiName.trim().toLowerCase()) {
      setContract(null)
      setPendingAnswers({})
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiName])

  // When discovery has produced a contract (regardless of can_generate),
  // check whether a connector with that slug already exists. Auto-enables
  // force-regenerate so the user can overwrite.
  //
  // NOTE: the backend sets contract.can_generate=false when the connector
  // already exists (service.py:857). Gating this effect on can_generate
  // would skip the existence check in exactly the case we need to surface
  // the Force-regenerate checkbox — the user would see the "already exists"
  // warning text but no UI to act on it.
  useEffect(() => {
    if (!contract || !connectorName) {
      setConnectorExists(null)
      return
    }
    let cancelled = false
    fetchMCPConnector(connectorName)
      .then(() => {
        if (!cancelled) {
          setConnectorExists(true)
          setForceRegenerate(true)
        }
      })
      .catch(() => {
        if (!cancelled) {
          setConnectorExists(false)
          setForceRegenerate(false)
        }
      })
    return () => { cancelled = true }
  }, [contract, connectorName])

  // Debounced name validation against /api/v1/connectors/validate.
  // Surfaces normalization suggestions ("Calendly" → "calendly"), existing-
  // connector warnings, and similar-name hints inline, before the user
  // bothers clicking Discover.
  useEffect(() => {
    const name = apiName.trim()
    if (!name) {
      setNameCheck(null)
      setCategoryDetect(null)
      return
    }
    // Validate the normalized slug, NOT the raw human term. The backend enforces
    // a kebab-only rule, so a natural-cased or punctuated name ("GitHub",
    // "Poke API") returns valid=false and disables Discover even though the
    // derived slug ("github" / "poke-api") — which is what generate actually uses
    // — is valid. Validating the slug also makes "already exists" detection match
    // the real connector id.
    const slug = name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
    let cancelled = false
    const handle = setTimeout(async () => {
      // Fire validate + detect-category in parallel. Both are idempotent
      // POSTs against the api-gateway and the backend already has the
      // logic; the wizard was the missing consumer for category.
      try {
        const [valRes, catRes] = await Promise.all([
          authFetch(API_ENDPOINTS.CONNECTORS.VALIDATE, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ connector_name: slug }),
          }),
          authFetch(`${API_GATEWAY_URL}/api/v1/connectors/detect-category`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ api_name: name }),
          }).catch(() => null),
        ])
        if (cancelled) return
        if (valRes.ok) {
          const data = await valRes.json()
          // F-1: backend `/validate` returns `normalized_name` in
          // underscore form (e.g. "shopify_admin_graphql") while also
          // enforcing a kebab-only rule on the input. Treating the
          // underscore variant as a "suggestion" makes the wizard
          // oscillate: user clicks "Did you mean shopify_admin_graphql"
          // → input becomes invalid → backend suggests reverting to
          // kebab → loop. Suppress the suggestion when the only
          // difference is hyphen↔underscore — the input is already
          // structurally normalized.
          const nameLower = name.toLowerCase()
          const normalizedDiffersByCaseOnly =
            typeof data?.normalized_name === "string" &&
            data.normalized_name !== nameLower &&
            data.normalized_name.replace(/_/g, "-") !== nameLower &&
            data.normalized_name.replace(/-/g, "_") !== nameLower
          setNameCheck({
            suggestedName:
              typeof data?.correct_name === "string" && data.correct_name !== slug
                ? data.correct_name
                : normalizedDiffersByCaseOnly
                  ? data.normalized_name
                  : undefined,
            similar: Array.isArray(data?.similar_connectors)
              ? data.similar_connectors.filter((s: unknown) => typeof s === "string")
              : [],
            warning: typeof data?.warning === "string" ? data.warning : undefined,
            isKnownAPI: Boolean(data?.is_known_api),
            alreadyExists: !data?.can_generate && /already exists/i.test(String(data?.warning || "")),
            nearDuplicates: Array.isArray(data?.near_duplicate_connectors)
              ? data.near_duplicate_connectors.filter((s: unknown) => typeof s === "string")
              : [],
            valid: typeof data?.valid === "boolean" ? data.valid : undefined,
            canGenerate: typeof data?.can_generate === "boolean" ? data.can_generate : undefined,
          })
        }
        if (catRes && catRes.ok) {
          const cd = await catRes.json()
          if (typeof cd?.category_hint === "string" && cd.category_hint) {
            setCategoryDetect({
              category_hint: cd.category_hint,
              confidence: typeof cd?.confidence === "number" ? cd.confidence : undefined,
              source: typeof cd?.source === "string" ? cd.source : undefined,
            })
          } else {
            setCategoryDetect(null)
          }
        }
      } catch {
        if (!cancelled) setNameCheck(null)
      }
    }, 350)
    return () => {
      cancelled = true
      clearTimeout(handle)
    }
  }, [apiName])

  const hasPendingAnswers = Object.values(pendingAnswers).some(
    (v) => v != null && v !== "" && !(Array.isArray(v) && v.length === 0),
  )

  return (
    <div className="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_360px] gap-6 items-start">
      <div className="space-y-4 min-w-0">
      {!contract ? (
        // ── Phase 1: Inputs ────────────────────────────────────────────
        <div className="space-y-4">
          <div className="rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shadow-sm">
            <div className="px-5 pt-5 pb-3 border-b border-zinc-100 dark:border-zinc-800">
              <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                Connector details
              </h2>
              <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-0.5">
                Pick a vendor or type any API name — we&apos;ll discover the rest.
              </p>
            </div>
            <div className="px-5 py-5 space-y-4">
              <VendorPicker
                apiName={apiName}
                onApiNameChange={setApiName}
                vendor={vendor}
                onVendorPick={setVendor}
                variantId={variantId}
                onVariantPick={setVariantId}
              />

              {/* Name validation banner — surfaces correction suggestions and
                  similar existing connectors from /api/v1/connectors/validate.
                  Click "Use" to accept a suggestion; "Open" to inspect an
                  existing one rather than generate a duplicate. */}
              {apiName.trim() && nameCheck && (nameCheck.suggestedName || nameCheck.warning || (nameCheck.similar && nameCheck.similar.length > 0) || (nameCheck.nearDuplicates && nameCheck.nearDuplicates.length > 0)) && (
                <div className={
                  // Red banner when backend says the name is outright invalid
                  // (test value, malformed). Amber for soft warnings (typos,
                  // similar-existing, no docs found). The Discover button is
                  // disabled in the red case so this is just a visual cue.
                  nameCheck.valid === false
                    ? "rounded-md border border-red-300 bg-red-50 px-3 py-2 text-xs text-red-900 dark:border-red-800 dark:bg-red-950/30 dark:text-red-200"
                    : "rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-200"
                }>
                  {nameCheck.suggestedName && (
                    <div className="flex items-center justify-between gap-2">
                      <span>
                        Did you mean <code className="font-mono bg-white/60 dark:bg-zinc-900/40 px-1 rounded">{nameCheck.suggestedName}</code>?
                      </span>
                      <button
                        type="button"
                        className="px-2 py-0.5 text-[11px] rounded bg-amber-900 text-white hover:bg-amber-800"
                        onClick={() => setApiName(nameCheck.suggestedName!)}
                      >
                        Use
                      </button>
                    </div>
                  )}
                  {nameCheck.warning && (
                    <div className="mt-1">{nameCheck.warning}</div>
                  )}
                  {/* Force-regenerate toggle — Phase 1 placement.
                      When backend reports the connector already exists, the user
                      needs to opt in to overwrite before Discover will unblock.
                      Without this checkbox in Phase 1 the warning text was just a
                      dead-end. */}
                  {nameCheck.alreadyExists && (
                    <label className="mt-2 flex items-center gap-2 cursor-pointer">
                      <input
                        data-testid="force-regenerate-toggle"
                        type="checkbox"
                        checked={forceRegenerate}
                        onChange={(e) => setForceRegenerate(e.target.checked)}
                        className="h-3.5 w-3.5 rounded border-amber-400 text-amber-600 focus:ring-amber-500"
                      />
                      <span>
                        <strong>Force regenerate</strong> — overwrite the existing connector.
                      </span>
                    </label>
                  )}
                  {nameCheck.similar && nameCheck.similar.length > 0 && (
                    <div className="mt-1">
                      Similar existing connectors:{" "}
                      {nameCheck.similar.slice(0, 4).map((s, i) => (
                        <span key={s}>
                          {i > 0 && ", "}
                          <code className="font-mono bg-white/60 dark:bg-zinc-900/40 px-1 rounded">{s}</code>
                        </span>
                      ))}
                    </div>
                  )}
                  {/* Near-duplicate advisory — a connector with the same vendor
                      root / display name already exists under a different id.
                      Non-blocking: this creates a SEPARATE connector. Worded to
                      avoid "already exists" so it never trips the force-regenerate
                      detection above (that regex-matches the `warning` text). */}
                  {nameCheck.nearDuplicates && nameCheck.nearDuplicates.length > 0 && (
                    <div className="mt-1">
                      This will create a <strong>separate</strong> connector. A similar one is already installed:{" "}
                      {nameCheck.nearDuplicates.slice(0, 4).map((s, i) => (
                        <span key={s}>
                          {i > 0 && ", "}
                          <code className="font-mono bg-white/60 dark:bg-zinc-900/40 px-1 rounded">{s}</code>
                        </span>
                      ))}
                      . To update it instead, regenerate that connector from the Connectors page.
                    </div>
                  )}
                </div>
              )}

              {/* Detected category chip — emitted by the validate effect's
                  parallel call to /connectors/detect-category. Surfaces what
                  the architect will use as api_category_hint (api_saas /
                  crm / ecommerce / etc.) and is also passed to the
                  generate request. */}
              {apiName.trim() && categoryDetect?.category_hint && (
                <div className="flex items-center gap-2 text-[11px] text-zinc-600 dark:text-zinc-400">
                  <span>Detected category:</span>
                  <span className="font-mono px-1.5 py-0.5 rounded bg-violet-100 text-violet-800 dark:bg-violet-950/40 dark:text-violet-200">
                    {categoryDetect.category_hint}
                  </span>
                  {typeof categoryDetect.confidence === "number" && (
                    <span className="text-zinc-400">
                      ({Math.round(categoryDetect.confidence * 100)}% · {categoryDetect.source || "auto"})
                    </span>
                  )}
                </div>
              )}

              {/* Connector name — editable slug, auto-derived from api_name+variant */}
              {apiName.trim() && (
                <div>
                  <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                    Connector name
                  </label>
                  <input
                    type="text"
                    value={connectorName}
                    onChange={(e) => {
                      setConnectorName(e.target.value)
                      setConnectorNameEdited(true)
                    }}
                    placeholder="shopify-admin-graphql"
                    className="w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500 font-mono"
                  />
                  <div className="text-[10px] text-zinc-500 mt-1">
                    The unique slug for this connector. Change it to register multiple variants
                    of the same API (e.g. <code>shopify-admin-graphql</code> vs <code>shopify-storefront</code>).
                  </div>
                </div>
              )}

              {/* Optional description — equivalent to the classic form's field. */}
              <div>
                <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                  Description <span className="text-zinc-400 font-normal">(optional)</span>
                </label>
                <input
                  type="text"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder={apiName ? `${apiName} connector` : "What this connector is for"}
                  className="w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                />
                <div className="text-[10px] text-zinc-500 mt-1">
                  Shown on the connector card. Defaults to <code>&lt;api_name&gt; connector</code>.
                </div>
              </div>

              {(!vendor || vendor.apis.length === 0) && (
                <>
                  <div>
                    <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                      Documentation URL <span className="text-zinc-400 font-normal">(optional)</span>
                    </label>
                    <input
                      type="text"
                      value={docsUrl}
                      onChange={(e) => setDocsUrl(e.target.value)}
                      placeholder="https://example.com/docs/api"
                      className="w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                    />
                    <div className="text-[10px] text-zinc-500 mt-1">
                      We&apos;ll detect the protocol and extract operations from this URL.
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                      API type
                    </label>
                    <select
                      value={protocolHint}
                      onChange={(e) => setProtocolHint(e.target.value)}
                      className="w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                    >
                      <option value="auto">Auto-detect (recommended)</option>
                      <option value="rest">REST</option>
                      <option value="graphql">GraphQL</option>
                      <option value="openapi">OpenAPI / Swagger</option>
                    </select>
                    <div className="text-[10px] text-zinc-500 mt-1 leading-snug">
                      <strong>OpenAPI/Swagger</strong>: REST API with a machine-readable spec
                      (best for auto-discovery).{" "}
                      <strong>REST</strong>: HTTP/JSON without a formal spec.{" "}
                      <strong>GraphQL</strong>: single endpoint with introspection.
                    </div>
                  </div>
                  <div className="rounded-md border border-zinc-200 dark:border-zinc-800">
                    <button
                      type="button"
                      onClick={() => setShowSpecFields((v) => !v)}
                      className="w-full flex items-center justify-between cursor-pointer px-3 py-2 text-xs font-medium text-zinc-700 dark:text-zinc-300 select-none"
                    >
                      <span>Advanced — paste a spec, GraphQL endpoint, or a curl example</span>
                      <span className="text-zinc-400">{showSpecFields ? "−" : "+"}</span>
                    </button>
                    {showSpecFields && (
                    <div className="px-3 pb-3 pt-1 space-y-4">
                  <div>
                    <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                      OpenAPI / Swagger spec <span className="text-zinc-400 font-normal">(optional — most accurate)</span>
                    </label>
                    <textarea
                      value={openApiSpec}
                      onChange={(e) => setOpenApiSpec(e.target.value)}
                      placeholder={'Paste your OpenAPI/Swagger spec (JSON or YAML)…\n{"openapi":"3.0.0","info":{...},"paths":{...}}'}
                      rows={6}
                      className="w-full text-[11px] font-mono px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                    />
                    <div className="text-[10px] text-zinc-500 mt-1">
                      Deterministic: resources come straight from your spec (no AI guessing). Overrides the docs URL.
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                      GraphQL endpoint <span className="text-zinc-400 font-normal">(optional)</span>
                    </label>
                    <input
                      type="text"
                      value={graphqlEndpoint}
                      onChange={(e) => setGraphqlEndpoint(e.target.value)}
                      placeholder="https://api.example.com/graphql"
                      className="w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                    />
                    <div className="text-[10px] text-zinc-500 mt-1">
                      We&apos;ll introspect the schema to discover queries deterministically (needs a public/no-auth endpoint).
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                      REST base URL <span className="text-zinc-400 font-normal">(optional)</span>
                    </label>
                    <input
                      type="text"
                      value={baseUrl}
                      onChange={(e) => setBaseUrl(e.target.value)}
                      placeholder="https://api.github.com"
                      className="w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                    />
                    <div className="text-[10px] text-zinc-500 mt-1">
                      The API host requests target. Also the endpoint when you paste a GraphQL introspection schema below.
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                      GraphQL introspection schema <span className="text-zinc-400 font-normal">(optional)</span>
                    </label>
                    <textarea
                      value={graphqlSchema}
                      onChange={(e) => setGraphqlSchema(e.target.value)}
                      placeholder={'Paste the GraphQL introspection JSON (result of the standard __schema query)…\n{"data":{"__schema":{...}}}'}
                      rows={6}
                      className="w-full text-[11px] font-mono px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                    />
                    <div className="text-[10px] text-zinc-500 mt-1">
                      Deterministic: types come straight from the schema (no live endpoint or auth needed). Set the REST base URL above to your GraphQL endpoint.
                    </div>
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1">
                      Paste a curl from the docs <span className="text-zinc-400 font-normal">(optional)</span>
                    </label>
                    <textarea
                      value={curlExample}
                      onChange={(e) => setCurlExample(e.target.value)}
                      placeholder={'curl -H "Authorization: Bearer YOUR_TOKEN" https://api.example.com/v1/items'}
                      rows={3}
                      className="w-full text-[11px] font-mono px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
                    />
                    <div className="text-[10px] text-zinc-500 mt-1">
                      No spec? Paste any example request from the API docs — we extract the endpoint, base URL, and auth with zero guessing.
                    </div>
                  </div>
                    </div>
                    )}
                  </div>
                </>
              )}
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-[11px] text-zinc-500 dark:text-zinc-400">
              Don&apos;t see your API? Just type the name — we&apos;ll discover it from the docs URL.
            </div>
            <Button
              onClick={startFlow}
              disabled={
                discovering ||
                !apiName.trim() ||
                // Backend validate flagged the name as invalid (test value,
                // malformed slug, etc.). Block discovery — the warning
                // banner above tells the user what's wrong.
                nameCheck?.valid === false ||
                // Backend says can_generate=false. The most common cause is
                // "connector already exists" — in that case we let the user
                // proceed iff they've ticked Force regenerate. Any other
                // canGenerate=false reason still blocks.
                (nameCheck?.canGenerate === false && !(nameCheck?.alreadyExists && forceRegenerate))
              }
              className="bg-gradient-to-r from-violet-600 to-indigo-600 hover:from-violet-700 hover:to-indigo-700 text-white shadow-sm"
            >
              {discovering ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4 mr-2" />
              )}
              Discover
            </Button>
          </div>
          {error && (
            <div className="rounded-md border border-red-200 dark:border-red-900/40 bg-red-50 dark:bg-red-950/30 px-3 py-2 text-xs text-red-700 dark:text-red-300">
              {error}
            </div>
          )}
        </div>
      ) : (
        // ── Phase 2: Contract preview + confirm ───────────────────────
        <div className="space-y-3">
          <ContractPreviewCard
            contract={contract}
            pendingAnswers={pendingAnswers}
            onAnswerChange={(dim, value) =>
              setPendingAnswers((prev) => ({ ...prev, [dim]: value }))
            }
          />

          {/* Connector-exists warning — shown either when the post-discover
              fetchMCPConnector says it exists, OR when the pre-discover
              validate endpoint already flagged alreadyExists. Without the
              pre-discover branch the user saw the backend's "already exists"
              warning text but no checkbox to act on it. */}
          {(connectorExists === true || nameCheck?.alreadyExists === true) && (
            <div className="rounded-md border border-amber-300 dark:border-amber-700 bg-amber-50 dark:bg-amber-950/30 px-3 py-2.5 flex items-start gap-2.5">
              <AlertTriangle className="h-4 w-4 text-amber-600 dark:text-amber-400 shrink-0 mt-0.5" />
              <div className="flex-1 space-y-2">
                <div className="text-xs font-medium text-amber-900 dark:text-amber-200">
                  A connector named <code className="font-mono">{connectorName}</code> already exists.
                </div>
                <label className="flex items-center gap-2 text-xs text-amber-800 dark:text-amber-300 cursor-pointer">
                  <input
                    data-testid="force-regenerate-toggle"
                    type="checkbox"
                    checked={forceRegenerate}
                    onChange={(e) => setForceRegenerate(e.target.checked)}
                    className="h-3.5 w-3.5 rounded border-amber-400 text-amber-600 focus:ring-amber-500"
                  />
                  <span>
                    <strong>Force regenerate</strong> — overwrite the existing connector.
                  </span>
                </label>
              </div>
            </div>
          )}

          {/* Advanced options — only shown when connector doesn't exist yet */}
          {connectorExists !== true && (
            <details
              open={showAdvanced}
              onToggle={(e) => setShowAdvanced((e.target as HTMLDetailsElement).open)}
              className="rounded-md border border-zinc-200 dark:border-zinc-800"
            >
              <summary className="cursor-pointer px-3 py-2 text-xs font-medium text-zinc-700 dark:text-zinc-300">
                Advanced options
              </summary>
              <div className="px-3 pb-3 pt-1">
                <label className="flex items-center gap-2 text-xs text-zinc-700 dark:text-zinc-300 cursor-pointer">
                  <input
                    data-testid="force-regenerate-toggle"
                    type="checkbox"
                    checked={forceRegenerate}
                    onChange={(e) => setForceRegenerate(e.target.checked)}
                    className="h-3.5 w-3.5 rounded border-zinc-300 text-violet-600 focus:ring-violet-500"
                  />
                  <span>
                    <strong>Force regenerate</strong> — overwrite the connector if it already exists.
                    Use this after fixing a vendor entry or upgrading a stale connector.
                  </span>
                </label>
              </div>
            </details>
          )}

          <div className="flex flex-wrap items-center gap-2">
            {hasPendingAnswers && (
              <Button onClick={submitAnswers} disabled={confirming}>
                {confirming ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4 mr-2" />
                )}
                Apply answers
              </Button>
            )}
            <Button
              onClick={handleGenerate}
              disabled={!contract.can_generate || generating}
              variant={contract.can_generate ? "default" : "outline"}
            >
              {generating ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Play className="h-4 w-4 mr-2" />
              )}
              Generate connector
            </Button>
            <Button onClick={reset} variant="ghost" size="sm">
              Start over
            </Button>
            {error && <span className="text-xs text-red-600">{error}</span>}
          </div>

        </div>
      )}
      </div>

      {/* ── Right column: live generation result panel ─────────────── */}
      <ResultPanel
        contract={contract}
        generating={generating}
        result={generationResult}
      />
    </div>
  )
}

function ResultPanel({
  contract,
  generating,
  result,
}: {
  contract: GenerationContract | null
  generating: boolean
  result: GenerateFromSessionResponse | null
}) {
  return (
    <aside
      className="rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shadow-sm lg:sticky lg:top-6"
      data-testid="generation-panel"
    >
      <div className="px-5 pt-5 pb-3 border-b border-zinc-100 dark:border-zinc-800">
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">
          Generation result
        </h2>
        <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-0.5">
          Status, contract, and generated artifacts.
        </p>
      </div>
      <div className="px-5 py-5">
        {generating ? (
          <div className="flex flex-col items-center justify-center py-8 text-center">
            <Loader2 className="h-8 w-8 text-violet-500 animate-spin mb-3" />
            <div className="text-sm font-medium text-zinc-800 dark:text-zinc-200">
              Generating connector…
            </div>
            <div className="text-[11px] text-zinc-500 mt-1">
              Validating contract, rendering templates, writing artifacts.
            </div>
          </div>
        ) : result?.success ? (
          <div data-testid="generation-success" className="space-y-3">
            <div className="rounded-lg border border-emerald-300 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-950/30 p-3">
              <div className="text-sm font-semibold text-emerald-900 dark:text-emerald-200">
                ✓ Generated{" "}
                <code className="font-mono">{result.connector_name}</code>
              </div>
              <div className="text-[11px] text-emerald-700 dark:text-emerald-300 mt-1 leading-relaxed">
                {result.protocol ? (
                  <>protocol: <strong>{result.protocol}</strong><br /></>
                ) : null}
                {result.operation_count != null ? (
                  <>operations: <strong>{result.operation_count}</strong><br /></>
                ) : null}
                {result.quality_tier ? (
                  <>quality: <strong>{result.quality_tier}</strong><br /></>
                ) : null}
                {result.total_time_ms != null ? (
                  <>elapsed: <strong>{Math.round(result.total_time_ms)}ms</strong></>
                ) : null}
              </div>
            </div>
            <Button
              size="sm"
              className="w-full bg-gradient-to-r from-violet-600 to-indigo-600 text-white"
              onClick={() => (window.location.href = "/connectors")}
            >
              View connector
            </Button>
          </div>
        ) : contract ? (
          <div className="space-y-3 text-xs">
            <div>
              <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-0.5">
                API
              </div>
              <div className="font-mono text-zinc-900 dark:text-zinc-100 break-all">
                {contract.api_name}
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <Stat label="Protocol" value={factString(contract, "protocol")} />
              <Stat label="Auth" value={factString(contract, "auth_type")} />
            </div>
            <Stat label="Operations" value={operationsCount(contract)} />
            <div className="rounded-md border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40 px-3 py-2 text-[11px] text-zinc-600 dark:text-zinc-400">
              {contract.can_generate
                ? "Contract is complete — click Generate connector."
                : "Answer the open questions on the left, then click Apply answers. If discovery couldn't resolve the API host, set it under “Advanced” → REST base URL on the left (e.g. https://api.github.com)."}
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-10 text-center">
            <div className="rounded-full bg-violet-50 dark:bg-violet-950/40 p-3 mb-3">
              <Sparkles className="h-6 w-6 text-violet-500" />
            </div>
            <div className="text-sm font-medium text-zinc-800 dark:text-zinc-200">
              Nothing yet
            </div>
            <div className="text-[11px] text-zinc-500 mt-1 max-w-[14rem]">
              Pick a vendor and hit <strong>Discover</strong>. The contract and
              generated connector will appear here.
            </div>
          </div>
        )}
      </div>
    </aside>
  )
}

function factString(contract: GenerationContract, dim: Dimension): string {
  const v = contract.facts?.[dim]?.value
  if (v == null) return "—"
  if (typeof v === "string") return v || "—"
  if (typeof v === "number" || typeof v === "boolean") return String(v)
  return "—"
}

function operationsCount(contract: GenerationContract): string {
  const v = contract.facts?.operations?.value
  if (Array.isArray(v)) return `${v.length}`
  return "—"
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-0.5">
        {label}
      </div>
      <div className="font-mono text-zinc-900 dark:text-zinc-100 truncate">
        {value}
      </div>
    </div>
  )
}
