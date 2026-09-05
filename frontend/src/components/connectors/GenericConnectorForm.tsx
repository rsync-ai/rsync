"use client"

import { useState, useEffect, useRef } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Loader2, Eye, EyeOff, CheckCircle2, AlertCircle, Zap, Clock, Info, Database, Radio, Container, Play, ShieldCheck } from "lucide-react"
import { MCPConnector, ConfigProperty, splitMethodCredentialKeys } from "@/lib/types/mcp-connector"
import { testMCPConnection } from "@/lib/api/mcp-connectors"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import { APIRequestError, parseAPIError, ErrorCodes } from "@/lib/errors/api-errors"
import { OAuthConnectButton } from "@/components/oauth/OAuthConnectButton"
import { AuthMethodPicker } from "@/components/connectors/AuthMethodPicker"
import { toast } from "sonner"

type SyncMode = "batch" | "cdc"
type CDCMode = "initial" | "streaming_only"

type UseCase = "realtime" | "analytics" | "backup" | ""
type FreshnessRequirement = "up_to_minute" | "hourly" | "daily" | ""

type OAuthProviderStatus = {
  name: string
  enabled: boolean
  message?: string
}

// Status of the caller's "bring your own" OAuth app for a provider, from
// GET /api/v1/oauth/apps/:provider. Drives the BYO credential form shown for
// providers that have no operator-set env app (e.g. generated connectors).
type BYOAppStatus = {
  provider: string
  known: boolean
  configured: boolean
  operator_managed?: boolean
  grant_type?: string
  redirect_uri?: string
  default_scopes?: string
  client_id?: string
  scopes?: string
}

/**
 * Parse and improve database connection error messages
 * Makes MySQL/PostgreSQL errors more user-friendly
 */
function improveErrorMessage(error: string, connectorType: string): string {
  // API Gateway sometimes wraps structured validation errors inside a string:
  // "Test service returned error (HTTP 400): { ...json... }"
  // Unwrap and show a readable bullet list.
  const tryUnwrapValidationJson = (raw: string): string | null => {
    const start = raw.indexOf("{")
    const end = raw.lastIndexOf("}")
    if (start === -1 || end === -1 || end <= start) return null
    const jsonStr = raw.slice(start, end + 1)
    try {
      const parsed: any = JSON.parse(jsonStr)
      const base = parsed?.message || parsed?.error || "Configuration validation failed"
      const ve: any[] = Array.isArray(parsed?.validation_errors) ? parsed.validation_errors : []
      if (!ve.length) return String(base)
      const bullets = ve
        .map((e) => {
          const field = e?.field ? String(e.field) : "field"
          const msg = e?.message ? String(e.message) : "is invalid"
          return `• ${field}: ${msg}`
        })
        .join("\n")
      return `${base}\n${bullets}`
    } catch {
      return null
    }
  }

  const unwrapped = tryUnwrapValidationJson(error)
  if (unwrapped) return unwrapped

  // MySQL Access Denied - explain the client IP confusion
  if (error.includes("Access denied for user") && error.includes("@")) {
    const match = error.match(/Access denied for user '([^']+)'@'([^']+)'/)
    if (match) {
      const [, user, clientHost] = match
      return `Authentication failed for user '${user}'. Please verify:\n` +
        `• Password is correct\n` +
        `• User '${user}' exists on the database server\n` +
        `• User has permission to connect from your network\n\n` +
        `(Connection attempted from: ${clientHost})`
    }
  }
  
  // MySQL Unknown host
  if (error.includes("Unknown MySQL server host") || error.includes("Unknown server host")) {
    const match = error.match(/host '([^']+)'/)
    const host = match ? match[1] : "specified host"
    return `Cannot reach MySQL server at '${host}'. Please verify:\n` +
      `• Hostname/IP address is correct\n` +
      `• Server is running and accessible\n` +
      `• Firewall allows connections on the specified port`
  }
  
  // Connection timeout
  if (error.includes("Connection timed out") || error.includes("timeout")) {
    return `Connection timed out. Please verify:\n` +
      `• Server hostname/IP is correct\n` +
      `• Port number is correct\n` +
      `• Firewall/Security Groups allow inbound connections\n` +
      `• Database server is running`
  }
  
  // Connection refused
  if (error.includes("Connection refused") || error.includes("ECONNREFUSED")) {
    return `Connection refused by server. Please verify:\n` +
      `• Database server is running\n` +
      `• Port number is correct\n` +
      `• Server is configured to accept remote connections`
  }

  // Draft connector blocked — should not normally reach the UI since test calls pass allow_draft=true,
  // but surface a clear message if it does (e.g. stale client).
  if (error.includes("draft_connector_blocked")) {
    return `This connector hasn't been used in a real pipeline yet. Click "Start & Test Connection" to validate it — a successful test will unlock pipeline creation.`
  }

  return error
}

interface InitialData {
  connectionName?: string
  connectionType?: "source" | "destination"
  syncMode?: SyncMode
  cdcMode?: CDCMode
  description?: string
  config?: Record<string, unknown>
}

interface GenericConnectorFormProps {
  connector: MCPConnector
  onSave: (config: Record<string, unknown>) => void
  onCancel: () => void
  initialData?: InitialData
  isEditing?: boolean
  connectionId?: string  // For testing existing connections with stored credentials
}

// A schema/config key names a credential when it contains any of these
// substrings. SINGLE source of truth, used both to decide whether a field must
// be masked (renderField) AND whether the schema already provides a credential
// input (so the synthesized auth fallback is suppressed). Keeping one list
// prevents the two from drifting — that drift caused the aws-s3 regression where
// access_key_id / secret_access_key were masked by renderField but missed by an
// exact-match credential Set, so a bogus "API Key" field was synthesized on top
// of the real fields and gated Save. Substrings cover non-standard keys too
// (access_key_id, secret_access_key, service_account_credentials, pat_token, …).
const CREDENTIAL_FIELD_SUBSTRINGS = [
  "password", "passwd", "secret", "api_key", "apikey", "token",
  "access_key", "secret_key", "credential", "private_key",
]
function isCredentialFieldName(key: string): boolean {
  const k = key.toLowerCase()
  return CREDENTIAL_FIELD_SUBSTRINGS.some((s) => k.includes(s))
}

// ---------------------------------------------------------------------------
// computeAuthUI — the SINGLE auth-rendering decision tree (plan §3).
//
// Replaces four overlapping hand-tuned gates (oauth_provider · multi-auth ·
// schema-credential · auth_type fallback) with one pure selector. Given the
// connector metadata and the currently-selected auth method, it returns which
// ONE primary auth block the form renders. Precedence (highest first):
//
//   1. supported_auth_methods present  → AuthMethodPicker (authoritative). When
//      the selected method is oauth2 AND a provider resolves, ALSO show the
//      OAuth Connect block; if no provider resolves, the picker surfaces
//      "OAuth not configured" (never a paste-token).
//   2. oauth_provider set, no methods  → OAuth Connect (+ BYO) only.
//   3. auth_type needs a credential AND the schema already declares one
//      (substring match) → the schema renders it; NO synthesis (the #290/#291
//      net: aws-s3 / github-rest-with-schema).
//   4. auth_type needs a credential, nothing above → synthesized fallback.
//   5. otherwise (none/empty)          → no auth UI.
//
// Pure and exported so the §6 test matrix can assert one case per auth shape.
// The provider the OAuth Connect button binds to is method.oauth_provider ??
// connector.oauth_provider (the pipedrive-gap per-method link wins).
export type AuthUIKind = "picker" | "oauth" | "schema" | "fallback" | "none"

export interface AuthUIDecision {
  kind: AuthUIKind
  // Render the standalone OAuth Connect section (provider status, BYO, button).
  showOAuthConnect: boolean
  // Provider id the Connect button binds to (non-null iff showOAuthConnect).
  oauthProvider: string | null
  // Picker + oauth2 selected but NO provider resolves: the method is unusable.
  // The picker shows "OAuth not configured"; Test/Save stay gated.
  oauthUnconfigured: boolean
  // The active path is OAuth → Test/Save require a completed authorization.
  requiresOAuthConnected: boolean
}

function isOauthAuthType(t?: string): boolean {
  const v = (t || "").toLowerCase()
  return v === "oauth2" || v === "oauth"
}

export function computeAuthUI(
  connector: Pick<
    MCPConnector,
    "auth_type" | "oauth_provider" | "supported_auth_methods" | "configuration_schema"
  >,
  authMethod: string,
): AuthUIDecision {
  const methods = connector.supported_auth_methods || []
  const connectorProvider = connector.oauth_provider || null

  // 1. Multi-auth picker is authoritative whenever any method is declared.
  if (methods.length > 0) {
    const active = methods.find((m) => m.method === authMethod) || methods[0]
    const activeIsOauth = isOauthAuthType(active?.method)
    const resolved = (active?.oauth_provider || connectorProvider) || null
    const showOAuthConnect = activeIsOauth && !!resolved
    return {
      kind: "picker",
      showOAuthConnect,
      oauthProvider: showOAuthConnect ? resolved : null,
      oauthUnconfigured: activeIsOauth && !resolved,
      requiresOAuthConnected: showOAuthConnect,
    }
  }

  // 2. Single-scheme OAuth: provider set, no methods.
  if (connectorProvider) {
    return {
      kind: "oauth",
      showOAuthConnect: true,
      oauthProvider: connectorProvider,
      oauthUnconfigured: false,
      requiresOAuthConnected: true,
    }
  }

  // 3-5. No methods, no provider — schema / synthesized fallback / none.
  const noAuth: AuthUIDecision = {
    kind: "none",
    showOAuthConnect: false,
    oauthProvider: null,
    oauthUnconfigured: false,
    requiresOAuthConnected: false,
  }
  const t = (connector.auth_type || "").toLowerCase()
  const needsCredential = t !== "" && t !== "none" && !isOauthAuthType(t)
  if (!needsCredential) return noAuth

  const props = connector.configuration_schema?.properties || {}
  const schemaHasCredentialField = Object.keys(props).some(isCredentialFieldName)
  return { ...noAuth, kind: schemaHasCredentialField ? "schema" : "fallback" }
}

export function GenericConnectorForm({
  connector,
  onSave,
  onCancel,
  initialData,
  isEditing = false,
  connectionId,
}: GenericConnectorFormProps) {
  // Determine available connection types based on capabilities (calculate first!)
  // If both are false/undefined, show both options as default (for API connectors)
  const availableTypes: Array<"source" | "destination"> = []
  
  if (connector.supports_source) availableTypes.push("source")
  if (connector.supports_destination) availableTypes.push("destination")
  
  // Fallback: if no types defined, show both options (common for API connectors)
  if (availableTypes.length === 0) {
    availableTypes.push("source", "destination")
  }

  // Initialize connectionType with the FIRST available type (generic solution)
  const defaultConnectionType = initialData?.connectionType || availableTypes[0] || "source"

  const [connectionName, setConnectionName] = useState(initialData?.connectionName || "")
  const [connectionType, setConnectionType] = useState<"source" | "destination">(defaultConnectionType)
  const [syncMode, setSyncMode] = useState<SyncMode>(initialData?.syncMode || "batch")
  const [cdcMode, setCdcMode] = useState<CDCMode>(initialData?.cdcMode || "initial")
  const [description, setDescription] = useState(initialData?.description || "")
  
  // HITL questionnaire state
  const [useCase, setUseCase] = useState<UseCase>("")
  const [freshnessRequirement, setFreshnessRequirement] = useState<FreshnessRequirement>("")
  const [showQuestions, setShowQuestions] = useState(false)
  const [recommendedMode, setRecommendedMode] = useState<SyncMode | null>(null)
  const [formData, setFormData] = useState<Record<string, unknown>>(() => {
    // If editing, use the existing config
    if (initialData?.config) {
      return { ...initialData.config }
    }
    // Otherwise initialize with defaults from schema
    const defaults: Record<string, unknown> = {}
    const props = connector.configuration_schema?.properties || {}
    Object.entries(props).forEach(([key, prop]) => {
      if (prop.default !== undefined) {
        defaults[key] = prop.default
      }
    })
    return defaults
  })

  // Phase 13b — multi-auth dropdown state. Active when the connector
  // metadata declares supported_auth_methods. The picker maintains its own
  // map of credential values keyed by config_key (e.g. access_token,
  // oauth_token) so switching methods doesn't lose previously-typed creds.
  const supportedAuthMethods = connector.supported_auth_methods || []
  const hasMultiAuth = supportedAuthMethods.length > 0
  const [authMethod, setAuthMethod] = useState<string>(() => {
    if (initialData?.config?.auth_method) return String(initialData.config.auth_method)
    return supportedAuthMethods[0]?.method || ""
  })
  const [authValues, setAuthValues] = useState<Record<string, string>>(() => {
    if (!initialData?.config) return {}
    const out: Record<string, string> = {}
    for (const m of supportedAuthMethods) {
      for (const k of m.config_keys) {
        const v = initialData.config[k]
        if (v != null) out[k] = String(v)
      }
    }
    return out
  })

  const [showSecrets, setShowSecrets] = useState<Record<string, boolean>>({})
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<{
    success: boolean
    message: string
  } | null>(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<{
    message: string
    suggestion?: string
    field?: string
    code?: string
  } | null>(null)
  
  // OAuth state
  const [oauthTokenId, setOauthTokenId] = useState<string | null>(null)
  const [oauthConnected, setOauthConnected] = useState(false)
  const [oauthProviderStatus, setOauthProviderStatus] = useState<OAuthProviderStatus | null>(null)
  // BYO OAuth app state (per-user client_id/secret for un-provisioned providers)
  const [byoApp, setByoApp] = useState<BYOAppStatus | null>(null)
  const [byoClientId, setByoClientId] = useState("")
  const [byoClientSecret, setByoClientSecret] = useState("")
  const [byoScopes, setByoScopes] = useState("")
  const [byoSaving, setByoSaving] = useState(false)
  const [showByoForm, setShowByoForm] = useState(false)
  
  // Docker state tracking
  const [dockerStarting, setDockerStarting] = useState(false)

  const schema = connector.configuration_schema
  const requiredFields = schema?.required || []
  const properties = schema?.properties || {}
  // Lowercased schema property keys — lets the auth picker + completeness gate
  // tell a method's DISTINCT credential fields from ALIAS key-names (see
  // splitMethodCredentialKeys; fixes aws-s3's dropped secret_access_key).
  const schemaKeys = new Set(Object.keys(properties).map((k) => k.toLowerCase()))

  // Auth fallback. Some generated connectors declare an auth_type that needs a
  // credential but emit NO way to enter it — no oauth_provider, no
  // supported_auth_methods, and no credential key in configuration_schema
  // (real example: github-rest → auth_type "bearer", empty schema). Without
  // this the modal renders zero auth UI and the connector is unusable.
  // Synthesize a credential field from auth_type so every connector is authable
  // regardless of metadata gaps; suppressed whenever the metadata already
  // provides an auth path (OAuth, multi-auth picker, or a schema credential).
  const authFallback = (() => {
    const t = (connector.auth_type || "").toLowerCase()
    if (t === "basic") {
      return {
        noun: "username and password",
        fields: [
          { key: "username", label: "Username", secret: false, placeholder: "Username" },
          { key: "password", label: "Password", secret: true, placeholder: "Password" },
        ],
      }
    }
    if (t === "api_key" || t === "apikey" || t === "api_key_query") {
      return {
        noun: "API key",
        fields: [{ key: "api_key", label: "API Key", secret: true, placeholder: "Your API key" }],
      }
    }
    if (t === "custom_header") {
      // A custom-header scheme with no method/header metadata to name the header
      // (else it'd be a supported_auth_method). Collect the token generically;
      // the generated connector knows which header to put it in.
      return {
        noun: "API token",
        fields: [{ key: "access_token", label: "API Token", secret: true, placeholder: "Your API token (sent in the connector's auth header)" }],
      }
    }
    // bearer / token / any other credential-based scheme
    return {
      noun: "access token",
      fields: [{ key: "access_token", label: "Access Token", secret: true, placeholder: "Your access token / API token" }],
    }
  })()
  // The single auth-rendering decision (plan §3). Every auth block below and the
  // Test/Save completeness gate derive from this — no more ad-hoc per-signal
  // booleans. `kind === "fallback"` is exactly the former `showAuthFallback`.
  const authUI = computeAuthUI(connector, authMethod)

  // Is the active auth path missing its credential? Drives Test/Save disabled.
  // - fallback   → the synthesized field(s) must be filled
  // - oauth path → a completed authorization is required (fixes the old bypass
  //                where oauth_provider + auth_type!=oauth skipped this gate)
  // - picker     → (create only) the chosen method's credential must be filled,
  //                and an oauth2 method with no resolvable provider is unusable.
  //                Edit mode relies on handleSave's own validation since stored
  //                credentials aren't re-sent to the form.
  const authIncomplete = (() => {
    if (authUI.kind === "fallback") {
      return authFallback.fields.some((f) => !String(formData[f.key] ?? "").trim())
    }
    if (authUI.requiresOAuthConnected) {
      return !oauthConnected
    }
    // An oauth2 method with no resolvable provider can never be completed (no
    // Connect button, no paste-token) — block in BOTH create and edit mode so
    // broken/generated metadata fails safe rather than persisting an
    // unauthenticated connection. (No shipping connector hits this; it guards
    // future connectors whose oauth2 method ships without a registered provider.)
    if (authUI.oauthUnconfigured) {
      return true
    }
    if (authUI.kind === "picker" && !isEditing) {
      // Reachable only for a NON-oauth method (oauth2 was handled above), so
      // require the chosen method's credential. Edit mode is skipped because
      // stored credentials aren't re-sent to the form.
      const active =
        supportedAuthMethods.find((m) => m.method === authMethod) || supportedAuthMethods[0]
      if (!active) return false
      // Require EVERY distinct credential field the picker renders — not just the
      // first key. (aws-s3 api_key has two distinct secrets; the old slice(0,1)
      // let Save/Test pass with secret_access_key empty → a broken connection.)
      const { fields } = splitMethodCredentialKeys(active, schemaKeys)
      return fields.some((k) => !String(authValues[k] ?? "").trim())
    }
    return false
  })()

  // Keep a ref to the latest formData so async handlers
  // don't accidentally operate on stale values.
  const formDataRef = useRef<Record<string, unknown>>(formData)
  useEffect(() => {
    formDataRef.current = formData
  }, [formData])

  // Component mount logging (minimal) - disabled in production
  useEffect(() => {
    if (process.env.NODE_ENV === 'development') {
      console.log(`[GenericConnectorForm] Initialized for ${connector.name} - Available types: ${availableTypes.join(', ')}`)
    }
  }, [])
  
  // Show questionnaire when connection type is set to source and connector supports CDC
  useEffect(() => {
    if (connectionType === "source" && connector.supports_cdc && !initialData?.syncMode) {
      // Keep the form compact by default; user can expand “Help me choose”.
      setShowQuestions(false)
    }
  }, [connectionType, connector.supports_cdc])

  // Keep CDC defaults sane without forcing additional “extra” screens in the modal.
  useEffect(() => {
    if (syncMode !== "cdc") return
    if (!cdcMode) setCdcMode("initial")
  }, [syncMode, cdcMode])
  
  // HITL questionnaire logic: recommend mode based on answers
  useEffect(() => {
    if (!useCase) {
      setRecommendedMode(null)
      return
    }
    
    // Q1: Use case determines initial recommendation
    if (useCase === "realtime") {
      setRecommendedMode("cdc")
      setSyncMode("cdc")
      return
    }
    
    if (useCase === "backup") {
      setRecommendedMode("batch")
      setSyncMode("batch")
      return
    }
    
    // Q2: For analytics, check freshness requirement
    if (useCase === "analytics") {
      if (!freshnessRequirement) {
        setRecommendedMode(null)
        return
      }
      
      if (freshnessRequirement === "up_to_minute") {
        setRecommendedMode("cdc")
        setSyncMode("cdc")
      } else {
        setRecommendedMode("batch")
        setSyncMode("batch")
      }
    }
  }, [useCase, freshnessRequirement])

  // If connector advertises an OAuth provider, check whether it's actually enabled in the API Gateway.
  // This prevents the UX where users click "Connect" and get a scary error when env vars aren't set.
  useEffect(() => {
    let cancelled = false

    async function loadOAuthProviderStatus() {
      if (!connector.oauth_provider) {
        setOauthProviderStatus(null)
        return
      }

      // Disable the connect button immediately while we check status (prevents fast-click → error state)
      setOauthProviderStatus({
        name: connector.oauth_provider,
        enabled: false,
        message: "Checking OAuth availability...",
      })

      try {
        const res = await authFetch(API_ENDPOINTS.OAUTH.PROVIDERS, { cache: "no-store" })

        if (!res.ok) {
          // If the endpoint isn't available or errors, fall back to "unknown" state (allow manual config)
          if (!cancelled) setOauthProviderStatus({ name: connector.oauth_provider, enabled: false, message: "OAuth providers endpoint unavailable" })
          return
        }

        const data = await res.json()
        const providers: OAuthProviderStatus[] = (data?.providers || []).map((p: any) => ({
          name: p?.name,
          enabled: !!p?.enabled,
          message: p?.message,
        }))

        const match = providers.find((p) => p.name === connector.oauth_provider)
        if (!cancelled) {
          setOauthProviderStatus(
            match || {
              name: connector.oauth_provider,
              enabled: false,
              message: `OAuth provider '${connector.oauth_provider}' not configured`,
            }
          )
        }
      } catch {
        if (!cancelled) setOauthProviderStatus({ name: connector.oauth_provider, enabled: false, message: "Failed to check OAuth provider status" })
      }
    }

    loadOAuthProviderStatus()
    return () => {
      cancelled = true
    }
  }, [connector.oauth_provider])

  // Load the caller's BYO OAuth-app status. Tells us whether this provider is
  // operator-managed (env app — no BYO needed), known-but-unconfigured (show the
  // BYO form), or already configured (BYO app present → connectable).
  useEffect(() => {
    let cancelled = false

    async function loadByoApp() {
      if (!connector.oauth_provider) {
        setByoApp(null)
        return
      }
      try {
        const res = await authFetch(API_ENDPOINTS.OAUTH.APP(connector.oauth_provider), { cache: "no-store" })
        if (!res.ok) {
          if (!cancelled) setByoApp(null)
          return
        }
        const data: BYOAppStatus = await res.json()
        if (cancelled) return
        setByoApp(data)
        if (data?.client_id) setByoClientId(data.client_id)
        if (data?.scopes) setByoScopes(data.scopes)
        else if (data?.default_scopes) setByoScopes(data.default_scopes)
        // Auto-open the form when BYO is required (known, not operator-managed, not yet configured).
        if (data?.known && !data?.operator_managed && !data?.configured) setShowByoForm(true)
      } catch {
        if (!cancelled) setByoApp(null)
      }
    }

    loadByoApp()
    return () => {
      cancelled = true
    }
  }, [connector.oauth_provider])

  // Reflect an already-issued OAuth token when EDITING an existing connection,
  // so a reopened modal shows "Authentication successful" and keeps Test/Save
  // enabled (they gate on oauthConnected). Deliberately scoped to edit mode: in
  // CREATE mode oauthConnected is set ONLY by a real OAuth completion
  // (handleOAuthSuccess). Auto-detecting a provider token during creation caused
  // a false-positive banner — a stale token left over from a prior flow made an
  // unauthenticated new connection look connected (PR #288 regression).
  useEffect(() => {
    if (oauthConnected) return
    if (!connector.oauth_provider || !isEditing || !connectionId) return

    // Prefer the connection's OWN stored token id when the saved config carries
    // it — exact, local, and free of cross-connection token bleed.
    const storedTokenId = initialData?.config?.oauth_token_id
    if (typeof storedTokenId === "string" && storedTokenId) {
      setOauthTokenId(storedTokenId)
      setOauthConnected(true)
      return
    }

    // Otherwise fall back to the most recent live token for this provider.
    let cancelled = false
    async function detectExistingToken() {
      try {
        const res = await authFetch(API_ENDPOINTS.OAUTH.TOKENS, { cache: "no-store" })
        if (!res.ok) return
        const data = await res.json()
        const tokens: Array<{ id?: string; provider?: string; expires_at?: string; created_at?: string }> =
          Array.isArray(data) ? data : (data?.tokens ?? data?.data ?? [])
        const now = Date.now()
        const match = tokens
          .filter((t) => t?.id && t.provider === connector.oauth_provider)
          .filter((t) => !t.expires_at || new Date(t.expires_at).getTime() > now)
          .sort((a, b) => new Date(b.created_at ?? 0).getTime() - new Date(a.created_at ?? 0).getTime())[0]
        if (!cancelled && match?.id) {
          setOauthTokenId(match.id)
          setOauthConnected(true)
        }
      } catch {
        // Best-effort: a missing/failed token lookup just leaves the normal
        // "Connect" flow in place; never block the form on it.
      }
    }
    detectExistingToken()
    return () => {
      cancelled = true
    }
  }, [connector.oauth_provider, oauthConnected, isEditing, connectionId, initialData])

  const handleInputChange = (key: string, value: unknown) => {
    setFormData((prev) => {
      const next: Record<string, unknown> = { ...prev, [key]: value }

      // Prevent ambiguous auth: many API connectors support either OAuth access_token OR api_key.
      // If user fills one, clear the other to avoid sending the wrong header (common cause of 401s).
      const strVal = typeof value === "string" ? value.trim() : value
      const hasValue = typeof strVal === "string" ? strVal.length > 0 : !!strVal

      if (key === "api_key" && hasValue) {
        if (typeof next["access_token"] === "string") next["access_token"] = ""
        if (typeof next["token"] === "string") next["token"] = ""
      }

      if ((key === "access_token" || key === "token") && hasValue) {
        if (typeof next["api_key"] === "string") next["api_key"] = ""
      }

      return next
    })
    setTestResult(null)
    setError(null)
  }

  const toggleSecretVisibility = (key: string) => {
    setShowSecrets((prev) => ({ ...prev, [key]: !prev[key] }))
  }

  const normalizeAuthFields = (cfg: Record<string, unknown>) => {
    const next = { ...cfg }
    const apiKey = typeof next.api_key === "string" ? next.api_key.trim() : ""
    const accessToken = typeof next.access_token === "string" ? next.access_token.trim() : ""
    const token = typeof next.token === "string" ? next.token.trim() : ""

    // If api_key is present, prefer it and clear bearer token fields (common user mistake is pasting API token into access_token).
    if (apiKey) {
      next.access_token = ""
      next.token = ""
      return next
    }

    // If bearer token is present, clear api_key to avoid ambiguity.
    if (accessToken || token) {
      next.api_key = ""
      return next
    }

    return next
  }

  const handleTest = async () => {
    // Snapshot the current config; we've observed cases where
    // async test flows lead to inputs visually clearing (likely due to
    // upstream re-renders). We'll restore if the state gets wiped.
    const snapshot = { ...formDataRef.current }

    setTesting(true)
    setTestResult(null)

    // Check if Docker container needs to be started
    const needsDockerStart = connector.docker_status === "stopped" || 
                            connector.docker_status === "not_deployed"

    try {
      // If Docker is not running, show starting state
      if (needsDockerStart) {
        setDockerStarting(true)
        toast.info("Starting connector...", {
          description: `Spinning up ${connector.display_name} Docker container`,
        })
        // Add a small delay to simulate startup
        await new Promise(resolve => setTimeout(resolve, 1500))
      }
      
      let result: { success: boolean; message: string }
      
      // If editing existing connection, use connection ID to test with stored credentials
      // This avoids sending masked passwords
      if (isEditing && connectionId) {
        const response = await authFetch(`${API_ENDPOINTS.CONNECTIONS.TEST_BY_ID(connectionId)}?allow_draft=true`, {
          method: "POST",
        })
        const data = await response.json()
        result = {
          success: data.success ?? false,
          message: !data.success && data.error 
            ? data.error 
            : (data.message || data.error || "Connection test successful"),
        }
      } else {
        // New connection - use form data, plus any credentials from the
        // multi-auth picker (authValues) which are NOT stored in formData.
        const baseConfig = oauthTokenId
          ? { ...snapshot, oauth_token_id: oauthTokenId }
          : { ...snapshot }
        // Inject the picker's auth_method + credential values whenever the
        // connector declares multi-auth. Previously this was gated on the
        // absence of oauth_provider — that's wrong when both are declared
        // (e.g. Shopify supports OAuth AND a custom-header token); the
        // picker needs to drive auth_method even in that case.
        const withAuth = hasMultiAuth
          ? { ...baseConfig, ...authValues, auth_method: authMethod }
          : baseConfig
        const testConfig = normalizeAuthFields(withAuth as Record<string, unknown>)

        result = await testMCPConnection(connector.name, testConfig)
      }
      
      // Improve error messages for better UX
      if (!result.success && result.message) {
        result.message = improveErrorMessage(result.message, connector.name)
      }
      
      setTestResult(result)
      
      if (result.success && needsDockerStart) {
        toast.success("Connector started successfully!")
      }
    } catch (err) {
      const rawMessage = err instanceof Error ? err.message : "Connection test failed"
      setTestResult({
        success: false,
        message: improveErrorMessage(rawMessage, connector.name),
      })
    } finally {
      setTesting(false)
      setDockerStarting(false)

      // Defensive restore: if the config got cleared during the async test,
      // put the user's inputs back.
      setFormData((current) => {
        const keysToCheck = (requiredFields?.length ? requiredFields : Object.keys(snapshot)) as string[]
        const hasNonEmptyValue = (obj: Record<string, unknown>, k: string) => {
          const v = obj?.[k]
          if (v === null || v === undefined) return false
          if (typeof v === "string") return v.trim().length > 0
          if (typeof v === "number") return !Number.isNaN(v)
          if (typeof v === "boolean") return true
          return true
        }

        const snapshotHasAny = keysToCheck.some((k) => hasNonEmptyValue(snapshot, k))
        const currentHasAny = keysToCheck.some((k) => hasNonEmptyValue(current, k))

        if (snapshotHasAny && !currentHasAny) return snapshot
        return current
      })
    }
  }
  
  // Handle OAuth success
  const handleOAuthSuccess = (tokenId: string) => {
    setOauthTokenId(tokenId)
    setOauthConnected(true)
    toast.success("OAuth authentication successful!", {
      description: `Connected to ${connector.display_name}`,
    })
  }
  
  // Handle OAuth error
  const handleOAuthError = (error: string) => {
    setError({
      message: "OAuth authentication failed",
      suggestion: error,
    })
  }

  // Save the caller's BYO OAuth app (client_id/secret) for this provider. On
  // success the provider becomes connectable without an operator env edit.
  const handleSaveOAuthApp = async () => {
    if (!connector.oauth_provider) return
    if (!byoClientId.trim() || !byoClientSecret.trim()) {
      toast.error("Client ID and Client Secret are required")
      return
    }
    setByoSaving(true)
    try {
      const res = await authFetch(API_ENDPOINTS.OAUTH.APPS, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider: connector.oauth_provider,
          client_id: byoClientId.trim(),
          client_secret: byoClientSecret.trim(),
          scopes: byoScopes.trim(),
        }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        toast.error(err?.message || "Failed to save OAuth app")
        return
      }
      const data = await res.json()
      setByoApp((prev) => ({
        ...(prev || { provider: connector.oauth_provider!, known: true }),
        configured: true,
        redirect_uri: data?.redirect_uri,
        client_id: data?.client_id,
      }))
      setByoClientSecret("") // don't retain the plaintext secret in memory
      setShowByoForm(false)
      // Changing the OAuth app credentials invalidates any prior connected
      // state — the existing token was minted under the old app. Force a fresh
      // authorization so we never test/save against a stale token.
      setOauthConnected(false)
      setOauthTokenId(null)
      toast.success("OAuth app saved — you can now connect")
    } catch {
      toast.error("Failed to save OAuth app")
    } finally {
      setByoSaving(false)
    }
  }

  // BYO is "eligible" when the provider is known to providers.json but has no
  // operator env app. The OAuth flow is usable once env-enabled OR a BYO app is
  // configured.
  const byoEligible = !!(byoApp?.known && !byoApp?.operator_managed)
  const oauthUsable = !!oauthProviderStatus?.enabled || !!byoApp?.configured

  const handleSave = async () => {
    // Generate trace ID for request tracking
    const traceId = `trace-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`
    if (process.env.NODE_ENV === 'development') {
      console.log(`[${traceId}] GenericConnectorForm: handleSave called`)
      console.log(`[${traceId}] Connection name: ${connectionName}`)
      console.log(`[${traceId}] Connection type: ${connectionType}`)
      console.log(`[${traceId}] Connector: ${connector.name}`)
    }
    
    // Validate connection name
    if (!connectionName.trim()) {
      setError({
        message: "Connection name is required",
        suggestion: "Please enter a name to identify this connection.",
        field: "connection_name",
      })
      document.getElementById("connection_name")?.focus()
      return
    }

    // Validate connection type
    if (!connectionType) {
      console.error(`[${traceId}] ERROR: connectionType is empty!`)
      setError({
        message: "Connection type is required",
        suggestion: "Please select whether this is a source or destination.",
        field: "connection_type",
      })
      return
    }

    // Validate required config fields
    const oauthSatisfiedFields = new Set([
      // Common token/app credential fields that OAuth flow can provide indirectly via oauth_token_id
      "access_token",
      "api_key",
      "client_id",
      "client_secret",
      "redirect_uri",
      "refresh_token",
      "token_type",
    ])

    const missingFields = requiredFields.filter((field) => {
      if (formData[field]) return false
      // If OAuth has completed, allow saving without manually entering token/app credentials.
      if (oauthTokenId && connector.oauth_provider && oauthSatisfiedFields.has(field)) return false
      // Phase 13g — credentials supplied via the multi-auth picker satisfy the
      // schema's required fields (the picker writes to authValues, not formData).
      if (hasMultiAuth && authValues[field]) return false
      return true
    })
    if (missingFields.length > 0) {
      const firstMissing = missingFields[0]
      setError({
        message: `Please fill in the required field: ${formatLabel(firstMissing)}`,
        suggestion: missingFields.length > 1 
          ? `Also missing: ${missingFields.slice(1).map(formatLabel).join(", ")}` 
          : undefined,
        field: firstMissing,
      })
      document.getElementById(firstMissing)?.focus()
      return
    }

    setSaving(true)
    setError(null)

    try {
      // Build request payload - different structure for create vs edit
      // Include OAuth token if available
      const baseConfigWithAuth = oauthTokenId
        ? { ...formData, oauth_token_id: oauthTokenId }
        : formData
      // When the connector declares supported_auth_methods, inject the
      // picker's auth_method + credential values so the generated
      // connector's runtime dispatcher routes correctly. Phase 13b
      // originally gated this on `!oauth_provider`, but that's wrong when
      // both are declared (Shopify supports OAuth AND a custom-header
      // token) — auth_method still needs to be threaded through.
      const withMultiAuth = hasMultiAuth
        ? { ...baseConfigWithAuth, ...authValues, auth_method: authMethod }
        : baseConfigWithAuth
      const configWithAuth = normalizeAuthFields(withMultiAuth as Record<string, unknown>)
      
      const requestPayload = isEditing
        ? {
            // When editing: only send name, config (API Gateway's UpdateConnectionRequest)
            // connection_type and connector_type are immutable after creation
            name: connectionName,
            config: configWithAuth,
            description: description || undefined,
            trace_id: traceId,
          }
        : {
            // When creating: send all fields (CreateConnectionRequest)
            name: connectionName,
            connection_type: connectionType,
            connector_type: connector.name,
            sync_mode: connectionType === "source" ? syncMode : undefined,
            cdc_mode: connectionType === "source" && syncMode === "cdc" ? cdcMode : undefined,
            config: configWithAuth,
            description: description || undefined,
            trace_id: traceId,
          }
      
      // Mask sensitive fields for logging
      const maskedConfig = { ...formData }
      const sensitiveKeys = ['password', 'secret', 'api_key', 'token', 'secret_key', 'access_key']
      sensitiveKeys.forEach(key => {
        if (maskedConfig[key]) {
          maskedConfig[key] = '********'
        }
      })
      
      if (process.env.NODE_ENV === 'development') {
        console.log(`[${traceId}] REQUEST SUMMARY (${isEditing ? 'UPDATE' : 'CREATE'}):`)
        console.log(`[${traceId}]   - name: ${requestPayload.name}`)
        if (!isEditing) {
          const rp = requestPayload as Record<string, unknown>
          console.log(`[${traceId}]   - connection_type: ${rp["connection_type"]}`)
          console.log(`[${traceId}]   - connector_type: ${rp["connector_type"]}`)
          console.log(`[${traceId}]   - sync_mode: ${rp["sync_mode"]}`)
          console.log(`[${traceId}]   - cdc_mode: ${rp["cdc_mode"]}`)
        }
        console.log(`[${traceId}]   - config: ${JSON.stringify(maskedConfig)}`)
      }
      
      await onSave(requestPayload)
      if (process.env.NODE_ENV === 'development') {
        console.log(`[${traceId}] ✅ Save successful`)
      }
    } catch (err) {
      console.error(`[${traceId}] ❌ Save failed:`, err)
      
      // Parse error into user-friendly format
      if (err instanceof APIRequestError) {
        // Map backend field names to our form input ids
        const field =
          err.field === "name"
            ? "connection_name"
            : err.field === "connector_type"
              ? "connector_type"
              : err.field

        setError({
          message: err.message,
          suggestion: err.suggestion,
          field,
          code: err.code,
        })
        
        // Focus the problematic field if identified
        if (field) {
          const fieldElement = document.getElementById(field)
          if (fieldElement) {
            fieldElement.focus()
            fieldElement.scrollIntoView({ behavior: 'smooth', block: 'center' })
          }
        }
      } else {
        const parsed = parseAPIError(err)
        setError({
          message: parsed.message,
          suggestion: parsed.suggestion,
          field: parsed.field,
          code: parsed.code,
        })
      }
    } finally {
      setSaving(false)
    }
  }

  const renderField = (key: string, prop: ConfigProperty) => {
    const isRequired = requiredFields.includes(key)
    // Check multiple indicators for secret fields:
    // 1. prop.secret (used in metadata.json)
    // 2. prop.sensitive (alternative naming)
    // 3. Common secret field-name substrings (shared with the auth-fallback
    //    credential check via isCredentialFieldName so the two never drift).
    const isSecret = prop.secret === true ||
                     prop.sensitive === true ||
                     isCredentialFieldName(key)
    const value = formData[key] ?? ""

    // Handle enum (dropdown)
    if (prop.enum && prop.enum.length > 0) {
      return (
        <div key={key} className="space-y-2">
          <Label htmlFor={key} className="flex items-center gap-1">
            {formatLabel(key)}
            {isRequired && <span className="text-red-500">*</span>}
          </Label>
          <Select
            value={value as string}
            onValueChange={(v) => handleInputChange(key, v)}
          >
            <SelectTrigger>
              <SelectValue placeholder={`Select ${formatLabel(key)}...`} />
            </SelectTrigger>
            <SelectContent>
              {prop.enum.map((opt) => (
                <SelectItem key={opt} value={opt}>
                  {opt}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {prop.description && (
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              {prop.description}
            </p>
          )}
        </div>
      )
    }

    // Handle integer
    if (prop.type === "integer" || prop.type === "number") {
      return (
        <div key={key} className="space-y-2">
          <Label htmlFor={key} className="flex items-center gap-1">
            {formatLabel(key)}
            {isRequired && <span className="text-red-500">*</span>}
          </Label>
          <Input
            id={key}
            type="number"
            value={value as number}
            onChange={(e) =>
              handleInputChange(key, parseInt(e.target.value) || 0)
            }
            // Don't fall back to description: it's already rendered as helper text
            // below, so duplicating it shows the same string twice (ISSUE-009).
            placeholder={prop.placeholder || ""}
          />
          {prop.description && (
            <p className="text-xs text-zinc-500 dark:text-zinc-400">
              {prop.description}
            </p>
          )}
        </div>
      )
    }

    // Handle boolean
    if (prop.type === "boolean") {
      return (
        <div key={key} className="flex items-center gap-3 py-2">
          <input
            id={key}
            type="checkbox"
            checked={value as boolean}
            onChange={(e) => handleInputChange(key, e.target.checked)}
            className="w-4 h-4 rounded border-zinc-300 dark:border-zinc-600"
          />
          <Label htmlFor={key} className="flex items-center gap-1 cursor-pointer">
            {formatLabel(key)}
            {isRequired && <span className="text-red-500">*</span>}
          </Label>
          {prop.description && (
            <span className="text-xs text-zinc-500 dark:text-zinc-400 ml-2">
              - {prop.description}
            </span>
          )}
        </div>
      )
    }

    // Default: text input (with secret handling)
    const secretPlaceholder = isEditing && isSecret
      ? "Leave blank to keep existing"
      : (prop.placeholder || "")  // description is shown as helper text below (ISSUE-009)
    
    return (
      <div key={key} className="space-y-2">
        <Label htmlFor={key} className="flex items-center gap-1">
          {formatLabel(key)}
          {isRequired && !isEditing && <span className="text-red-500">*</span>}
        </Label>
        <div className="relative">
          <Input
            id={key}
            type={isSecret && !showSecrets[key] ? "password" : "text"}
            value={value as string}
            onChange={(e) => handleInputChange(key, e.target.value)}
            placeholder={secretPlaceholder}
            className={isSecret ? "pr-10" : ""}
            // Stop the browser from autofilling saved login credentials (e.g. the
            // user's email) into connection config fields like Host / Database.
            autoComplete={isSecret ? "new-password" : "off"}
          />
          {isSecret && (
            <button
              type="button"
              onClick={() => toggleSecretVisibility(key)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-600"
            >
              {showSecrets[key] ? (
                <EyeOff className="h-4 w-4" />
              ) : (
                <Eye className="h-4 w-4" />
              )}
            </button>
          )}
        </div>
        {isEditing && isSecret ? (
          <p className="text-xs text-zinc-500 dark:text-zinc-400">
            Leave blank to keep the existing value
          </p>
        ) : prop.description ? (
          <p className="text-xs text-zinc-500 dark:text-zinc-400">
            {prop.description}
          </p>
        ) : null}
      </div>
    )
  }

  // Some generated API connectors include mock-only fields used by QA harnesses.
  // These are confusing for end-users and (in most connectors) not actually consumed at runtime.
  const HIDDEN_CONFIG_KEYS = new Set<string>([
    "mock_server_name",
    "mock_base_url",
  ])

  const isPrimaryConfigField = (key: string, prop: ConfigProperty): boolean => {
    // When the schema declares an explicit ui_tier (metadata.json), honor it —
    // this lets a connector curate which fields are shown up-front vs collapsed.
    // Required fields always stay primary so they can't hide behind "Advanced".
    if (prop?.ui_tier === "basic") return true
    if (prop?.ui_tier === "advanced") return requiredFields.includes(key)
    // Fallback heuristic (connectors without ui_tier behave exactly as before).
    if (requiredFields.includes(key)) return true
    if (prop?.secret === true || prop?.sensitive === true) return true
    // Show common auth-ish fields up-front even if they are technically optional (OAuth may satisfy them).
    const k = key.toLowerCase()
    if (k.includes("access_token")) return true
    if (k.includes("refresh_token")) return true
    if (k === "token" || k.includes("_token")) return true
    if (k.includes("api_key") || k.includes("apikey")) return true
    if (k.includes("client_id") || k.includes("client_secret")) return true
    if (k.includes("password") || k.includes("secret")) return true
    return false
  }

  // A field with an explicit `applies` is hidden when it doesn't match the chosen
  // direction (e.g. source-only knobs on a destination). Absent/"both" → always shown,
  // so connectors that don't declare `applies` are unaffected.
  const appliesToDirection = (prop: ConfigProperty): boolean => {
    const a = prop?.applies
    if (!a || a === "both") return true
    return a === connectionType
  }

  // Stable ordering within a tier: fields with ui_order sort ascending; fields
  // without one keep their schema (insertion) order after the ordered ones.
  const byUiOrder = (
    a: readonly [string, ConfigProperty],
    b: readonly [string, ConfigProperty],
  ): number => {
    const ao = a[1]?.ui_order ?? Number.MAX_SAFE_INTEGER
    const bo = b[1]?.ui_order ?? Number.MAX_SAFE_INTEGER
    return ao - bo
  }

  // Whether this connector collects any secret/credential input — gates the
  // "encrypted at rest" reassurance so it only shows when there's actually a
  // credential to reassure about (any auth path, or a secret config field).
  const collectsSecrets =
    authUI.kind !== "none" ||
    Object.entries(properties).some(
      ([k, p]) => p.secret === true || p.sensitive === true || isCredentialFieldName(k),
    )

  return (
    <div className="space-y-6">
      {/* Connection Metadata */}
      <div className="space-y-4 pb-4 border-b">
      <div className="space-y-2">
        <Label htmlFor="connection_name" className="flex items-center gap-1">
          Connection Name
          <span className="text-red-500">*</span>
        </Label>
        <Input
          id="connection_name"
            value={connectionName}
            onChange={(e) => setConnectionName(e.target.value)}
          placeholder={`My ${connector.display_name} Connection`}
          autoComplete="off"
        />
      </div>

        {/* Connection Type - disabled when editing */}
        {!isEditing && (
          <div className="space-y-2">
            <Label htmlFor="connection_type" className="flex items-center gap-1">
              Connection Type
              <span className="text-red-500">*</span>
            </Label>
            {availableTypes.length > 0 ? (
              <Select
                value={connectionType}
                onValueChange={(value) => setConnectionType(value as "source" | "destination")}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select connection type...">
                    {connectionType === "source" ? "Source (Read Data)" : "Destination (Write Data)"}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent position="popper">
                  {availableTypes.includes("source") && (
                    <SelectItem value="source">
                      Source (Read Data)
                    </SelectItem>
                  )}
                  {availableTypes.includes("destination") && (
                    <SelectItem value="destination">
                      Destination (Write Data)
                    </SelectItem>
                  )}
                </SelectContent>
              </Select>
            ) : (
              <div className="p-3 rounded-md bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800">
                <p className="text-sm text-red-600 dark:text-red-400">
                  ⚠️ This connector doesn't specify supported connection types. Please contact support.
                </p>
              </div>
            )}
            <p className="text-xs text-zinc-500">
              Choose whether to read data from or write data to this connector
            </p>
          </div>
        )}

        {/* Help me choose (compact / collapsible) */}
        {connectionType === "source" && connector.supports_cdc && !recommendedMode && (
          <Accordion type="single" collapsible className="border rounded-lg">
            <AccordionItem value="help-choose" className="border-none">
              <AccordionTrigger className="px-4 py-3 text-sm font-medium">
                Not sure? Help me choose Batch vs Real-time
              </AccordionTrigger>
              <AccordionContent className="px-4 pb-4">
                <div className="space-y-4">
                  <div className="flex items-start gap-3">
                    <Info className="h-4 w-4 text-violet-600 mt-0.5 flex-shrink-0" />
                    <p className="text-sm text-zinc-600 dark:text-zinc-400">
                      Answer 1–2 quick questions and we’ll recommend the best default.
                    </p>
                  </div>

                  {/* Q1 */}
                  <div className="space-y-2">
                    <Label className="text-sm font-semibold">
                      What are you building?
                    </Label>
                    <div className="grid gap-2">
                      <button
                        type="button"
                        onClick={() => setUseCase("realtime")}
                        className={`p-3 rounded-lg border text-left transition-all ${
                          useCase === "realtime"
                            ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                            : "border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900/10 hover:border-zinc-300"
                        }`}
                      >
                        <div className="font-medium">Real-time dashboards / features</div>
                        <div className="text-xs text-zinc-500 mt-1">Live metrics, alerts, user-facing apps</div>
                      </button>
                      <button
                        type="button"
                        onClick={() => setUseCase("analytics")}
                        className={`p-3 rounded-lg border text-left transition-all ${
                          useCase === "analytics"
                            ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                            : "border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900/10 hover:border-zinc-300"
                        }`}
                      >
                        <div className="font-medium">Analytics & reporting</div>
                        <div className="text-xs text-zinc-500 mt-1">BI, reports, warehouse</div>
                      </button>
                      <button
                        type="button"
                        onClick={() => setUseCase("backup")}
                        className={`p-3 rounded-lg border text-left transition-all ${
                          useCase === "backup"
                            ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                            : "border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900/10 hover:border-zinc-300"
                        }`}
                      >
                        <div className="font-medium">Backup / archival</div>
                        <div className="text-xs text-zinc-500 mt-1">Snapshots, periodic copies</div>
                      </button>
                    </div>
                  </div>

                  {/* Q2 */}
                  {useCase === "analytics" && (
                    <div className="space-y-2">
                      <Label className="text-sm font-semibold">
                        How fresh does it need to be?
                      </Label>
                      <div className="grid gap-2">
                        <button
                          type="button"
                          onClick={() => setFreshnessRequirement("up_to_minute")}
                          className={`p-3 rounded-lg border text-left transition-all ${
                            freshnessRequirement === "up_to_minute"
                              ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                              : "border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900/10 hover:border-zinc-300"
                          }`}
                        >
                          <div className="font-medium">Up-to-the-minute (&lt; 5 min)</div>
                        </button>
                        <button
                          type="button"
                          onClick={() => setFreshnessRequirement("hourly")}
                          className={`p-3 rounded-lg border text-left transition-all ${
                            freshnessRequirement === "hourly"
                              ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                              : "border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900/10 hover:border-zinc-300"
                          }`}
                        >
                          <div className="font-medium">Hourly is fine</div>
                        </button>
                        <button
                          type="button"
                          onClick={() => setFreshnessRequirement("daily")}
                          className={`p-3 rounded-lg border text-left transition-all ${
                            freshnessRequirement === "daily"
                              ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                              : "border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900/10 hover:border-zinc-300"
                          }`}
                        >
                          <div className="font-medium">Daily or less</div>
                        </button>
                      </div>
                    </div>
                  )}

                  <div className="flex items-center justify-end">
                    <button
                      type="button"
                      onClick={() => {
                        setUseCase("")
                        setFreshnessRequirement("")
                      }}
                      className="text-xs text-zinc-500 hover:underline"
                    >
                      Reset answers
                    </button>
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        )}
        
        {/* Recommendation banner - show after questionnaire */}
        {recommendedMode && connectionType === "source" && (
          <div className={`p-4 rounded-lg border-2 ${
            recommendedMode === "cdc"
              ? "border-emerald-200 dark:border-emerald-800 bg-emerald-50 dark:bg-emerald-900/20"
              : "border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/20"
          }`}>
            <div className="flex items-start gap-3">
              <CheckCircle2 className={`h-5 w-5 mt-0.5 flex-shrink-0 ${
                recommendedMode === "cdc" ? "text-emerald-600" : "text-blue-600"
              }`} />
              <div className="flex-1">
                <div className="font-semibold">
                  Recommended: {recommendedMode === "cdc" ? "Real-time (CDC)" : "Batch"}
                </div>
                <div className="text-sm mt-1 opacity-90">
                  {recommendedMode === "cdc" 
                    ? useCase === "realtime"
                      ? "Real-time data freshness required for operational dashboards"
                      : "Up-to-the-minute analytics requires continuous data streaming"
                    : useCase === "backup"
                      ? "Batch mode is ideal for backups and archival use cases"
                      : "Scheduled batch sync is efficient for your analytics needs"}
                </div>
                <button
                  type="button"
                  onClick={() => {
                    setRecommendedMode(null)
                    setShowQuestions(true)
                    setUseCase("")
                    setFreshnessRequirement("")
                  }}
                  className="text-sm mt-2 underline opacity-75 hover:opacity-100"
                >
                  Change recommendation
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Sync Mode Selection - Single selector (shared config form for Batch/CDC) */}
        {connectionType === "source" && (
          <div className="space-y-4">
            <Label className="flex items-center gap-1">
              Sync Mode
              <span className="text-red-500">*</span>
              {recommendedMode && (
                <span className="ml-2 text-xs px-2 py-1 rounded bg-violet-100 dark:bg-violet-900/50 text-violet-700 dark:text-violet-300">
                  Recommended: {recommendedMode === "cdc" ? "CDC" : "Batch"}
                </span>
              )}
            </Label>
            <div className="grid grid-cols-2 gap-3">
              {/* Batch Mode */}
              <button
                type="button"
                onClick={() => setSyncMode("batch")}
                className={`p-4 rounded-lg border-2 text-left transition-all ${
                  syncMode === "batch"
                    ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                    : "border-zinc-200 dark:border-zinc-700 hover:border-zinc-300 dark:hover:border-zinc-600"
                }`}
              >
                <div className="flex items-center gap-2 mb-2">
                  <Clock className={`h-5 w-5 ${syncMode === "batch" ? "text-violet-600" : "text-zinc-400"}`} />
                  <span className={`font-medium ${syncMode === "batch" ? "text-violet-700 dark:text-violet-300" : "text-zinc-700 dark:text-zinc-300"}`}>
                    Batch
                  </span>
                </div>
                <p className="text-xs text-zinc-500 dark:text-zinc-400">
                  One-time or scheduled data sync
                </p>
              </button>

              {/* CDC/Real-time Mode - Only if connector supports it */}
              <button
                type="button"
                onClick={() => connector.supports_cdc && setSyncMode("cdc")}
                disabled={!connector.supports_cdc}
                className={`p-4 rounded-lg border-2 text-left transition-all ${
                  !connector.supports_cdc
                    ? "border-zinc-100 dark:border-zinc-800 opacity-50 cursor-not-allowed"
                    : syncMode === "cdc"
                    ? "border-violet-500 bg-violet-50 dark:bg-violet-900/20"
                    : "border-zinc-200 dark:border-zinc-700 hover:border-zinc-300 dark:hover:border-zinc-600"
                }`}
              >
                <div className="flex items-center gap-2 mb-2">
                  <Zap className={`h-5 w-5 ${syncMode === "cdc" ? "text-violet-600" : "text-zinc-400"}`} />
                  <span className={`font-medium ${syncMode === "cdc" ? "text-violet-700 dark:text-violet-300" : "text-zinc-700 dark:text-zinc-300"}`}>
                    Real-time (CDC)
                  </span>
                  {!connector.supports_cdc && (
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-zinc-100 dark:bg-zinc-800 text-zinc-500">
                      Not Available
                    </span>
                  )}
                </div>
                <p className="text-xs text-zinc-500 dark:text-zinc-400">
                  {connector.supports_cdc 
                    ? "Stream changes continuously" 
                    : "CDC not supported for this connector"}
                </p>
              </button>
            </div>

            {/* CDC options are part of the SAME form, but tucked into Advanced to avoid doubling the modal height */}
            {syncMode === "cdc" && connector.supports_cdc && (
              <Accordion type="single" collapsible className="border rounded-lg">
                <AccordionItem value="cdc-advanced" className="border-none">
                  <AccordionTrigger className="px-4 py-3 text-sm font-medium">
                    Advanced CDC options
                  </AccordionTrigger>
                  <AccordionContent className="px-4 pb-4 space-y-4">
                    <div className="space-y-2">
                      <Label className="text-sm font-medium">Start mode</Label>
                      <Select value={cdcMode} onValueChange={(v) => setCdcMode(v as CDCMode)}>
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="initial">Start with all data, then stream changes</SelectItem>
                          <SelectItem value="streaming_only">Only stream new changes (no snapshot)</SelectItem>
                        </SelectContent>
                      </Select>
                      <p className="text-xs text-zinc-500">
                        Default is recommended for new pipelines.
                      </p>
                    </div>

                    <div className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-zinc-50/50 dark:bg-zinc-900/20 p-3">
                      <div className="flex items-start gap-2">
                        <Info className="h-4 w-4 text-zinc-500 mt-0.5 flex-shrink-0" />
                        <div className="text-xs text-zinc-600 dark:text-zinc-400 space-y-1">
                          <div className="font-medium text-zinc-700 dark:text-zinc-300">How changes are written</div>
                          <div>
                            - <span className="font-medium">Databases</span>: inserts/updates/deletes are applied when supported.
                          </div>
                          <div>
                            - <span className="font-medium">Cloud storage</span>: changes are written as append-only files (no in-place updates).
                          </div>
                          <div>
                            - <span className="font-medium">Warehouses</span>: the pipeline will use the best available write strategy (e.g., merge/upsert when supported).
                          </div>
                        </div>
                      </div>
                    </div>
                  </AccordionContent>
                </AccordionItem>
              </Accordion>
            )}

            <p className="text-xs text-zinc-500">
              {syncMode === "batch" 
                ? "Data will be extracted on-demand or on a schedule" 
                : cdcMode === "initial"
                ? "All existing data will be captured first, then changes streamed in real-time"
                : "Only new changes from this moment forward will be captured"}
            </p>
          </div>
        )}

        <div className="space-y-2">
          <Label htmlFor="description" className="text-sm font-medium">
            Description{" "}
            <span className="text-muted-foreground text-xs">(optional)</span>
          </Label>
          <Input
            id="description"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Brief description of this connection"
            autoComplete="off"
          />
        </div>
      </div>

      {/* Credentials-encrypted-at-rest reassurance. Shown once at the top of the
          credential area whenever the form collects a secret. Grounded: the
          connection config (credentials included) is stored with AES-256-GCM —
          shared/go/crypto/encryption.go (32-byte key → AES-256, GCM). */}
      {collectsSecrets && (
        <div className="flex items-start gap-2.5 rounded-lg border border-emerald-200 dark:border-emerald-900/40 bg-emerald-50/60 dark:bg-emerald-950/20 px-3 py-2.5">
          <ShieldCheck className="h-4 w-4 mt-0.5 flex-shrink-0 text-emerald-600 dark:text-emerald-400" />
          <p className="text-xs text-emerald-800 dark:text-emerald-300">
            Your credentials are encrypted at rest with{" "}
            <span className="font-semibold">AES-256</span>. They&apos;re stored securely and never shown to the AI.
          </p>
        </div>
      )}

      {/* OAuth section. When the connector ALSO declares supported_auth_methods
          (multi-auth picker rendered below), this section is only relevant
          when the user has actually selected oauth2 — hide otherwise so it
          doesn't compete for attention. Single-auth OAuth-only connectors
          render this section unconditionally as before. */}
      {authUI.showOAuthConnect && (
        <div className="space-y-4 pb-4 border-b border-zinc-200 dark:border-zinc-800">
          <div className="flex items-center gap-2">
            <div className="flex-1">
              <Label className="text-sm font-semibold flex items-center gap-2">
                <span>🔐</span> {hasMultiAuth ? "Sign in with OAuth" : "Authentication"}
              </Label>
              <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-1">
                {hasMultiAuth
                  ? `Authorize via ${connector.display_name}'s OAuth flow — no manual token paste needed.`
                  : (connector.auth_type === "oauth" || connector.auth_type === "oauth2"
                    ? `Connect your ${connector.display_name} account using OAuth`
                    : `OAuth is supported for ${connector.display_name} (optional)`)}
              </p>
            </div>
            {oauthConnected && (
              <CheckCircle2 className="h-5 w-5 text-green-500" />
            )}
          </div>

          {/* If OAuth is not enabled in backend AND we can't offer a BYO app,
              show guidance and disable the connect button. When byoEligible, we
              render the "Your OAuth App" form below instead of this warning. */}
          {oauthProviderStatus &&
            !oauthProviderStatus.enabled &&
            !byoEligible &&
            oauthProviderStatus.message !== "Checking OAuth availability..." && (
            <div className="p-3 rounded-lg bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800">
              <p className="text-sm font-medium text-amber-800 dark:text-amber-200">
                ⚠️ OAuth Not Available
              </p>
              <p className="text-xs text-amber-700 dark:text-amber-300 mt-1">
                {oauthProviderStatus.message || `OAuth provider '${oauthProviderStatus.name}' not configured`}
              </p>
              <p className="text-xs text-amber-700 dark:text-amber-300 mt-2">
                {hasMultiAuth
                  ? "💡 Tip: switch the Authentication method dropdown below to use a manual token instead."
                  : "💡 Tip: You can still use API token/manual credentials below."}
              </p>
            </div>
          )}

          {/* BYO OAuth app form — shown when the provider is known to
              providers.json but has no operator-set env app. The user registers
              their own OAuth app at the vendor and pastes its credentials. */}
          {byoEligible && (
            <div className="space-y-3 p-3 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40">
              <div className="flex items-center justify-between">
                <p className="text-sm font-medium">Your OAuth App</p>
                {byoApp?.configured && (
                  <span className="text-xs text-green-600 dark:text-green-400 flex items-center gap-1">
                    <CheckCircle2 className="h-3.5 w-3.5" /> Configured
                  </span>
                )}
              </div>
              <p className="text-xs text-zinc-500 dark:text-zinc-400">
                {byoApp?.configured ? (
                  <>
                    Your {connector.display_name} OAuth app is configured on this instance. Edit it
                    below if your client ID/secret changed, then use Connect to authorize.
                  </>
                ) : (
                  <>
                    {connector.display_name}{" "}isn&apos;t preconfigured on this instance. Register an
                    OAuth app with {connector.display_name}, set its redirect / callback URL to the
                    value below, then paste its credentials here.
                  </>
                )}
              </p>

              {byoApp?.redirect_uri && (
                <div className="space-y-1">
                  <Label className="text-xs">Redirect URI to register</Label>
                  <div className="flex items-center gap-2">
                    <code className="flex-1 text-xs font-mono bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded px-2 py-1.5 overflow-x-auto">
                      {byoApp.redirect_uri}
                    </code>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        navigator.clipboard?.writeText(byoApp.redirect_uri || "")
                        toast.success("Redirect URI copied")
                      }}
                    >
                      Copy
                    </Button>
                  </div>
                  {/* Fail loud if the server handed us a localhost callback while
                      the app is served from a real origin — registering this at
                      the vendor would make every OAuth redirect fail silently.
                      Root cause is server-side OAUTH_CALLBACK_URL being unset. */}
                  {/^https?:\/\/(localhost|127\.0\.0\.1)/i.test(byoApp.redirect_uri) &&
                    typeof window !== "undefined" &&
                    !/^https?:\/\/(localhost|127\.0\.0\.1)/i.test(window.location.origin) && (
                      <p className="text-[11px] text-amber-700 dark:text-amber-400">
                        ⚠️ This looks like a local-dev callback URL but the app is served from{" "}
                        <code className="font-mono">{window.location.origin}</code>. Confirm it
                        matches where this app is hosted before registering it — otherwise the OAuth
                        redirect will fail. (Operator: set <code className="font-mono">OAUTH_CALLBACK_URL</code>.)
                      </p>
                    )}
                </div>
              )}

              {showByoForm || !byoApp?.configured ? (
                <>
                  <div className="space-y-1">
                    <Label className="text-xs">Client ID</Label>
                    <Input
                      value={byoClientId}
                      onChange={(e) => setByoClientId(e.target.value)}
                      placeholder="Your app's client ID"
                      // Chrome ignores autoComplete="off" and autofills the saved
                      // login email here; "new-password" is the only value it honors.
                      autoComplete="new-password"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">Client Secret</Label>
                    <Input
                      type="password"
                      value={byoClientSecret}
                      onChange={(e) => setByoClientSecret(e.target.value)}
                      placeholder="Your app's client secret"
                      autoComplete="new-password"
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">
                      Scopes <span className="text-zinc-400">(optional)</span>
                    </Label>
                    <Input
                      value={byoScopes}
                      onChange={(e) => setByoScopes(e.target.value)}
                      placeholder={byoApp?.default_scopes || "space-separated scopes"}
                      autoComplete="off"
                    />
                  </div>
                  {byoApp?.grant_type === "client_credentials" && (
                    <p className="text-xs text-amber-600 dark:text-amber-400">
                      This provider uses a server-to-server (client_credentials) flow — automatic
                      token minting on save is coming soon; for now the app credentials are stored.
                    </p>
                  )}
                  <Button
                    type="button"
                    size="sm"
                    onClick={handleSaveOAuthApp}
                    disabled={byoSaving}
                    className="w-full"
                  >
                    {byoSaving ? "Saving…" : byoApp?.configured ? "Update OAuth App" : "Save OAuth App"}
                  </Button>
                </>
              ) : (
                <Button type="button" variant="ghost" size="sm" onClick={() => setShowByoForm(true)}>
                  Edit OAuth app
                </Button>
              )}
            </div>
          )}

          {(() => {
            // Required non-credential fields the OAuth flow can't satisfy itself
            // (e.g. a per-tenant subdomain like Shopify's `shop`). Generic: we list
            // whatever the connector declares as required + non-credential and point
            // the user at the Configuration section. The per-field guidance (e.g.
            // "store subdomain only — acme not acme.myshopify.com") lives in each
            // field's own `configuration_schema` description, rendered there — no
            // connector-name special case here.
            const credentialLike = new Set(["api_key", "access_token", "token", "refresh_token", "client_id", "client_secret"])
            const missingExtraFields = requiredFields.filter(
              (f) => !credentialLike.has(f) && !formData[f]
            )
            if (missingExtraFields.length === 0) return null
            const labels = missingExtraFields.map((f) =>
              f.split(/[-_]/).map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ")
            )
            return (
              <div className="p-3 rounded-lg bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800 text-xs text-blue-800 dark:text-blue-200">
                Enter your <strong>{labels.join(", ")}</strong> in the Configuration section below, then click Connect.
              </div>
            )
          })()}

          <OAuthConnectButton
            // Section renders only when showOAuthConnect, so oauthProvider is set;
            // the "" tail is just to satisfy the non-null prop type.
            provider={authUI.oauthProvider ?? connector.oauth_provider ?? ""}
            displayName={connector.display_name}
            extraParams={(() => {
              // Metadata-driven: a connector with a per-tenant OAuth URL declares
              // oauth_authorize_params = { authorizeParam: configField } (e.g.
              // Shopify: { shop: "shop" } → ?shop=acme). We forward each mapped
              // field's value verbatim. No connector-name special case — any
              // connector that declares the mapping works with zero modal changes.
              const params: Record<string, string> = {}
              for (const [param, field] of Object.entries(connector.oauth_authorize_params ?? {})) {
                const v = formData[field]
                if (v != null && String(v).trim() !== "") params[param] = String(v).trim()
              }
              return Object.keys(params).length > 0 ? params : undefined
            })()}
            onSuccess={handleOAuthSuccess}
            onError={handleOAuthError}
            variant="outline"
            className="w-full"
            disabled={
              // Usable when the operator env app is enabled OR the user has saved
              // their own BYO app for this provider.
              !oauthUsable ||
              requiredFields.some((f) => {
                const credentialLike = new Set(["api_key", "access_token", "token", "refresh_token", "client_id", "client_secret"])
                return !credentialLike.has(f) && !formData[f]
              })
            }
          />

          {oauthConnected && (
            <div className="p-3 rounded-lg bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800">
              <p className="text-sm text-green-700 dark:text-green-300 flex items-center gap-2">
                <CheckCircle2 className="h-4 w-4" />
                Authentication successful! You can now test and save the connection.
              </p>
              <p className="text-xs text-green-700/80 dark:text-green-300/80 mt-1.5">
                Note: a brand-new {connector.display_name} token can take up to ~1 minute to activate. If Test Connection fails right after connecting, it&apos;s just the token propagating — Test retries automatically, or wait a moment and try again. You can also Save now; it&apos;ll work once the token activates.
              </p>
            </div>
          )}
        </div>
      )}

      {/* Phase 13b — Multi-auth picker. Active when the connector metadata
          declares supported_auth_methods. The user picks ONE method; the
          credential fields swap to match. The runtime dispatcher inside the
          generated connector reads config.auth_method to format requests.

          Coexists with the OAuth section above when both are declared
          (oauth_provider AND supported_auth_methods including oauth2):
          the picker lists all methods including "OAuth 2.0", and when the
          user picks oauth2 the picker hides its manual credential fields
          (AuthMethodPicker handles that internally) — the user authenticates
          via the OAuth Connect button rendered above instead. */}
      {/* Auth fallback — synthesized credential field for connectors whose
          metadata declares an auth_type but no input to enter it
          (computeAuthUI → kind "fallback"; see §3 row 4). */}
      {authUI.kind === "fallback" && (
        <div className="space-y-4 pb-4 border-b border-zinc-200 dark:border-zinc-800">
          <div>
            <Label className="text-sm font-semibold flex items-center gap-2">
              <span>🔐</span> Authentication
            </Label>
            <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-1">
              Provide your {connector.display_name} {authFallback.noun} below
            </p>
          </div>
          {authFallback.fields.map((fld) => (
            <div key={fld.key} className="space-y-1">
              <Label className="text-xs">
                {fld.label} <span className="text-red-500">*</span>
              </Label>
              <Input
                type={fld.secret ? "password" : "text"}
                value={String(formData[fld.key] ?? "")}
                onChange={(e) => handleInputChange(fld.key, e.target.value)}
                placeholder={fld.placeholder}
                autoComplete="off"
              />
            </div>
          ))}
        </div>
      )}

      {hasMultiAuth && (
        <div className="space-y-4 pb-4 border-b border-zinc-200 dark:border-zinc-800">
          <div>
            <Label className="text-sm font-semibold flex items-center gap-2">
              <span>🔐</span> Authentication
            </Label>
            <p className="text-xs text-zinc-500 dark:text-zinc-400 mt-1">
              {supportedAuthMethods.length === 1
                ? `Provide your ${connector.display_name} credentials below`
                : `${connector.display_name} supports ${supportedAuthMethods.length} authentication methods — pick the one you have credentials for`}
            </p>
          </div>
          <AuthMethodPicker
            methods={supportedAuthMethods}
            selectedMethod={authMethod}
            onMethodChange={setAuthMethod}
            values={authValues}
            onValueChange={(key, value) =>
              setAuthValues((prev) => ({ ...prev, [key]: value }))
            }
            oauthProvider={connector.oauth_provider}
            schemaKeys={schemaKeys}
          />
        </div>
      )}

      {/* Connector-specific Configuration */}
      {(() => {
        // Phase 13g — when the multi-auth picker above renders the chosen
        // method's credential field(s), suppress duplicate config_schema
        // entries for the same keys so the user doesn't see two
        // "Access Token" inputs in the modal. Runs regardless of
        // oauth_provider: when both are declared (Shopify), the picker
        // owns credential entry and the schema's access_token would
        // otherwise duplicate.
        const authKeysFromPicker = new Set<string>()
        if (hasMultiAuth) {
          for (const m of supportedAuthMethods) {
            for (const k of m.config_keys) authKeysFromPicker.add(k)
          }
        }
        const entries = Object.entries(properties).map(([key, prop]) => [key, prop as ConfigProperty] as const)
        const visible = entries.filter(
          ([key, prop]) =>
            !HIDDEN_CONFIG_KEYS.has(key) &&
            !authKeysFromPicker.has(key) &&
            // Required fields are always shown (hiding one would deadlock save
            // validation); otherwise honor the field's `applies` direction hint.
            (requiredFields.includes(key) || appliesToDirection(prop)),
        )

        // Hide the entire Configuration section (heading included) when there
        // are no non-auth fields to show — e.g. a connector whose only schema
        // key is its credential, which the auth picker already renders.
        if (visible.length === 0) return null

        const primary = visible
          .filter(([key, prop]) => isPrimaryConfigField(key, prop))
          .sort(byUiOrder)
        const advanced = visible
          .filter(([key, prop]) => !isPrimaryConfigField(key, prop))
          .sort(byUiOrder)

        return (
          <div className="space-y-4">
            <h3 className="text-sm font-semibold">
              {connector.auth_type === "oauth" ? "Additional Configuration" : "Configuration"}
            </h3>

            {/* Primary fields (required + auth-ish fields) */}
            {primary.map(([key, prop]) => renderField(key, prop))}

            {/* Everything else hidden behind Advanced */}
            {advanced.length > 0 && (
              <Accordion type="single" collapsible className="border rounded-lg">
                <AccordionItem value="advanced-config" className="border-none">
                  <AccordionTrigger className="px-4 py-3 text-sm font-medium">
                    Advanced settings
                  </AccordionTrigger>
                  <AccordionContent className="px-4 pb-4 space-y-6">
                    {advanced.map(([key, prop]) => renderField(key, prop))}
                  </AccordionContent>
                </AccordionItem>
              </Accordion>
            )}
          </div>
        )
      })()}

      {/* Test Result */}
      {testResult && (
        <div
          className={`p-4 rounded-lg flex items-start gap-3 ${
            testResult.success
              ? "bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800"
              : "bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800"
          }`}
        >
          {testResult.success ? (
            <CheckCircle2 className="h-5 w-5 text-green-600 mt-0.5 flex-shrink-0" />
          ) : (
            <AlertCircle className="h-5 w-5 text-red-600 mt-0.5 flex-shrink-0" />
          )}
          <div
            className={`text-sm whitespace-pre-line ${
              testResult.success
                ? "text-green-700 dark:text-green-300"
                : "text-red-700 dark:text-red-300"
            }`}
          >
            {testResult.message}
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="p-4 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800">
          <div className="flex items-start gap-3">
            <AlertCircle className="h-5 w-5 text-red-600 dark:text-red-400 mt-0.5 flex-shrink-0" />
            <div className="flex-1 space-y-1">
              <p className="text-sm font-medium text-red-700 dark:text-red-300">
                {error.message}
              </p>
              {error.suggestion && (
                <p className="text-sm text-red-600 dark:text-red-400 flex items-center gap-1.5">
                  <Info className="h-3.5 w-3.5" />
                  {error.suggestion}
                </p>
              )}
              {error.field && (
                <p className="text-xs text-red-500 dark:text-red-500 mt-1">
                  Check the <strong>{formatLabel(error.field)}</strong> field above.
                </p>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Actions */}
      <div className="flex items-center justify-end gap-3 pt-4 border-t border-zinc-200 dark:border-zinc-800">
        <Button variant="outline" onClick={onCancel}>
          Cancel
        </Button>
        <Button 
          variant="outline" 
          onClick={handleTest}
          disabled={
            testing ||
            authIncomplete
          }
        >
          {testing ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              {dockerStarting ? "Starting connector..." : "Testing..."}
            </>
          ) : connector.docker_status === "stopped" || connector.docker_status === "not_deployed" ? (
            <>
              <Play className="h-4 w-4 mr-2" />
              Start & Test Connection
            </>
          ) : (
            <>
              <Container className="h-4 w-4 mr-2" />
              Test Connection
            </>
          )}
        </Button>
        <Button
          onClick={handleSave}
          disabled={
            saving ||
            !connectionName.trim() ||
            authIncomplete
          }
          className="bg-gradient-to-r from-violet-600 to-indigo-600"
        >
          {saving ? (
            <>
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              {isEditing ? "Updating..." : "Saving..."}
            </>
          ) : (
            isEditing ? "Update Connection" : "Save Connection"
          )}
        </Button>
      </div>
    </div>
  )
}

// Helper to format field labels
function formatLabel(key: string): string {
  return key
    .split(/[-_]/)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ")
}

