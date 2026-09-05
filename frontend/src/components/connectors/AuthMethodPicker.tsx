"use client"

import { useEffect, useState } from "react"
import { Eye, EyeOff, Lock } from "lucide-react"
import { cn } from "@/lib/utils"
import { type SupportedAuthMethod, splitMethodCredentialKeys } from "@/lib/types/mcp-connector"

interface Props {
  /** Auth methods declared by the connector's metadata. */
  methods: SupportedAuthMethod[]
  /** Currently selected auth_method (snake_case identifier from the picker). */
  selectedMethod: string
  /** Called when user picks a different method. */
  onMethodChange: (method: string) => void
  /** Connection-time credential values keyed by config_key (e.g. access_token). */
  values: Record<string, string>
  /** Called when a credential field changes. */
  onValueChange: (key: string, value: string) => void
  /**
   * Connector-level OAuth provider id (connector.oauth_provider), used as the
   * fallback when a method doesn't carry its own `oauth_provider`. The oauth2
   * method NEVER renders a manual paste field regardless of this prop (the
   * Connect button owns it); the provider only decides whether we point the
   * user at Connect (provider resolved) or warn that OAuth isn't configured.
   */
  oauthProvider?: string | null
  /**
   * Lowercased keys of the connector's `configuration_schema.properties`. Used
   * to tell a method's DISTINCT credential fields (declared in the schema, e.g.
   * aws-s3 access_key_id + secret_access_key) from ALIAS key names for one secret
   * (e.g. shopify access_token / token). Defaults to empty (classic single-field
   * behaviour) when omitted.
   */
  schemaKeys?: Set<string>
}

/**
 * Renders an auth-method dropdown plus the credential fields for whichever
 * method the user picked. When only one method is declared the dropdown is
 * hidden — the form is just the credential fields for that method.
 *
 * The component is purely presentational; the parent form is responsible
 * for including `auth_method` + the chosen method's credentials in the
 * connection's config dict on save.
 */
export function AuthMethodPicker({
  methods,
  selectedMethod,
  onMethodChange,
  values,
  onValueChange,
  oauthProvider,
  schemaKeys,
}: Props) {
  // Auto-pick the first method when nothing is selected
  useEffect(() => {
    if (!selectedMethod && methods.length > 0) {
      onMethodChange(methods[0].method)
    }
  }, [methods, selectedMethod, onMethodChange])

  const active = methods.find((m) => m.method === selectedMethod) || methods[0]
  if (!active) return null

  // An `oauth2` method is ALWAYS acquired via the OAuth Connect button (rendered
  // by the parent form), never by pasting a token here. Derive this from the
  // method id — NOT from the provider prop. The old `!!oauthProvider && ...`
  // gate meant a multi-auth connector with an oauth2 method but no registered
  // provider fell through to a free-text `oauth_token_id` paste box (the
  // footgun): the user pasted a token that the runtime never used, and no
  // Connect button appeared. Now oauth2 renders no credential field regardless;
  // when a provider resolves we point at Connect, otherwise we say it's not
  // configured. Per-method provider (the pipedrive-gap fix) wins over the
  // connector-level prop.
  const isOauth2 = active.method === "oauth2" || active.method === "oauth"
  const activeProvider = active.oauth_provider || oauthProvider || null

  // Distinct credential fields vs alias key-names are disambiguated centrally
  // from the connector's configuration_schema (see splitMethodCredentialKeys):
  // basic → all keys; oauth2 → none (Connect owns it); single-key methods →
  // first field + alias names; multi-secret methods (aws-s3 access_key_id +
  // secret_access_key) → every distinct field, so the secret is no longer
  // dropped. `isOauth2` is still used below for the Connect/no-provider notices.
  const { fields: renderedKeys, aliases: aliasKeys } = splitMethodCredentialKeys(
    active,
    schemaKeys ?? new Set<string>(),
  )

  return (
    <div className="space-y-3" data-testid="auth-method-picker">
      {methods.length > 1 ? (
        <div>
          <label
            htmlFor="auth-method-select"
            className="block text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1"
          >
            Authentication method
          </label>
          <select
            id="auth-method-select"
            data-testid="auth-method-select"
            value={selectedMethod}
            onChange={(e) => onMethodChange(e.target.value)}
            className="w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700 bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500"
          >
            {methods.map((m) => (
              <option key={m.method} value={m.method}>
                {labelFor(m)}
              </option>
            ))}
          </select>
          {active.description && (
            <div className="mt-1 text-[11px] text-zinc-500 dark:text-zinc-400">
              {active.description}
            </div>
          )}
        </div>
      ) : (
        // Single-method case: still tell the user what auth they're using so
        // there's no ambiguity ("is multi-auth working?").
        <div
          data-testid="auth-method-single"
          className="text-[11px] text-zinc-500 dark:text-zinc-400"
        >
          Method: <span className="font-medium text-zinc-700 dark:text-zinc-300">{labelFor(active)}</span>
          {active.description ? ` — ${active.description}` : ""}
        </div>
      )}

      {/* Credential fields for the chosen method */}
      <div className="space-y-2">
        {renderedKeys.map((key) => (
          <CredentialField
            key={key}
            name={key}
            value={values[key] || ""}
            secret={!isUsernameField(key)}
            onChange={(v) => onValueChange(key, v)}
            helpText={
              aliasKeys.length > 0 && key === renderedKeys[0]
                ? `Also accepted under: ${aliasKeys.join(", ")}`
                : undefined
            }
          />
        ))}
        {renderedKeys.length === 0 && isOauth2 && activeProvider && (
          <div className="rounded-md border border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-900/20 p-3 text-xs text-amber-800 dark:text-amber-200">
            Use the <span className="font-medium">Connect with OAuth</span> button above to authorize. Credentials are stored via the OAuth flow — no manual input needed here.
          </div>
        )}
        {renderedKeys.length === 0 && isOauth2 && !activeProvider && (
          <div
            data-testid="oauth-not-configured"
            className="rounded-md border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-3 text-xs text-red-800 dark:text-red-200"
          >
            OAuth isn&apos;t configured for this connector — no OAuth provider is registered, so
            this method can&apos;t be used. Pick a different authentication method above.
          </div>
        )}
        {renderedKeys.length === 0 && !isOauth2 && (
          <div className="text-xs text-zinc-500 italic">
            This auth method has no credential fields (likely public/no-auth API).
          </div>
        )}
      </div>
    </div>
  )
}

function CredentialField({
  name,
  value,
  secret,
  onChange,
  helpText,
}: {
  name: string
  value: string
  secret: boolean
  onChange: (v: string) => void
  helpText?: string
}) {
  const [reveal, setReveal] = useState(false)
  const labelText = humanLabel(name)
  return (
    <div>
      <label
        htmlFor={`credential-${name}`}
        className="flex items-center gap-1.5 text-xs font-medium text-zinc-700 dark:text-zinc-300 mb-1"
      >
        {secret && <Lock className="h-3 w-3 text-zinc-400" />}
        {labelText}
      </label>
      <div className="relative">
        <input
          id={`credential-${name}`}
          data-testid={`credential-${name}`}
          type={secret && !reveal ? "password" : "text"}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          // Never use bullet dots as a secret placeholder — an empty field then
          // looks identical to a saved/masked value. Use an explicit hint.
          placeholder={`Enter ${labelText}`}
          className={cn(
            "w-full text-sm px-3 py-2 rounded-md border border-zinc-300 dark:border-zinc-700",
            "bg-white dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-violet-500",
            secret && "pr-9 font-mono",
          )}
          // "new-password" (not "off") is the only value Chrome honors for a
          // password field — it stops the saved localhost login from being
          // autofilled into the credential field (and the email into the
          // adjacent Description). Mirrors GenericConnectorForm's secret guard.
          autoComplete={secret ? "new-password" : "off"}
        />
        {secret && (
          <button
            type="button"
            onClick={() => setReveal(!reveal)}
            className="absolute right-2 top-1/2 -translate-y-1/2 p-1 text-zinc-400 hover:text-zinc-600"
            tabIndex={-1}
            aria-label={reveal ? `Hide ${labelText}` : `Show ${labelText}`}
          >
            {reveal ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
          </button>
        )}
      </div>
      {helpText && (
        <div className="mt-1 text-[11px] text-zinc-500 dark:text-zinc-400">{helpText}</div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------

const METHOD_LABELS: Record<string, string> = {
  bearer: "Bearer token",
  oauth2: "OAuth 2.0",
  oauth: "OAuth 2.0",
  api_key: "API key",
  api_key_query: "API key (query parameter)",
  basic: "Basic auth (username + password)",
  custom_header: "Custom header token",
  none: "No authentication",
}

function labelFor(m: SupportedAuthMethod): string {
  const base = METHOD_LABELS[m.method] || m.method
  if (m.method === "custom_header" && m.header_name) {
    return `${base} (${m.header_name})`
  }
  return base
}

function humanLabel(snakeKey: string): string {
  return snakeKey
    .split("_")
    .map((s) => s.charAt(0).toUpperCase() + s.slice(1))
    .join(" ")
}

/** Treat fields whose name suggests a username/identifier as non-secret. */
function isUsernameField(key: string): boolean {
  const k = key.toLowerCase()
  return k === "username" || k === "user" || k === "email" || k === "shop" || k === "subdomain"
}

