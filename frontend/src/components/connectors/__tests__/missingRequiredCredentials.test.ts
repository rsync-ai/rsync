import { describe, it, expect } from "vitest"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import {
  missingRequiredCredentials,
  type SupportedAuthMethod,
} from "@/lib/types/mcp-connector"

// ---------------------------------------------------------------------------
// missingRequiredCredentials IS the connection form's Test/Save gate. It answers
// one question — which credential fields may this connection not be saved
// blank? — and the answer is the connector's own `configuration_schema.required`,
// which is exactly what the orchestrator's pre-start check validates
// (backend-orchestrator/internal/mcp/server_manager.go, missingRequiredConfig).
//
// The bug this replaced: the gate demanded EVERY field the chosen auth method
// named, so a connector with a legitimate credential-less path could never be
// saved at all. mongodb against an unauthenticated deployment, gcs/bigquery via
// Application Default Credentials and azure-blob anonymous each had a Save
// button that no input could enable.
// ---------------------------------------------------------------------------

const method = (over: Partial<SupportedAuthMethod>): SupportedAuthMethod => ({
  method: "api_key",
  header_name: "",
  header_prefix: "",
  config_keys: ["api_key"],
  description: "",
  ...over,
})

const keys = (...k: string[]) => new Set(k)

describe("missingRequiredCredentials — the shapes that regressed", () => {
  it("mongodb: an unauthenticated deployment saves with user+password blank", () => {
    // metadata: basic method over [user, password], `required` names neither
    // (the description says "Omit for an unauthenticated deployment").
    expect(
      missingRequiredCredentials(
        method({ method: "basic", config_keys: ["user", "password"] }),
        keys("host", "port", "database", "user", "password"),
        ["host", "database"],
        {},
      ),
    ).toEqual([])
  })

  it("aws-s3: BOTH required secrets still block Save when blank", () => {
    const m = method({ config_keys: ["access_key_id", "secret_access_key"] })
    const schema = keys("bucket", "region", "access_key_id", "secret_access_key")
    const required = ["bucket", "access_key_id", "secret_access_key"]

    expect(missingRequiredCredentials(m, schema, required, {})).toEqual([
      "access_key_id",
      "secret_access_key",
    ])
    // The original defect this gate was widened to catch: a saved connection
    // with the second secret empty.
    expect(
      missingRequiredCredentials(m, schema, required, { access_key_id: "AKIA…" }),
    ).toEqual(["secret_access_key"])
    expect(
      missingRequiredCredentials(m, schema, required, {
        access_key_id: "AKIA…",
        secret_access_key: "s3cr3t",
      }),
    ).toEqual([])
  })

  it("clickhouse: a password-less user saves; a blank user does not", () => {
    // metadata: basic over [user, password]; only `user` is required.
    const m = method({ method: "basic", config_keys: ["user", "password"] })
    const schema = keys("host", "port", "database", "user", "password")
    const required = ["host", "database", "user"]

    expect(missingRequiredCredentials(m, schema, required, { user: "default" })).toEqual([])
    expect(missingRequiredCredentials(m, schema, required, {})).toEqual(["user"])
  })

  it("stripe: a secret key is required, and its aliases are not separate fields", () => {
    // config_keys lists four accepted spellings of ONE secret; only
    // access_token is a schema property, so only it is a rendered field.
    const m = method({
      method: "bearer",
      config_keys: ["access_token", "token", "secret_key", "api_key"],
    })
    const schema = keys("base_url", "access_token")
    expect(missingRequiredCredentials(m, schema, ["access_token"], {})).toEqual([
      "access_token",
    ])
    expect(
      missingRequiredCredentials(m, schema, ["access_token"], { access_token: "sk_live_…" }),
    ).toEqual([])
  })

  it("a whitespace-only value is blank", () => {
    expect(
      missingRequiredCredentials(
        method({ config_keys: ["api_key"] }),
        keys("api_key"),
        ["api_key"],
        { api_key: "   " },
      ),
    ).toEqual(["api_key"])
  })

  it("oauth2 renders no credential fields, so it gates on none", () => {
    expect(
      missingRequiredCredentials(
        method({ method: "oauth2", config_keys: ["access_token"] }),
        keys("access_token"),
        ["access_token"],
        {},
      ),
    ).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// Bind the gate to what actually ships. A hand-written fixture drifts from the
// metadata; this reads the metadata. For every non-oauth method on disk the gate
// must block a blank save UNLESS the method declares `credentials_optional`,
// which is the same contract llm-service/tests/test_connector_auth_contract.py
// enforces from the Python side. A future connector — generated or hand-written
// — is covered here the moment its metadata.json lands.
// ---------------------------------------------------------------------------

const CONNECTORS_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../../../shared/mcp-connectors/public",
)

type Shipped = {
  id: string
  methods: SupportedAuthMethod[]
  schemaKeys: Set<string>
  required: string[]
}

function loadShippedConnectors(): Shipped[] {
  const out: Shipped[] = []
  const walk = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue
      const child = path.join(dir, entry.name)
      const latest = path.join(child, "latest.json")
      if (fs.existsSync(latest)) {
        const cv = JSON.parse(fs.readFileSync(latest, "utf8")).current_version
        const meta = path.join(child, "versions", cv, "metadata.json")
        if (!fs.existsSync(meta)) continue
        const m = JSON.parse(fs.readFileSync(meta, "utf8"))
        const schema = m.config_schema || m.configuration_schema || {}
        out.push({
          id: m.id || entry.name,
          methods: (m.supported_auth_methods || []) as SupportedAuthMethod[],
          schemaKeys: new Set(Object.keys(schema.properties || {}).map((k) => k.toLowerCase())),
          required: (schema.required || []) as string[],
        })
        continue
      }
      walk(child)
    }
  }
  if (fs.existsSync(CONNECTORS_ROOT)) walk(CONNECTORS_ROOT)
  return out.sort((a, b) => a.id.localeCompare(b.id))
}

const SHIPPED = loadShippedConnectors()

describe("missingRequiredCredentials — every shipped connector", () => {
  it("discovers a real corpus (an empty one would pass vacuously)", () => {
    const gated = SHIPPED.flatMap((c) =>
      c.methods.filter((m) => m.method !== "oauth2" && (m.method as string) !== "oauth"),
    )
    expect(SHIPPED.length).toBeGreaterThanOrEqual(10)
    expect(gated.length).toBeGreaterThanOrEqual(10)
  })

  it("blocks a blank save unless the method declares credentials_optional", () => {
    const wrong: string[] = []
    for (const c of SHIPPED) {
      c.methods.forEach((m, i) => {
        if (m.method === "oauth2" || (m.method as string) === "oauth") return
        const blocked = missingRequiredCredentials(m, c.schemaKeys, c.required, {}).length > 0
        const optional = m.credentials_optional === true
        if (blocked === optional) {
          wrong.push(
            `${c.id}.supported_auth_methods[${i}] (${m.method}): ` +
              `blank save is ${blocked ? "blocked" : "allowed"} but ` +
              `credentials_optional is ${optional}`,
          )
        }
      })
    }
    expect(wrong).toEqual([])
  })
})
