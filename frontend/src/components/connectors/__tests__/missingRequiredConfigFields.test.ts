import { describe, it, expect } from "vitest"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { missingRequiredConfigFields } from "@/lib/types/mcp-connector"

// ---------------------------------------------------------------------------
// missingRequiredConfigFields is the second half of the connection form's Save
// gate — the non-credential half. missingRequiredCredentials answers "may this
// credential be blank?"; this answers "is this required field satisfied, here or
// by an alternative the connector accepts in its place?".
//
// The bug: it had no alternatives. The gate filtered configuration_schema.required
// on presence in formData alone, while the orchestrator's pre-start check has
// honoured config_aliases since oracle-by-dsn shipped
// (backend-orchestrator/internal/mcp/server_manager.go, missingRequiredConfig).
// That made the UI strictly stricter than the runtime it gates for, and the
// failure mode is the worst kind: the connector would have worked, the
// connection just could not be saved.
//
// MongoDB Atlas is the case that forced it. Atlas is reachable only as
// `mongodb+srv://…` in connection_string — there is no host:port to type, SRV
// resolves the seed list — and mongodb's own _build_uri returns the explicit URI
// and never reads `host`. So the form demanded a field the connector ignores,
// for a deployment that cannot supply it.
// ---------------------------------------------------------------------------

// Reads what the form actually receives. The wire's `configuration_schema` is
// disk `config_schema` (api-gateway mapToMCPConnector), NOT disk
// `configuration_schema` — the two differ, and reading the wrong one models a
// form that does not exist.
const CONNECTORS_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../../../shared/mcp-connectors/public",
)

type Shipped = {
  id: string
  required: string[]
  aliases: Record<string, string[]>
  /** Keys the form renders an input for — disk config_schema.properties. */
  renderable: Set<string>
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
        const schema = m.config_schema || {}
        out.push({
          id: m.id || entry.name,
          required: (schema.required || []) as string[],
          aliases: (m.config_aliases || {}) as Record<string, string[]>,
          renderable: new Set(Object.keys(schema.properties || {})),
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
const byId = (id: string) => {
  const c = SHIPPED.find((x) => x.id === id)
  if (!c) throw new Error(`${id} not found under ${CONNECTORS_ROOT}`)
  return c
}

/** The form's own satisfaction test: a non-blank value in formData. */
const filled = (values: Record<string, unknown>) => (field: string) =>
  Boolean(String(values[field] ?? "").trim())

describe("missingRequiredConfigFields — MongoDB Atlas, on the shipped metadata", () => {
  const mongo = byId("mongodb")

  it("the metadata still declares what this gate depends on", () => {
    // Vacuity floor. Every assertion below is about mongodb's required fields
    // and its host alias; if either vanished from the shipped file the tests
    // would keep passing while the form went back to being unsaveable.
    expect(mongo.required).toContain("host")
    // A server-level connection (#31) names no database: it reads every
    // database the login can see, narrowed by the connection's Scope.
    expect(mongo.required).not.toContain("database")
    expect(mongo.aliases.host ?? []).toContain("connection_string")
  })

  it("an Atlas SRV connection saves with no host", () => {
    expect(
      missingRequiredConfigFields(
        mongo.required,
        mongo.aliases,
        filled({
          connection_string: "mongodb+srv://u:p@cluster0.abcd.mongodb.net/?retryWrites=true",
          database: "shop",
        }),
      ),
    ).toEqual([])
  })

  it("a self-hosted connection still saves the old way, by host", () => {
    expect(
      missingRequiredConfigFields(
        mongo.required,
        mongo.aliases,
        filled({ host: "mongo.internal", database: "shop" }),
      ),
    ).toEqual([])
  })

  // --- negative controls: the gate must still gate -------------------------

  it("neither host nor any alias is still rejected", () => {
    expect(
      missingRequiredConfigFields(mongo.required, mongo.aliases, filled({ database: "shop" })),
    ).toEqual(["host"])
  })

  it("an Atlas SRV connection with no database saves as a server-level connection", () => {
    expect(
      missingRequiredConfigFields(
        mongo.required,
        mongo.aliases,
        filled({ connection_string: "mongodb+srv://u:p@cluster0.abcd.mongodb.net/" }),
      ),
    ).toEqual([])
  })

  it("a required field with NO alias is unaffected by another field's alias", () => {
    expect(
      missingRequiredConfigFields(
        ["host", "database"],
        mongo.aliases,
        filled({ connection_string: "mongodb+srv://u:p@cluster0.abcd.mongodb.net/" }),
      ),
    ).toEqual(["database"])
  })

  it("a blank alias satisfies nothing", () => {
    // Deliberately stricter than the Go gate, which is key-presence-only. An
    // empty connection_string would sail past the server and fail at connect.
    expect(
      missingRequiredConfigFields(
        mongo.required,
        mongo.aliases,
        filled({ connection_string: "   ", database: "shop" }),
      ),
    ).toEqual(["host"])
  })

  it("an unrelated key is not mistaken for an alias", () => {
    expect(
      missingRequiredConfigFields(
        mongo.required,
        mongo.aliases,
        filled({ replica_set: "rs0", database: "shop" }),
      ),
    ).toEqual(["host"])
  })

  it("no aliases at all behaves exactly as the pre-fix gate did", () => {
    expect(missingRequiredConfigFields(["host", "database"], undefined, filled({}))).toEqual([
      "host",
      "database",
    ])
  })
})

// ---------------------------------------------------------------------------
// Reachability. Honouring an alias buys nothing if the form renders no input to
// type it into: the user is left satisfying the canonical field instead, exactly
// as before. So every alias-declaring connector must leave at least one usable
// path to each aliased required field.
//
// This is a structural guard over the shipped corpus, not a claim about one
// connector — it fires on the next connector that declares config_aliases
// pointing only at keys absent from its config_schema.
// ---------------------------------------------------------------------------
describe("missingRequiredConfigFields — every shipped connector that declares aliases", () => {
  const aliasing = SHIPPED.filter((c) => Object.keys(c.aliases).length > 0)

  it("finds a real corpus (an empty one would pass vacuously)", () => {
    expect(SHIPPED.length).toBeGreaterThanOrEqual(15)
    expect(aliasing.length).toBeGreaterThanOrEqual(2)
    expect(aliasing.map((c) => c.id)).toContain("mongodb")
  })

  it("leaves every aliased required field satisfiable in the form", () => {
    const unreachable: string[] = []
    for (const c of aliasing) {
      for (const [canonical, alts] of Object.entries(c.aliases)) {
        if (!c.required.includes(canonical)) continue // alias on an optional field: nothing to gate
        const paths = [canonical, ...alts].filter((k) => c.renderable.has(k))
        if (paths.length === 0) {
          unreachable.push(
            `${c.id}: required "${canonical}" is satisfiable by none of ` +
              `[${[canonical, ...alts].join(", ")}] — config_schema renders no input for any of them, ` +
              `so this connection cannot be saved from the UI at all`,
          )
        }
      }
    }
    expect(unreachable).toEqual([])
  })

  it("mongodb's Atlas path is renderable, not just permitted", () => {
    // The specific instance of the rule above that this change exists to make
    // work end to end: gate accepts connection_string AND the form draws the box.
    expect(byId("mongodb").renderable.has("connection_string")).toBe(true)
  })
})
