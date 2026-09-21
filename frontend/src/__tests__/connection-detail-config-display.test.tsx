import { Suspense } from "react"
import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { act, render, screen } from "@testing-library/react"

import ConnectionDetailPage from "@/app/(dashboard)/connections/[id]/page"
import { authFetch } from "@/lib/api/auth-fetch"

// ---------------------------------------------------------------------------
// Issue #11: the connection page's Configuration summary.
//
// It printed every stored config value as-is, so a MongoDB connection whose
// port box had been cleared (saved as `port: 0`) read "Port 0", and a MongoDB
// URI stored under `mongodb_uri` — a key the gateway does NOT mask on GET —
// was printed with its username and password. These render the real page
// against the config shapes the gateway returns and read what a user sees.
// ---------------------------------------------------------------------------

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))
// One router object for every render, as Next gives: the page's load effect
// depends on `router`, so a fresh object per render would refetch forever.
const router = vi.hoisted(() => ({ push: () => {}, refresh: () => {}, replace: () => {} }))
vi.mock("next/navigation", () => ({
  useRouter: () => router,
  useSearchParams: () => new URLSearchParams(),
  notFound: vi.fn(),
}))
vi.mock("@/components/connectors/ConnectionLogo", () => ({
  ConnectionLogo: () => <span data-testid="logo" />,
}))
vi.mock("@/components/connectors/GenericConnectorForm", () => ({
  GenericConnectorForm: () => <div data-testid="form" />,
}))
vi.mock("@/components/oauth/OAuthConnectButton", () => ({
  OAuthConnectButton: () => <button type="button">Connect</button>,
}))

const MASK = "••••••••"

function connection(connectorType: string, config: Record<string, unknown>) {
  return {
    id: "conn-1",
    name: `${connectorType} conn`,
    type: "source",
    connector_type: connectorType,
    sync_mode: "batch",
    config,
    status: "active",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
  }
}

type Row = { label: string; value: string }

async function renderSummary(conn: ReturnType<typeof connection>): Promise<Row[]> {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    if (url.includes("/api/v1/connectors/")) {
      return { ok: false, status: 404, json: async () => ({}) }
    }
    return { ok: true, status: 200, json: async () => conn }
  })
  // The page reads its route params with use(): the render has to be awaited
  // inside act so React can resume once the params promise settles.
  await act(async () => {
    render(
      <Suspense fallback={<div>loading</div>}>
        <ConnectionDetailPage params={Promise.resolve({ id: conn.id })} />
      </Suspense>,
    )
  })
  const heading = await screen.findByRole("heading", { name: "Configuration" })
  const grid = heading.nextElementSibling as HTMLElement
  const rows = Array.from(grid.children).map((row) => {
    const spans = row.querySelectorAll("span")
    return { label: spans[0]?.textContent ?? "", value: spans[1]?.textContent ?? "" }
  })
  // A summary with no rows would make every "not shown" check pass vacuously.
  expect(rows.length).toBeGreaterThan(0)
  return rows
}

// Labels are display text ("MongoDB URI Host"); the checks name them by key, in
// lower case, so a label change of case alone does not break every assertion.
const labels = (rows: Row[]) => rows.map((r) => r.label.toLowerCase())
const valueOf = (rows: Row[], label: string) =>
  rows.find((r) => r.label.toLowerCase() === label.toLowerCase())?.value

beforeEach(() => vi.clearAllMocks())

describe("connection page — port", () => {
  it("control: a normal Postgres port still shows, and its password is masked", async () => {
    const rows = await renderSummary(
      connection("postgresql", { host: "pg.internal", port: 5432, database: "app", user: "etl", password: "hunter2" }),
    )
    expect(valueOf(rows, "port")).toBe("5432")
    expect(valueOf(rows, "host")).toBe("pg.internal")
    expect(valueOf(rows, "password")).toBe(MASK)
    expect(document.body.textContent).not.toContain("hunter2")
  })

  it("MongoDB stored with port 0 shows no port row", async () => {
    const rows = await renderSummary(connection("mongodb", { host: "mongo.internal", port: 0, database: "app" }))
    expect(valueOf(rows, "host")).toBe("mongo.internal")
    expect(labels(rows)).not.toContain("port")
  })

  it("a port stored as \"0\" or empty is hidden for any connector", async () => {
    const rows = await renderSummary(
      connection("mysql", { host: "db.internal", port: "0", ssh_port: "", database: "app" }),
    )
    expect(valueOf(rows, "host")).toBe("db.internal")
    expect(labels(rows)).not.toContain("port")
    expect(labels(rows)).not.toContain("ssh port")
  })

  it("control: MongoDB by host with a real port still shows the port", async () => {
    const rows = await renderSummary(connection("mongodb", { host: "mongo.internal", port: 27017, database: "app" }))
    expect(valueOf(rows, "port")).toBe("27017")
  })
})

describe("connection page — MongoDB connection string", () => {
  it("shows the connection string host with the credentials stripped, instead of host/port", async () => {
    const rows = await renderSummary(
      connection("mongodb", {
        host: "old-host.internal",
        port: 27017,
        database: "app",
        mongodb_uri: "mongodb+srv://etl_user:S3cr3tPw@cluster0.abcd.mongodb.net/app?retryWrites=true&w=majority",
      }),
    )
    expect(valueOf(rows, "mongodb uri host")).toBe("cluster0.abcd.mongodb.net")
    expect(labels(rows)).not.toContain("host")
    expect(labels(rows)).not.toContain("port")
    expect(valueOf(rows, "database")).toBe("app")
    const text = document.body.textContent ?? ""
    expect(text).not.toContain("etl_user")
    expect(text).not.toContain("S3cr3tPw")
    expect(text).not.toContain("retryWrites")
  })

  it("an alias spelling of the MongoDB type (\"Mongo-DB_Atlas\") gets the same rule", async () => {
    const rows = await renderSummary(
      connection("Mongo-DB_Atlas", {
        host: "old-host.internal",
        port: 27017,
        database: "app",
        uri: "mongodb+srv://etl_user:S3cr3tPw@cluster1.abcd.mongodb.net/app",
      }),
    )
    expect(valueOf(rows, "uri host")).toBe("cluster1.abcd.mongodb.net")
    expect(labels(rows)).not.toContain("host")
    expect(labels(rows)).not.toContain("port")
    expect(document.body.textContent).not.toContain("S3cr3tPw")
  })

  it("lists every host of a replica-set URI", async () => {
    const rows = await renderSummary(
      connection("mongodb", {
        database: "app",
        uri: "mongodb://etl:Pw9@h1.internal:27017,h2.internal:27018/app?replicaSet=rs0",
      }),
    )
    expect(valueOf(rows, "uri host")).toBe("h1.internal:27017, h2.internal:27018")
    expect(document.body.textContent).not.toContain("Pw9")
  })

  it("the gateway-masked connection_string stays masked; host stays, the ignored port is hidden", async () => {
    const rows = await renderSummary(
      connection("mongodb", {
        host: "cluster0.abcd.mongodb.net",
        port: 27017,
        database: "app",
        user: "etl_user",
        password: MASK,
        connection_string: MASK,
      }),
    )
    expect(valueOf(rows, "connection string")).toBe(MASK)
    expect(valueOf(rows, "host")).toBe("cluster0.abcd.mongodb.net")
    expect(labels(rows)).not.toContain("port")
  })

  it("a URI whose password holds an unescaped / is masked, never cut into a fake host", async () => {
    // Cut at the first "/", this would read as host "etl", port 2718.
    const rows = await renderSummary(
      connection("mongodb", { database: "app", mongodb_uri: "mongodb://etl:2718/9xK@h1.internal/app" }),
    )
    expect(valueOf(rows, "mongodb uri")).toBe(MASK)
    const text = document.body.textContent ?? ""
    expect(text).not.toContain("2718")
    expect(text).not.toContain("9xK")
  })

  it("a URI with credentials but no host is masked, not shown as a host", async () => {
    const rows = await renderSummary(
      connection("mongodb", { database: "app", connection_string: "mongodb://etl_user:S3cr3tPw" }),
    )
    expect(valueOf(rows, "connection string")).toBe(MASK)
    expect(document.body.textContent).not.toContain("S3cr3tPw")
  })

  it.each([
    ["mongodb_uri", "mongodb://etl_user:31415?Winter@h1.internal/app", "31415", "Winter"],
    ["uri", "mongodb://etl_user:27182#Summer@h1.internal/app", "27182", "Summer"],
  ])(
    "a URI (%s) whose password holds an unescaped ? or # is masked, never cut into a fake host",
    async (key, uri, passwordStart, passwordEnd) => {
      // Cut at the "?" or "#", the authority would read "etl_user:31415" — a
      // well-formed host:port made of the username and the start of the password.
      const rows = await renderSummary(connection("mongodb", { database: "app", [key]: uri }))
      expect(valueOf(rows, key.replace(/_/g, " "))).toBe(MASK)
      const text = document.body.textContent ?? ""
      expect(text).not.toContain("etl_user")
      expect(text).not.toContain(passwordStart)
      expect(text).not.toContain(passwordEnd)
    },
  )

  it("a password holding an unescaped @ still shows only the real host", async () => {
    const rows = await renderSummary(
      connection("mongodb", { database: "app", mongodb_uri: "mongodb://etl_user:Pa@ss9@h1.internal:27017/app" }),
    )
    expect(valueOf(rows, "mongodb uri host")).toBe("h1.internal:27017")
    const text = document.body.textContent ?? ""
    expect(text).not.toContain("etl_user")
    expect(text).not.toContain("ss9")
  })

  it("a URI stored under mongodb_connection_string replaces host and port too", async () => {
    const rows = await renderSummary(
      connection("mongodb", {
        host: "old-host.internal",
        port: 27017,
        database: "app",
        mongodb_connection_string: "mongodb+srv://etl_user:S3cr3tPw@c2.abcd.mongodb.net/app",
      }),
    )
    expect(valueOf(rows, "mongodb connection string host")).toBe("c2.abcd.mongodb.net")
    expect(labels(rows)).not.toContain("host")
    expect(labels(rows)).not.toContain("port")
    expect(document.body.textContent).not.toContain("S3cr3tPw")
  })

  // The gateway stores connector_type as sent; the MongoDB metadata lists these
  // aliases, and connection validation folds them all to "mongodb".
  it.each(["mongo", "atlas", "mongodb_atlas"])(
    "connector type %s gets the MongoDB connection-string rule",
    async (connectorType) => {
      const rows = await renderSummary(
        connection(connectorType, {
          host: "old-host.internal",
          port: 27017,
          database: "app",
          uri: "mongodb+srv://etl_user:S3cr3tPw@cluster2.abcd.mongodb.net/app",
        }),
      )
      expect(valueOf(rows, "uri host")).toBe("cluster2.abcd.mongodb.net")
      expect(labels(rows)).not.toContain("host")
      expect(labels(rows)).not.toContain("port")
    },
  )

  it("control: a blank uri next to host and port is not a connection string, so the port still shows", async () => {
    // The gateway does not mask `uri`, so a box typed in and cleared arrives as
    // blank text. The connector then connects by host:port (connector.py).
    const rows = await renderSummary(
      connection("mongodb", { host: "mongo.internal", port: 27018, database: "app", uri: "   " }),
    )
    expect(valueOf(rows, "host")).toBe("mongo.internal")
    expect(valueOf(rows, "port")).toBe("27018")
  })
})

describe("connection page — secrets", () => {
  it("every kind of secret key is masked, whatever its case", async () => {
    // Mostly keys real connectors declare (access_key_id, private_key_file,
    // credentials_path, service_account_json, account_key), each hitting a
    // different rule. AuthToken is one the gateway itself does not mask (no
    // exact, suffix or substring match there), so the page is all that hides it.
    const secrets: Record<string, string> = {
      Password: "pw-value-a",
      db_passwd: "pw-value-b",
      client_secret: "pw-value-c",
      AuthToken: "tok-live-123",
      API_KEY: "pw-value-d",
      api_key_sid: "pw-value-e",
      ApiKey: "pw-value-f",
      access_key_id: "pw-value-g",
      private_key_file: "pw-value-h",
      credentials_path: "pw-value-i",
      service_account_json: "pw-value-j",
      account_key: "pw-value-k",
      ssh_key: "pw-value-l",
    }
    const rows = await renderSummary(connection("postgresql", { host: "pg.internal", ...secrets }))
    // Control: an ordinary value next to them still prints.
    expect(valueOf(rows, "host")).toBe("pg.internal")
    for (const key of Object.keys(secrets)) {
      expect({ key, value: valueOf(rows, key.replace(/_/g, " ")) }).toEqual({ key, value: MASK })
    }
    const text = document.body.textContent ?? ""
    expect(text).not.toContain("pw-value")
    expect(text).not.toContain("tok-live")
  })

  it("a secret key whose value is a URI is masked, not reduced to its host", async () => {
    const rows = await renderSummary(
      connection("postgresql", { host: "pg.internal", AuthToken: "https://svc:pw@auth.internal/issue" }),
    )
    expect(valueOf(rows, "AuthToken")).toBe(MASK)
    expect(labels(rows)).not.toContain("authtoken host")
    expect(document.body.textContent).not.toContain("auth.internal")
  })
})

describe("connection page — other connectors", () => {
  it("control: a URL with no credentials under an ordinary key prints as-is", async () => {
    const rows = await renderSummary(
      connection("aws-s3", { bucket: "b1", endpoint_url: "https://storage.example.com/bucket-a?region=eu" }),
    )
    expect(valueOf(rows, "endpoint url")).toBe("https://storage.example.com/bucket-a?region=eu")
    expect(labels(rows)).not.toContain("endpoint url host")
  })

  it("a credentialed URI under any other key shows only its host", async () => {
    const rows = await renderSummary(
      connection("postgresql", { database_url: "postgresql://etl:Pw123@pg.internal:5432/app", port: 5432 }),
    )
    expect(valueOf(rows, "database url host")).toBe("pg.internal:5432")
    expect(document.body.textContent).not.toContain("Pw123")
  })

  it("control: a non-MongoDB connector keeps its host and port next to a connection string", async () => {
    const rows = await renderSummary(
      connection("postgresql", {
        host: "pg.internal",
        port: 5432,
        connection_string: "postgresql://etl:Pw123@pg-replica.internal:5432/app",
      }),
    )
    expect(valueOf(rows, "host")).toBe("pg.internal")
    expect(valueOf(rows, "port")).toBe("5432")
    expect(valueOf(rows, "connection string host")).toBe("pg-replica.internal:5432")
    expect(document.body.textContent).not.toContain("Pw123")
  })
})

describe("connection page — config labels (#45)", () => {
  it("names run-together and acronym keys the way people write them", async () => {
    const rows = await renderSummary(
      connection("postgresql", { host: "pg.internal", sslmode: "require", api_key: "k", ssh_host: "bastion" }),
    )
    const shown = rows.map((r) => r.label)
    expect(shown).toContain("SSL Mode")
    expect(shown).toContain("API Key")
    expect(shown).toContain("SSH Host")
    expect(shown).not.toContain("Sslmode")
    expect(screen.getByText("PostgreSQL")).toBeTruthy()
  })
})

describe("connection page — status and dates (#56)", () => {
  const statusCell = () => {
    const heading = screen.getByRole("heading", { name: "Status" })
    return (heading.nextElementSibling as HTMLElement).textContent
  }

  it("says what the list says, not the raw status column", async () => {
    await renderSummary(connection("postgresql", { host: "pg.internal" }))
    expect(statusCell()).toBe("Not tested")
    expect(screen.queryByText("active")).toBeNull()
  })

  it("a stored passing test reads Connected, as on the list", async () => {
    await renderSummary({
      ...connection("postgresql", { host: "pg.internal" }),
      last_test_status: "success",
      last_tested_at: "2026-09-01T10:00:00Z",
    } as ReturnType<typeof connection>)
    expect(statusCell()).toBe("Connected")
  })

  it("a stored failed test reads Test failed, not the raw status column", async () => {
    await renderSummary({
      ...connection("postgresql", { host: "pg.internal" }),
      last_test_status: "failed",
      last_test_error: "password authentication failed",
    } as ReturnType<typeof connection>)
    expect(statusCell()).toBe("Test failed")
  })

  it("an expired token says so in the list's words", async () => {
    await renderSummary({ ...connection("hubspot", { api_key: "k" }), is_expired: true } as ReturnType<typeof connection>)
    expect(statusCell()).toBe("Token expired")
  })

  it("Created and Last Updated name their time zone, like every other absolute time", async () => {
    await renderSummary(connection("postgresql", { host: "pg.internal" }))
    const created = screen.getByRole("heading", { name: "Created" }).nextElementSibling?.textContent ?? ""
    expect(created).toMatch(/Sep 1|Aug 31/)
    expect(created).toMatch(/GMT|UTC|[A-Z]{2,4}$/)
    expect(created).not.toMatch(/^\d{1,2}\/\d{1,2}\/\d{4}/)
  })
})
