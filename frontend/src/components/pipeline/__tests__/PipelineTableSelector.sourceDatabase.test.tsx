import { describe, it, expect, vi, beforeAll, beforeEach, type Mock } from "vitest"
import { act, render, screen, within, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

// Issue #14: the table picker listed tables but never said which database (or
// which connection) they came from. With one MongoDB connection per database a
// wrong pick means syncing the wrong data. These tests pin the picker header:
// the named database, one derived from the tables, several listed when the
// tables span more than one, and the connection's name.

const json = (status: number, data: unknown) => ({ ok: status >= 200 && status < 300, status, json: async () => data })

vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import {
  PipelineTableSelector,
  describeTableSource,
  tableNamespaceNoun,
  type AvailableTable,
} from "../PipelineTableSelector"

import { primeNamespaceModels } from "@/lib/pipeline/namespaceModel"
import { repoNamespaceModels } from "@/lib/pipeline/__tests__/repoNamespaceModels"

// Connector types resolve through the namespace models the repo's metadata
// declares, as they do once the page has fetched them from the gateway.
beforeAll(() => primeNamespaceModels(repoNamespaceModels()))

// Shapes as the executor sends them: MongoDB collections carry the database as
// `schema`; PostgreSQL tables carry the PG schema; MySQL tables carry the database.
const mongoCollections: AvailableTable[] = [
  { name: "orders", schema: "orders_db", row_count: 1200, columns: 2 },
  { name: "customers", schema: "orders_db", row_count: 0, columns: 1 },
]
const postgresTables: AvailableTable[] = [
  { name: "users", schema: "public", row_count: 10, columns: 2 },
  { name: "invoices", schema: "sales", row_count: 42, columns: 1 },
]
const mysqlTwoDatabases: AvailableTable[] = [
  { name: "users", schema: "shop", row_count: 5, columns: 3 },
  { name: "events", schema: "metrics", row_count: 9, columns: 4 },
]

// Route authFetch by URL: GET /connections/<id> answers with a connection record
// (or `connectionStatus`), everything else is an empty 200.
let connectionStatus = 200
let connectionRecordName = "Orders prod (Mongo)"
function routeAuthFetch() {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    if (/\/api\/v1\/connections\/conn-orders$/.test(String(url))) {
      return connectionStatus === 200
        ? json(200, { id: "conn-orders", name: connectionRecordName, type: "source", connector_type: "mongodb" })
        : json(connectionStatus, { error: "Connection not found" })
    }
    return json(200, {})
  })
}

function renderSelector(props: Record<string, unknown>) {
  return render(
    <PipelineTableSelector
      isOpen
      onClose={() => {}}
      pipelineId="pl_src_db"
      availableTables={[] as never}
      // Present (even empty) => the caller owns suggestions => no self-fetch polling.
      suggestedTables={[]}
      {...props}
    />
  )
}

function header() {
  return screen.getByTestId("table-source-header")
}

describe("describeTableSource", () => {
  it("present: uses the database the backend named", () => {
    const d = describeTableSource({ sourceDatabase: "orders_db", sourceType: "mongodb", tables: mongoCollections })
    expect(d).toMatchObject({ kind: "given", heading: "Tables in orders_db", noun: "database" })
  })

  it("present: a PostgreSQL database with tables in two schemas is still one database", () => {
    const d = describeTableSource({ sourceDatabase: "appdb", sourceType: "postgresql", tables: postgresTables })
    expect(d).toMatchObject({ kind: "given", heading: "Tables in appdb" })
    expect(d.namespaces).toEqual(["public", "sales"])
  })

  it("derived: no database named, but every collection shares one", () => {
    const d = describeTableSource({ sourceType: "mongodb", tables: mongoCollections })
    expect(d).toMatchObject({ kind: "derived", heading: "Tables in orders_db" })
  })

  it("derived: a single PostgreSQL schema is labelled as a schema, not a database", () => {
    const d = describeTableSource({
      sourceDatabase: "  ",
      sourceType: "postgresql",
      tables: [{ name: "users", schema: " public " }, { name: "orders", schema: "public" }],
    })
    expect(d).toMatchObject({ kind: "derived", heading: "Tables in schema public" })
  })

  it("mixed: tables from two databases are listed, none is picked", () => {
    const d = describeTableSource({ sourceType: "mysql", tables: mysqlTwoDatabases })
    expect(d).toMatchObject({ kind: "mixed", heading: "Tables in 2 databases" })
    expect(d.namespaces).toEqual(["metrics", "shop"])
  })

  it("mixed: a named database does not hide tables that come from another database", () => {
    const d = describeTableSource({ sourceDatabase: "shop", sourceType: "mysql", tables: mysqlTwoDatabases })
    expect(d.kind).toBe("mixed")
    expect(d.heading).toBe("Tables in 2 databases")
  })

  it("mixed: several schemas with no database named", () => {
    const d = describeTableSource({ sourceType: "postgresql", tables: postgresTables })
    expect(d).toMatchObject({ kind: "mixed", heading: "Tables in 2 schemas" })
  })

  it("the tables' own database wins over a different named one", () => {
    const d = describeTableSource({ sourceDatabase: "old_db", sourceType: "mongodb", tables: mongoCollections })
    expect(d).toMatchObject({ kind: "derived", heading: "Tables in orders_db" })
  })

  it("unknown: nothing to go on gives no heading", () => {
    const d = describeTableSource({ sourceType: "stripe", tables: [{ name: "charges" }] })
    expect(d).toMatchObject({ kind: "unknown", heading: "" })
    expect(d.namespaces).toEqual([])
  })

  it("names the namespace unit per source", () => {
    expect(tableNamespaceNoun("mongodb")).toBe("database")
    expect(tableNamespaceNoun("mariadb")).toBe("database")
    expect(tableNamespaceNoun("clickhouse")).toBe("database")
    expect(tableNamespaceNoun("mysql")).toBe("database")
    expect(tableNamespaceNoun("bigquery")).toBe("dataset")
    expect(tableNamespaceNoun("postgresql")).toBe("schema")
    expect(tableNamespaceNoun(undefined)).toBe("schema")
  })

  it("names the namespace unit whatever the case of the source type", () => {
    expect(tableNamespaceNoun("MongoDB")).toBe("database")
    expect(tableNamespaceNoun("MySQL")).toBe("database")
    expect(tableNamespaceNoun("BigQuery")).toBe("dataset")
    expect(describeTableSource({ sourceType: "MongoDB", tables: mysqlTwoDatabases }).heading).toBe("Tables in 2 databases")
  })

  it("no source type: the heading names the one namespace without calling it a schema", () => {
    const d = describeTableSource({ tables: mongoCollections })
    expect(d).toMatchObject({ kind: "derived", heading: "Tables in orders_db", unit: "database or schema" })
    expect(describeTableSource({ sourceType: " ", tables: mongoCollections }).heading).toBe("Tables in orders_db")
  })

  it("no source type: several namespaces are counted without guessing the word", () => {
    const d = describeTableSource({ tables: mysqlTwoDatabases })
    expect(d).toMatchObject({ kind: "mixed", heading: "Tables in 2 databases or schemas", unit: "database or schema" })
  })

  it("a known source type keeps its own word", () => {
    expect(describeTableSource({ sourceType: "mongodb", tables: mysqlTwoDatabases }).unit).toBe("database")
    expect(describeTableSource({ sourceType: "postgresql", tables: postgresTables }).unit).toBe("schema")
  })
})

describe("PipelineTableSelector source header", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    connectionStatus = 200
    connectionRecordName = "Orders prod (Mongo)"
    routeAuthFetch()
  })

  it("present: shows the named database and the connection's name", async () => {
    renderSelector({
      sourceType: "mongodb",
      sourceDatabase: "orders_db",
      sourceConnectionId: "conn-orders",
      availableTables: mongoCollections,
    })

    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in orders_db")
    await waitFor(() =>
      expect(within(header()).getByTestId("table-source-connection")).toHaveTextContent("Connection: Orders prod (Mongo)")
    )
    const connCalls = (authFetch as Mock).mock.calls.filter(([url]) => /\/api\/v1\/connections\/conn-orders$/.test(String(url)))
    expect(connCalls.length).toBeGreaterThan(0)
    // Control: the table list itself is unchanged.
    expect(screen.getByText("orders_db.orders")).toBeInTheDocument()
    expect(screen.getByText("orders_db.customers")).toBeInTheDocument()
    expect(within(header()).getByText("2 tables found")).toBeInTheDocument()
    expect(screen.queryByTestId("table-source-namespaces")).not.toBeInTheDocument()
  })

  it("derived: names the one database every collection shares when none was sent", async () => {
    renderSelector({ sourceType: "mongodb", availableTables: mongoCollections })

    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in orders_db")
    expect(within(header()).queryByText(/^Source:/)).not.toBeInTheDocument()
  })

  it("mixed: lists every database instead of guessing one", async () => {
    renderSelector({ sourceType: "mysql", sourceDatabase: "shop", availableTables: mysqlTwoDatabases })

    const heading = within(header()).getByTestId("table-source-heading")
    expect(heading).toHaveTextContent("Tables in 2 databases")
    expect(heading).not.toHaveTextContent("Tables in shop")
    const listed = within(header()).getByTestId("table-source-namespaces")
    expect(listed).toHaveTextContent("more than one database")
    expect(within(listed).getByText("metrics")).toBeInTheDocument()
    expect(within(listed).getByText("shop")).toBeInTheDocument()
    // Rows keep their database prefix so each one can be told apart.
    expect(screen.getByText("metrics.events")).toBeInTheDocument()
    expect(screen.getByText("shop.users")).toBeInTheDocument()
  })

  it("mixed: caps the listed databases and counts the rest", () => {
    const many: AvailableTable[] = Array.from({ length: 8 }, (_, i) => ({ name: "t", schema: `db${i}` }))
    renderSelector({ sourceType: "mongodb", availableTables: many })

    const listed = within(header()).getByTestId("table-source-namespaces")
    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in 8 databases")
    expect(within(listed).getByText("db5")).toBeInTheDocument()
    expect(within(listed).queryByText("db6")).not.toBeInTheDocument()
    expect(within(listed).getByText("+2 more")).toBeInTheDocument()
  })

  it("a connection that cannot be read leaves the name out and keeps the database", async () => {
    connectionStatus = 404
    renderSelector({
      sourceType: "mongodb",
      sourceDatabase: "orders_db",
      sourceConnectionId: "conn-orders",
      availableTables: mongoCollections,
    })

    await waitFor(() =>
      expect(
        (authFetch as Mock).mock.calls.some(([url]) => /\/api\/v1\/connections\/conn-orders$/.test(String(url)))
      ).toBe(true)
    )
    // Let the failed response settle before asserting nothing was shown.
    await act(async () => {
      await new Promise((r) => setTimeout(r, 20))
    })
    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in orders_db")
    expect(within(header()).getByTestId("table-source-connection")).not.toHaveTextContent("Connection:")
  })

  it("switching to another connection never shows the previous connection's name", async () => {
    const props = {
      sourceType: "mongodb",
      sourceDatabase: "orders_db",
      availableTables: mongoCollections,
    }
    const view = renderSelector({ ...props, sourceConnectionId: "conn-orders" })
    await waitFor(() =>
      expect(within(header()).getByTestId("table-source-connection")).toHaveTextContent("Connection: Orders prod (Mongo)")
    )

    // The next wait comes from a different connection whose name cannot be read.
    view.rerender(
      <PipelineTableSelector
        isOpen
        onClose={() => {}}
        pipelineId="pl_src_db"
        suggestedTables={[]}
        {...props}
        sourceConnectionId="conn-billing"
      />
    )
    await waitFor(() =>
      expect(
        (authFetch as Mock).mock.calls.some(([url]) => /\/api\/v1\/connections\/conn-billing$/.test(String(url)))
      ).toBe(true)
    )
    await act(async () => {
      await new Promise((r) => setTimeout(r, 20))
    })
    expect(within(header()).getByTestId("table-source-connection")).not.toHaveTextContent("Orders prod (Mongo)")
    expect(within(header()).getByTestId("table-source-connection")).not.toHaveTextContent("Connection:")
  })

  it("a named database gets a header even with no source type and no connection", () => {
    renderSelector({ sourceDatabase: "appdb", availableTables: [{ name: "users" }, { name: "orders" }] })

    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in appdb")
    expect(within(header()).queryByTestId("table-source-connection")).not.toBeInTheDocument()
  })

  it("names the source type under the heading when no connection name is known", () => {
    renderSelector({ sourceType: "postgresql", sourceDatabase: "appdb", availableTables: postgresTables })

    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in appdb")
    expect(within(header()).getByTestId("table-source-connection").textContent).toBe("PostgreSQL")
  })

  it("shows the connection name without the spaces around it", async () => {
    connectionRecordName = "  Orders prod (Mongo)  "
    renderSelector({
      sourceType: "mongodb",
      sourceDatabase: "orders_db",
      sourceConnectionId: "conn-orders",
      availableTables: mongoCollections,
    })

    const name = await within(header()).findByText("Orders prod (Mongo)")
    expect(name.textContent).toBe("Orders prod (Mongo)")
  })

  it("an empty connection id asks the server for nothing", async () => {
    renderSelector({ sourceType: "mongodb", sourceConnectionId: "", availableTables: mongoCollections })

    await act(async () => {
      await new Promise((r) => setTimeout(r, 20))
    })
    const connectionCalls = (authFetch as Mock).mock.calls.filter(([url]) => /\/api\/v1\/connections/.test(String(url)))
    expect(connectionCalls).toEqual([])
    // Control: the header is still there.
    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in orders_db")
  })

  it("mixed: exactly as many databases as fit are all listed with no overflow count", () => {
    const six: AvailableTable[] = Array.from({ length: 6 }, (_, i) => ({ name: "t", schema: `db${i}` }))
    renderSelector({ sourceType: "mongodb", availableTables: six })

    const listed = within(header()).getByTestId("table-source-namespaces")
    expect(within(listed).getByText("db5")).toBeInTheDocument()
    expect(within(listed).queryByText(/more$/)).not.toBeInTheDocument()
  })

  it("mixed: one database over the limit is counted", () => {
    const seven: AvailableTable[] = Array.from({ length: 7 }, (_, i) => ({ name: "t", schema: `db${i}` }))
    renderSelector({ sourceType: "mongodb", availableTables: seven })

    const listed = within(header()).getByTestId("table-source-namespaces")
    expect(within(listed).queryByText("db6")).not.toBeInTheDocument()
    expect(within(listed).getByText("+1 more")).toBeInTheDocument()
  })

  it("mixed with no source type: warns without calling them schemas", () => {
    renderSelector({ availableTables: mysqlTwoDatabases })

    expect(within(header()).getByTestId("table-source-heading")).toHaveTextContent("Tables in 2 databases or schemas")
    expect(within(header()).getByTestId("table-source-namespaces")).toHaveTextContent(
      "These tables come from more than one database or schema. Each name below starts with its database or schema"
    )
  })

  it("no tables found: the message names the database that was searched", async () => {
    renderSelector({
      sourceType: "postgresql",
      sourceDatabase: " appdb ",
      sourceConnectionId: "conn-orders",
      availableTables: [],
    })

    expect(await screen.findByText(/^No tables found in "appdb"\./)).toBeInTheDocument()
    // Control: discovery really ran against the connection and came back empty.
    expect(
      (authFetch as Mock).mock.calls.some(([url]) => /\/api\/v1\/connections\/conn-orders\/metadata\?limit=5000$/.test(String(url)))
    ).toBe(true)
  })

  it("no tables found and no database named: the message falls back to the source type", async () => {
    renderSelector({ sourceType: "postgresql", sourceConnectionId: "conn-orders", availableTables: [] })

    expect(await screen.findByText(/^No tables found in this PostgreSQL database\./)).toBeInTheDocument()
  })

  it("an empty discovery the backend reported names the database, trimmed", () => {
    renderSelector({ sourceType: "postgresql", sourceDatabase: " appdb ", discoveryStatus: "empty", availableTables: [] })

    expect(screen.getByText(/^"appdb" has no tables\./)).toBeInTheDocument()
  })

  it("unknown: falls back to the source type", () => {
    renderSelector({ sourceType: "stripe", availableTables: [{ name: "charges" }] })

    expect(within(header()).queryByTestId("table-source-heading")).not.toBeInTheDocument()
    expect(within(header()).getByText("Source:")).toBeInTheDocument()
    expect(within(header()).getByText("Stripe")).toBeInTheDocument()
  })
})
