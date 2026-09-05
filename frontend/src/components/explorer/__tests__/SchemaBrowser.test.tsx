import { describe, it, expect, vi } from "vitest"
import { render, screen, fireEvent } from "@testing-library/react"
import { SchemaBrowser } from "../SchemaBrowser"
import type { SchemaTableLike } from "@/lib/explorer/schemaTree"

const TABLES: SchemaTableLike[] = [
  {
    name: "orders",
    schema: "sales",
    row_count: 50,
    columns: [{ name: "id", is_primary_key: true }, { name: "total", type: "decimal" }],
  },
  { name: "users", schema: "sales", columns: [{ name: "id" }, { name: "email" }] },
  { name: "events", schema: "analytics", columns: [{ name: "ts", type: "timestamp" }] },
]

// Athena-style layout: a Database *dropdown* selects one namespace at a time,
// its tables are listed directly under a "Tables (N)" header, and a table
// expands to reveal its columns. Selection checkboxes + click-to-insert are
// preserved (rsync uses them for pipeline / NL→SQL table picking).
describe("SchemaBrowser (Athena layout)", () => {
  it("renders a Database selector listing every database (alphabetised)", () => {
    render(<SchemaBrowser tables={TABLES} />)
    expect(screen.getByRole("combobox", { name: "Database" })).toBeInTheDocument()
    expect(screen.getByRole("option", { name: "analytics" })).toBeInTheDocument()
    expect(screen.getByRole("option", { name: "sales" })).toBeInTheDocument()
  })

  it("shows the selected database's tables directly, under a Tables (N) header", () => {
    render(<SchemaBrowser tables={TABLES} />)
    // default = first database alphabetically (analytics) → its table is shown
    // directly, no node to expand first
    expect(screen.getByRole("button", { name: "events" })).toBeInTheDocument()
    expect(screen.getByText(/Tables \(1\)/)).toBeInTheDocument()
    // another database's tables are NOT shown until it is selected
    expect(screen.queryByRole("button", { name: "orders" })).not.toBeInTheDocument()
  })

  it("switches the table list when the Database selector changes", () => {
    render(<SchemaBrowser tables={TABLES} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    expect(screen.getByRole("button", { name: "orders" })).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "users" })).toBeInTheDocument()
    expect(screen.getByText(/Tables \(2\)/)).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "events" })).not.toBeInTheDocument()
  })

  it("expands a table to reveal its columns", () => {
    render(<SchemaBrowser tables={TABLES} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    expect(screen.getByText("total")).toBeInTheDocument()
    expect(screen.getByText("id")).toBeInTheDocument()
  })

  it("inserts the schema-qualified table name on demand", () => {
    const onInsertTable = vi.fn()
    render(<SchemaBrowser tables={TABLES} onInsertTable={onInsertTable} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.click(screen.getByRole("button", { name: "Insert table orders" }))
    expect(onInsertTable).toHaveBeenCalledWith("sales.orders")
  })

  it("inserts a column name on demand", () => {
    const onInsertColumn = vi.fn()
    render(<SchemaBrowser tables={TABLES} onInsertColumn={onInsertColumn} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.click(screen.getByRole("button", { name: "orders" }))
    fireEvent.click(screen.getByRole("button", { name: "Insert column total" }))
    expect(onInsertColumn).toHaveBeenCalledWith("total")
  })

  it("keeps a per-table selection checkbox (custom key)", () => {
    const onToggleTable = vi.fn()
    render(
      <SchemaBrowser
        tables={TABLES}
        onToggleTable={onToggleTable}
        selectionKey={(t) => `db:${t.name}`}
        selectedTables={["db:orders"]}
      />,
    )
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    expect(screen.getByRole("checkbox", { name: "Select orders" })).toBeChecked()
    fireEvent.click(screen.getByRole("checkbox", { name: "Select users" }))
    expect(onToggleTable).toHaveBeenCalledWith("db:users")
  })

  it("filters tables within the selected database", () => {
    render(<SchemaBrowser tables={TABLES} />)
    fireEvent.change(screen.getByRole("combobox", { name: "Database" }), {
      target: { value: "sales" },
    })
    fireEvent.change(screen.getByPlaceholderText(/filter/i), { target: { value: "user" } })
    expect(screen.getByRole("button", { name: "users" })).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "orders" })).not.toBeInTheDocument()
  })

  it("shows a loading state", () => {
    render(<SchemaBrowser tables={[]} loading />)
    expect(screen.getByText(/loading/i)).toBeInTheDocument()
  })

  it("shows an empty hint when there are no tables", () => {
    render(<SchemaBrowser tables={[]} emptyHint="Select a connection to browse its schema" />)
    expect(screen.getByText("Select a connection to browse its schema")).toBeInTheDocument()
  })
})
