import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

// The modal fires authFetch on mount (table discovery / suggestion / dismissal
// polling). Stub it so those effects no-op — this suite only exercises the
// destination-namespace gating logic, which needs no network.
const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: vi.fn(async () => jsonRes({})),
}))

import { PipelineTableSelector } from "../PipelineTableSelector"

// A selection that spans three source schemas (sales + procurement + hr), with a
// same-named table (orders) in two of them — the exact shape that used to
// flatten-and-lose rows before PR #549.
const multiSchemaTables = [
  { name: "orders", schema: "sales", row_count: 100, columns: 5 },
  { name: "orders", schema: "procurement", row_count: 80, columns: 4 },
  { name: "employees", schema: "hr", row_count: 20, columns: 6 },
]

function setup(props: Record<string, unknown> = {}) {
  const onTablesSelected = vi.fn(async () => {})
  const user = userEvent.setup({ pointerEventsCheck: 0 })
  render(
    <PipelineTableSelector
      isOpen
      onClose={() => {}}
      onTablesSelected={onTablesSelected}
      pipelineId="pl_test"
      sourceType="postgresql"
      availableTables={multiSchemaTables as never}
      // Present (even empty) ⇒ caller owns suggestions ⇒ no self-fetch polling.
      suggestedTables={[]}
      destinationType="aws-s3"
      // Mirrors the real, already-seeded pipeline from the bug report: the field
      // was mislabeled "schema" (createable) with a blank namespace, which is
      // what wedged the button. The fix must unblock even this shape.
      destinationConfig={{ namespace: "", namespace_kind: "schema", create_if_not_exists: true }}
      {...props}
    />
  )
  return { onTablesSelected, user }
}

describe("PipelineTableSelector — multi-schema destination namespace", () => {
  beforeEach(() => vi.clearAllMocks())

  it('enables "Sync entire database" with a blank namespace and preserves source schemas', async () => {
    const { onTablesSelected, user } = setup()

    // Nothing selected yet → confirm is disabled.
    expect(screen.getByRole("button", { name: /^Continue$/i })).toBeDisabled()

    // Tick "Select entire database" (whole-source sentinel).
    const wholeDbLabel = screen.getByText("Select entire database").closest("label") as HTMLElement
    await user.click(within(wholeDbLabel).getByRole("checkbox"))

    // The namespace field is now optional and a preserve hint is shown, even
    // though the field is blank. aws-s3 is a path destination, so the copy talks
    // about a folder per source schema.
    expect(screen.getByText(/\(optional\)/i)).toBeInTheDocument()
    expect(screen.getByText(/written to its own\s+folder on the destination/i)).toBeInTheDocument()

    // The button is enabled despite the blank namespace — this is the fix.
    const confirm = screen.getByRole("button", { name: /Sync entire database/i })
    expect(confirm).toBeEnabled()

    // Confirming sends the whole-source sentinel plus an explicit preserve
    // directive (blank namespace) — so each source schema is mirrored regardless
    // of the seeded destination namespace. aws-s3 resolves to a path kind (not
    // "schema"), so it is non-createable even though the persisted kind was the
    // stale "schema".
    await user.click(confirm)
    expect(onTablesSelected).toHaveBeenCalledTimes(1)
    expect(onTablesSelected).toHaveBeenCalledWith(["*"], {
      namespace: "",
      namespace_kind: "path",
      create_if_not_exists: false,
      schema_mode: "preserve",
    })
  })

  it("emits an explicit flatten directive when a name is typed for a multi-schema selection", async () => {
    // Regression for the re-review's sticky-preserve gap: typing a name for a
    // multi-schema selection means "merge into this one namespace", and must send
    // schema_mode:"flatten" so it overrides any preserve mode a prior blank run
    // stuck on the pipeline.
    const { onTablesSelected, user } = setup()

    const wholeDbLabel = screen.getByText("Select entire database").closest("label") as HTMLElement
    await user.click(within(wholeDbLabel).getByRole("checkbox"))

    await user.type(screen.getByLabelText(/Path prefix/i), "warehouse")

    await user.click(screen.getByRole("button", { name: /Sync entire database/i }))
    expect(onTablesSelected).toHaveBeenCalledWith(["*"], {
      namespace: "warehouse",
      namespace_kind: "path",
      create_if_not_exists: false,
      schema_mode: "flatten",
    })
  })

  it("does NOT wedge a single-schema selection on an existing S3 pipeline with a stale persisted kind:'schema'", async () => {
    // Regression for the reviewer's Finding 1: an already-seeded aws-s3 pipeline
    // carries namespace_kind:"schema" (seeded before aws-s3 was mapped). The
    // connector type must win → path kind → non-createable → name NOT required,
    // even for a single-schema (non-whole-DB) selection with a blank namespace.
    const { onTablesSelected, user } = setup()

    const salesOrders = screen.getByText("sales.orders").closest("label") as HTMLElement
    await user.click(within(salesOrders).getByRole("checkbox"))

    // Field is labeled a Path prefix (optional), not a required "Schema name".
    expect(screen.getByText(/Path prefix/i)).toBeInTheDocument()
    expect(screen.getByText(/\(optional\)/i)).toBeInTheDocument()

    const confirm = screen.getByRole("button", { name: /Sync 1 table/i })
    expect(confirm).toBeEnabled()

    // Single-schema + path destination + blank namespace → omit destination_config
    // (no cross-schema collision to guard against).
    await user.click(confirm)
    expect(onTablesSelected).toHaveBeenCalledWith(["sales.orders"], undefined)
  })

  it("still requires a name for a single-schema selection into a createable (schema) destination", async () => {
    const { user } = setup({ destinationType: "postgresql" })

    // Select one table in a single schema (sales.orders) — not multi-schema.
    const rows = screen.getAllByRole("checkbox")
    // Row order is alphabetical by "schema.table": hr.employees, procurement.orders, sales.orders.
    // The whole-database checkbox is the first checkbox rendered.
    const salesOrders = screen.getByText("sales.orders").closest("label") as HTMLElement
    await user.click(within(salesOrders).getByRole("checkbox"))
    expect(rows.length).toBeGreaterThan(0)

    // The always-visible header count surfaces the selection even when the
    // checked rows are scrolled out of the window.
    expect(screen.getByText(/1 selected/i)).toBeInTheDocument()

    // Clear the seeded namespace to make it blank.
    const input = screen.getByLabelText(/Schema name/i)
    await user.clear(input)

    // The single-schema help copy must read distinctly from the multi-schema
    // one AND render with correct inter-word spacing. Regression guard for the
    // JSX line-wrap-after-</span> glue bug that shipped "schemaon the
    // destination" (caught on staging, not by the earlier assertions).
    expect(
      screen.getByText(
        (_content, el) =>
          el?.tagName.toLowerCase() === "p" &&
          /creates this schema on the destination/i.test(el.textContent || ""),
      ),
    ).toBeInTheDocument()

    // Single-schema + createable destination ⇒ a name is required ⇒ disabled.
    const confirm = screen.getByRole("button", { name: /Sync 1 table/i })
    expect(confirm).toBeDisabled()
  })

  it("ties the multi-schema copy to the concrete selection count (schemas + tables)", async () => {
    // Regression for the staging confusion: the copy said "syncing tables from
    // 7 source schemas" while the checked rows were scrolled out of view, so it
    // read as a claim about a selection the user couldn't see. The copy must now
    // name the concrete table count so it's obviously tied to a real selection.
    const { user } = setup({ destinationType: "postgresql" })

    const salesOrders = screen.getByText("sales.orders").closest("label") as HTMLElement
    const hrEmployees = screen.getByText("hr.employees").closest("label") as HTMLElement
    await user.click(within(salesOrders).getByRole("checkbox"))
    await user.click(within(hrEmployees).getByRole("checkbox"))

    // Header shows the always-visible count.
    expect(screen.getByText(/2 selected/i)).toBeInTheDocument()

    // Copy names both the schema span and the concrete table count.
    expect(
      screen.getByText(
        (_content, el) =>
          el?.tagName.toLowerCase() === "p" &&
          /Your selection spans 2 source schemas \(2 tables\)/i.test(el.textContent || ""),
      ),
    ).toBeInTheDocument()
  })

  it("blanks an engine-default namespace on a multi-schema selection so it preserves (never flattens into public)", async () => {
    // Data-loss guard (PR #549/#552 regression): the field is seeded with the
    // destination engine default ("public"). Left in place for a multi-schema
    // selection it would FLATTEN every source schema into public (same-named
    // tables across schemas collide → rows lost). It must default to blank ⇒
    // preserve. A name the user actually types is still honored as "merge".
    const { onTablesSelected, user } = setup({
      destinationType: "postgresql",
      destinationConfig: { namespace: "public", namespace_kind: "schema", create_if_not_exists: true },
    })

    const salesOrders = screen.getByText("sales.orders").closest("label") as HTMLElement
    const hrEmployees = screen.getByText("hr.employees").closest("label") as HTMLElement
    await user.click(within(salesOrders).getByRole("checkbox"))
    await user.click(within(hrEmployees).getByRole("checkbox"))

    // The seeded "public" is blanked → the preserve default is shown, not a
    // flatten target.
    expect((screen.getByLabelText(/Schema name/i) as HTMLInputElement).value).toBe("")

    // Confirming sends an explicit preserve directive (mirror each source
    // schema), NOT flatten-into-public.
    await user.click(screen.getByRole("button", { name: /Sync 2 tables/i }))
    expect(onTablesSelected).toHaveBeenCalledWith(
      expect.arrayContaining(["sales.orders", "hr.employees"]),
      expect.objectContaining({ namespace: "", schema_mode: "preserve" }),
    )
  })

  it("blanks a NON-engine-default source-slug seed on multi-schema (SQL Server / Snowflake data-loss fix)", async () => {
    // The HIGH bug the corner-case probe caught: SQL Server / Snowflake / Oracle
    // / Databricks sources seed the SOURCE SLUG ("sqlserver"), which is NOT an
    // engine default — the earlier isEngineDefaultNamespace gate left it in
    // place → flatten → cross-schema collision. ANY untouched seed on a
    // multi-schema selection must blank ⇒ preserve.
    const { onTablesSelected, user } = setup({
      sourceType: "sqlserver",
      destinationType: "postgresql",
      destinationConfig: { namespace: "sqlserver", namespace_kind: "schema", create_if_not_exists: true },
    })

    const salesOrders = screen.getByText("sales.orders").closest("label") as HTMLElement
    const hrEmployees = screen.getByText("hr.employees").closest("label") as HTMLElement
    await user.click(within(salesOrders).getByRole("checkbox"))
    await user.click(within(hrEmployees).getByRole("checkbox"))

    expect((screen.getByLabelText(/Schema name/i) as HTMLInputElement).value).toBe("")
    await user.click(screen.getByRole("button", { name: /Sync 2 tables/i }))
    expect(onTablesSelected).toHaveBeenCalledWith(
      expect.arrayContaining(["sales.orders", "hr.employees"]),
      expect.objectContaining({ namespace: "", schema_mode: "preserve" }),
    )
  })

  it("still flattens when the user TYPES a name on a multi-schema selection (explicit merge)", async () => {
    const { onTablesSelected, user } = setup({
      destinationType: "postgresql",
      destinationConfig: { namespace: "public", namespace_kind: "schema", create_if_not_exists: true },
    })

    const salesOrders = screen.getByText("sales.orders").closest("label") as HTMLElement
    const hrEmployees = screen.getByText("hr.employees").closest("label") as HTMLElement
    await user.click(within(salesOrders).getByRole("checkbox"))
    await user.click(within(hrEmployees).getByRole("checkbox"))

    // Field auto-blanked; the user types a DELIBERATE target → merge (flatten).
    await user.type(screen.getByLabelText(/Schema name/i), "warehouse")
    await user.click(screen.getByRole("button", { name: /Sync 2 tables/i }))
    expect(onTablesSelected).toHaveBeenCalledWith(
      expect.arrayContaining(["sales.orders", "hr.employees"]),
      expect.objectContaining({ namespace: "warehouse", schema_mode: "flatten" }),
    )
  })

  it("restores the seeded namespace when the selection narrows back to a single schema", async () => {
    // Regression for the multi→single transition (probe L7): blanking for
    // multi-schema must not leave a single-schema selection stuck with a blank
    // required field — restore the seed so the button isn't wedged.
    const { user } = setup({
      destinationType: "postgresql",
      destinationConfig: { namespace: "public", namespace_kind: "schema", create_if_not_exists: true },
    })

    const salesOrders = screen.getByText("sales.orders").closest("label") as HTMLElement
    const hrEmployees = screen.getByText("hr.employees").closest("label") as HTMLElement
    await user.click(within(salesOrders).getByRole("checkbox"))
    await user.click(within(hrEmployees).getByRole("checkbox"))
    expect((screen.getByLabelText(/Schema name/i) as HTMLInputElement).value).toBe("") // multi → blanked

    // Deselect back to one schema → seed restored (single-schema needs a name).
    await user.click(within(hrEmployees).getByRole("checkbox"))
    expect((screen.getByLabelText(/Schema name/i) as HTMLInputElement).value).toBe("public")
  })
})
