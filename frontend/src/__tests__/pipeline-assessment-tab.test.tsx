/**
 * The pipeline Assessment tab (DMS-style pre-migration checks) and the pieces
 * around it:
 *
 *   - a run blocked by a Critical check (422 "pre_migration_assessment_blocked")
 *     reaches the caller as AssessmentRequiredError, so the modal opens with
 *     the blockers instead of a bare "Execution failed" toast;
 *   - the history reads (list + one run, 404 ⇒ never assessed);
 *   - the tab: empty state → Run assessment → checks, counts, filters,
 *     remediation (SQL and mongosh commands), and older runs from history;
 *   - the tab badge (open Critical, else High);
 *   - the pre-run modal renders commands_to_run and links to the tab.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor, fireEvent, act, within } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import {
  AssessmentRequiredError,
  executePipelineWithRunMode,
  getPipelineAssessment,
  listPipelineAssessments,
  type AssessmentCheck,
  type AssessmentReport,
  type AssessmentRunSummary,
} from "@/lib/api/pipelines"
import {
  ASSESSMENT_UPDATED_EVENT,
  AssessmentTab,
  AssessmentTabBadge,
  filterChecks,
  sortChecks,
} from "@/components/pipeline/AssessmentTab"
import { PreMigrationAssessmentModal } from "@/components/pipeline/PreMigrationAssessmentModal"

const PID = "2cb685ed-4cf7-445b-9f77-071794d25423"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

const CHECKS: AssessmentCheck[] = [
  {
    code: "WAL_LEVEL",
    title: "wal_level is logical",
    category: "source",
    level: "critical",
    result: "passed",
    message: "wal_level is logical.",
  },
  {
    code: "NO_PRIMARY_KEY",
    title: "Table primary key",
    category: "tables",
    level: "high",
    result: "warning",
    message: "Reported on 2 objects — see each one below.",
    objects: [
      { name: "public.orders", message: "orders has no primary key." },
      { name: "public.events", message: "events has no primary key." },
    ],
    remediation: {
      steps: ["Add a primary key to each table."],
      sql_to_run: ["ALTER TABLE public.orders ADD PRIMARY KEY (id);"],
    },
  },
  {
    code: "MONGODB_NOT_REPLICA_SET",
    title: "Replica set",
    category: "source",
    level: "critical",
    result: "failed",
    message: "The MongoDB server is not a replica set member.",
    remediation: { commands_to_run: ["rs.initiate()"], doc_url: "https://example.test/rs" },
  },
  {
    code: "JSON_COLLAPSE",
    title: "Nested columns land as JSON",
    category: "destination",
    level: "low",
    result: "info",
    message: "2 nested columns are stored as JSON strings.",
  },
]

function report(overrides: Partial<AssessmentReport> = {}): AssessmentReport {
  return {
    blocking: true,
    summary: "1 blocking error, 1 warning",
    tables: [],
    generated_at: "2026-09-18T10:00:00Z",
    source_connector_type: "mongodb",
    destination_connector_type: "gcs",
    destination_supports_ddl: false,
    checks: CHECKS,
    counts: { critical: 1, high: 1, medium: 0, low: 0, passed: 1 },
    run_id: "aaaaaaaa-0000-0000-0000-000000000001",
    ...overrides,
  }
}

function run(id: string, overrides: Partial<AssessmentRunSummary> = {}): AssessmentRunSummary {
  return {
    id,
    trigger: "manual",
    blocking: true,
    counts: { critical: 1, high: 1, medium: 0, low: 0, passed: 1 },
    created_at: "2026-09-18T10:00:00Z",
    ...overrides,
  }
}

function url(call: unknown[]): string {
  return String(call[0])
}

beforeEach(() => {
  authFetch.mockReset()
})

describe("run gate: a blocked start opens the modal", () => {
  it("maps 422 pre_migration_assessment_blocked to AssessmentRequiredError", async () => {
    const blocked = report()
    authFetch.mockResolvedValueOnce(
      res(422, {
        error: "pre_migration_assessment_blocked",
        message: "Pre-migration assessment found blocking errors",
        assessment: blocked,
      }),
    )
    const err = await executePipelineWithRunMode(PID, "resume").catch((e) => e)
    expect(err).toBeInstanceOf(AssessmentRequiredError)
    expect((err as AssessmentRequiredError).report.blocking).toBe(true)
    expect((err as AssessmentRequiredError).report.checks).toHaveLength(4)
  })

  it("still maps the ack-required 422", async () => {
    authFetch.mockResolvedValueOnce(
      res(422, { error: "pre_migration_assessment", assessment: report({ blocking: false }) }),
    )
    await expect(executePipelineWithRunMode(PID, "resume")).rejects.toBeInstanceOf(AssessmentRequiredError)
  })

  it("leaves any other 422 as a plain error", async () => {
    authFetch.mockResolvedValueOnce(res(422, { error: "something_else", message: "nope" }))
    const err = await executePipelineWithRunMode(PID, "resume").catch((e) => e)
    expect(err).not.toBeInstanceOf(AssessmentRequiredError)
    expect(err).toBeInstanceOf(Error)
  })
})

describe("history reads", () => {
  it("reads the latest run and returns null when the pipeline was never assessed", async () => {
    authFetch.mockResolvedValueOnce(res(404, { error: "Assessment run not found" }))
    await expect(getPipelineAssessment(PID)).resolves.toBeNull()
    expect(url(authFetch.mock.calls[0])).toMatch(new RegExp(`/pipelines/${PID}/assessments/latest$`))
  })

  it("lists runs with a limit and tolerates a null list", async () => {
    authFetch.mockResolvedValueOnce(res(200, { runs: null }))
    await expect(listPipelineAssessments(PID, 1)).resolves.toEqual([])
    expect(url(authFetch.mock.calls[0])).toMatch(new RegExp(`/pipelines/${PID}/assessments\\?limit=1$`))
  })

  it("surfaces a failed read as an error, not as an empty history", async () => {
    authFetch.mockResolvedValueOnce(res(500, { error: "Failed to load assessment history" }))
    await expect(listPipelineAssessments(PID)).rejects.toThrow("Failed to load assessment history")
  })
})

describe("sorting and filtering", () => {
  it("puts open issues first, by level, and passes last", () => {
    expect(sortChecks(CHECKS).map((c) => c.code)).toEqual([
      "MONGODB_NOT_REPLICA_SET", // failed critical
      "NO_PRIMARY_KEY", // warning high
      "JSON_COLLAPSE", // note
      "WAL_LEVEL", // passed (critical nominal level) — after every issue
    ])
  })

  it("filters issues only, by level, by category and by object name", () => {
    const base = { search: "", level: "all", result: "all", category: "all" } as const
    expect(filterChecks(CHECKS, { ...base, result: "issues" }).map((c) => c.code).sort()).toEqual([
      "MONGODB_NOT_REPLICA_SET",
      "NO_PRIMARY_KEY",
    ])
    expect(filterChecks(CHECKS, { ...base, level: "critical" }).map((c) => c.code).sort()).toEqual([
      "MONGODB_NOT_REPLICA_SET",
      "WAL_LEVEL",
    ])
    expect(filterChecks(CHECKS, { ...base, category: "destination" }).map((c) => c.code)).toEqual(["JSON_COLLAPSE"])
    expect(filterChecks(CHECKS, { ...base, search: "public.events" }).map((c) => c.code)).toEqual(["NO_PRIMARY_KEY"])
  })
})

/** Routes the tab's reads: GET latest / GET one run / GET list / POST assess. */
function routeFetch(opts: {
  latest: () => { status: number; body: unknown }
  list: () => AssessmentRunSummary[]
  runs?: Record<string, { run: AssessmentRunSummary; report: AssessmentReport }>
  assess?: () => { status: number; body: unknown }
}) {
  authFetch.mockImplementation(async (u: string, init?: RequestInit) => {
    if (init?.method === "POST" && u.endsWith("/assess")) {
      const r = opts.assess?.() ?? { status: 200, body: report() }
      return res(r.status, r.body)
    }
    if (u.includes("/assessments/latest")) {
      const r = opts.latest()
      return res(r.status, r.body)
    }
    const m = u.match(/\/assessments\/([0-9a-f-]{36})$/)
    if (m) {
      const hit = opts.runs?.[m[1]]
      return hit ? res(200, hit) : res(404, { error: "Assessment run not found" })
    }
    if (u.includes("/assessments?")) return res(200, { runs: opts.list() })
    return res(404, {})
  })
}

describe("AssessmentTab", () => {
  it("offers a first run when the pipeline was never assessed, then shows the result", async () => {
    let assessed = false
    routeFetch({
      latest: () =>
        assessed
          ? { status: 200, body: { run: run("aaaaaaaa-0000-0000-0000-000000000001"), report: report() } }
          : { status: 404, body: { error: "Assessment run not found" } },
      list: () => (assessed ? [run("aaaaaaaa-0000-0000-0000-000000000001")] : []),
      assess: () => {
        assessed = true
        return { status: 200, body: report() }
      },
    })

    render(<AssessmentTab pipelineId={PID} pipelineType="cdc" />)
    expect(await screen.findByText("This pipeline has not been assessed yet.")).toBeInTheDocument()

    fireEvent.click(screen.getByRole("button", { name: /run assessment/i }))

    expect(await screen.findByText("Start blocked — fix the Critical checks below")).toBeInTheDocument()
    const posts = authFetch.mock.calls.filter((c) => (c[1] as RequestInit | undefined)?.method === "POST")
    expect(posts).toHaveLength(1)
    expect(url(posts[0])).toMatch(new RegExp(`/pipelines/${PID}/assess$`))
    expect(screen.getByText("4 of 4 checks")).toBeInTheDocument()
  })

  it("shows counts, expands a check into its fix, and filters by a count card", async () => {
    routeFetch({
      latest: () => ({ status: 200, body: { run: run("aaaaaaaa-0000-0000-0000-000000000001"), report: report() } }),
      list: () => [run("aaaaaaaa-0000-0000-0000-000000000001")],
    })
    render(<AssessmentTab pipelineId={PID} pipelineType="cdc" />)
    await screen.findByText("Start blocked — fix the Critical checks below")

    expect(screen.getByRole("button", { name: /1 critical — show these checks/i })).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /1 passed — show these checks/i })).toBeInTheDocument()
    // Passing checks are listed too (DMS shows every check it ran).
    expect(screen.getByText("wal_level is logical")).toBeInTheDocument()

    // The mongosh fix is a command, not SQL, and still reaches the reader.
    fireEvent.click(screen.getByRole("button", { name: "Show details for Replica set" }))
    expect(screen.getByText("Commands to run")).toBeInTheDocument()
    expect(screen.getByText("rs.initiate()")).toBeInTheDocument()
    expect(screen.getByRole("link", { name: /read the documentation/i })).toHaveAttribute(
      "href",
      "https://example.test/rs",
    )

    fireEvent.click(screen.getByRole("button", { name: "Show details for Table primary key" }))
    expect(screen.getByText("public.events")).toBeInTheDocument()
    expect(screen.getByText("ALTER TABLE public.orders ADD PRIMARY KEY (id);")).toBeInTheDocument()

    // The Passed card narrows the table to passes.
    fireEvent.click(screen.getByRole("button", { name: /1 passed — show these checks/i }))
    expect(screen.getByText("1 of 4 checks")).toBeInTheDocument()
    expect(screen.queryByText("Replica set")).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "Clear filters" }))
    expect(screen.getByText("4 of 4 checks")).toBeInTheDocument()
  })

  it("opens an older run from the history", async () => {
    const newest = run("aaaaaaaa-0000-0000-0000-000000000002", { created_at: "2026-09-18T12:00:00Z" })
    const older = run("aaaaaaaa-0000-0000-0000-000000000001", {
      trigger: "scheduled",
      blocking: false,
      counts: { critical: 0, high: 0, medium: 0, low: 0, passed: 1 },
    })
    routeFetch({
      latest: () => ({ status: 200, body: { run: newest, report: report() } }),
      list: () => [newest, older],
      runs: {
        [older.id]: {
          run: older,
          report: report({
            blocking: false,
            checks: [CHECKS[0]],
            counts: { critical: 0, high: 0, medium: 0, low: 0, passed: 1 },
          }),
        },
      },
    })
    render(<AssessmentTab pipelineId={PID} pipelineType="cdc" />)
    await screen.findByText("Start blocked — fix the Critical checks below")

    const history = screen.getByRole("heading", { name: "History" }).parentElement as HTMLElement
    fireEvent.click(within(history).getByText("Scheduled re-check"))

    expect(await screen.findByText("Nothing blocks the start")).toBeInTheDocument()
    expect(screen.getByText(/Older run/)).toBeInTheDocument()
    expect(url(authFetch.mock.calls.find((c) => url(c).endsWith(older.id))!)).toContain(
      `/pipelines/${PID}/assessments/${older.id}`,
    )

    fireEvent.click(screen.getByRole("button", { name: "Show the latest run" }))
    expect(await screen.findByText("Start blocked — fix the Critical checks below")).toBeInTheDocument()
  })

  it("keeps a run that failed to assess visible as an error", async () => {
    routeFetch({
      latest: () => ({ status: 404, body: { error: "Assessment run not found" } }),
      list: () => [],
      assess: () => ({ status: 500, body: { error: "assessment failed; please try again" } }),
    })
    render(<AssessmentTab pipelineId={PID} pipelineType="etl" />)
    fireEvent.click(await screen.findByRole("button", { name: /run assessment/i }))
    expect(await screen.findByText("assessment failed; please try again")).toBeInTheDocument()
  })

  it("says so when a read fails instead of showing the never-assessed state", async () => {
    routeFetch({
      latest: () => ({ status: 500, body: { error: "Failed to load assessment run" } }),
      list: () => [],
    })
    render(<AssessmentTab pipelineId={PID} pipelineType="cdc" />)
    expect(await screen.findByText("Failed to load assessment run")).toBeInTheDocument()
    expect(screen.queryByText("This pipeline has not been assessed yet.")).not.toBeInTheDocument()
  })
})

describe("AssessmentTabBadge", () => {
  it("shows open Critical issues, else High, else nothing — and re-reads on update", async () => {
    let counts = { critical: 2, high: 3, medium: 0, low: 0, passed: 4 }
    authFetch.mockImplementation(async () => res(200, { runs: [run("aaaaaaaa-0000-0000-0000-000000000001", { counts })] }))

    const { container } = render(<AssessmentTabBadge pipelineId={PID} />)
    expect(await screen.findByLabelText("2 critical")).toBeInTheDocument()
    expect(url(authFetch.mock.calls[0])).toContain("/assessments?limit=1")

    counts = { critical: 0, high: 3, medium: 0, low: 0, passed: 4 }
    act(() => {
      window.dispatchEvent(new CustomEvent(ASSESSMENT_UPDATED_EVENT, { detail: { pipelineId: PID } }))
    })
    expect(await screen.findByLabelText("3 high")).toBeInTheDocument()

    counts = { critical: 0, high: 0, medium: 1, low: 0, passed: 4 }
    act(() => {
      window.dispatchEvent(new CustomEvent(ASSESSMENT_UPDATED_EVENT, { detail: { pipelineId: PID } }))
    })
    await waitFor(() => expect(container).toBeEmptyDOMElement())
  })

  it("ignores updates for another pipeline", async () => {
    authFetch.mockResolvedValue(res(200, { runs: [] }))
    render(<AssessmentTabBadge pipelineId={PID} />)
    await waitFor(() => expect(authFetch).toHaveBeenCalledTimes(1))
    act(() => {
      window.dispatchEvent(new CustomEvent(ASSESSMENT_UPDATED_EVENT, { detail: { pipelineId: "other" } }))
    })
    expect(authFetch).toHaveBeenCalledTimes(1)
  })
})

describe("PreMigrationAssessmentModal", () => {
  it("renders commands_to_run and links to the Assessment tab", () => {
    const onOpenChange = vi.fn()
    render(
      <PreMigrationAssessmentModal
        open
        onOpenChange={onOpenChange}
        onProceed={vi.fn()}
        assessmentTabHref={`/pipelines/${PID}?tab=assessment`}
        report={report({
          tables: [
            {
              name: "(source readiness)",
              findings: [
                {
                  code: "MONGODB_NOT_REPLICA_SET",
                  severity: "error",
                  message: "The MongoDB server is not a replica set member.",
                  details: { commands_to_run: ["rs.initiate()"] },
                },
              ],
              primary_keys: [],
              primary_key_source: "declared",
              column_count: 0,
              json_column_count: 0,
              mode: "upsert",
            },
          ],
        })}
      />,
    )
    expect(screen.getByText("rs.initiate()")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Resolve errors first" })).toBeDisabled()
    const link = screen.getByRole("link", { name: /view every check in the assessment tab/i })
    expect(link).toHaveAttribute("href", `/pipelines/${PID}?tab=assessment`)
    fireEvent.click(link)
    expect(onOpenChange).toHaveBeenCalledWith(false)
  })
})
