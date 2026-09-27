/**
 * `PipelineCheckpointsCard` — the UI for GET /pipelines/:id/checkpoints, which
 * has been served and workspace-gated since migration 026 with no caller.
 *
 * Two things are guarded here.
 *
 * 1. `formatPosition` stays shape-agnostic. `position` is free-form JSONB by
 *    design — Postgres writes an LSN, MongoDB a resume token, an API connector
 *    a nested `watermark.value` — so a renderer that assumed one engine's
 *    vocabulary would print "—" for the others.
 *
 * 2. A failed read must NOT render the empty state. "No checkpoint recorded
 *    yet. A restart would start this pipeline's tables from the beginning." is
 *    a positive claim about the pipeline's resume position; saying it because
 *    an HTTP call 500'd is the F-242 failure mode (a read error that looks like
 *    a healthy empty result), and here it would tell an operator to expect a
 *    full replay that is not actually going to happen.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

import { PipelineCheckpointsCard, formatPosition } from "@/components/pipeline/PipelineCheckpointsCard"

const mockFetch = vi.fn()
vi.stubGlobal("fetch", mockFetch)

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response
}

const EMPTY_STATE = /no checkpoint recorded yet/i

function checkpoint(over: Partial<Record<string, unknown>> = {}) {
  return {
    id: "cp1",
    pipeline_id: "p1",
    connection_id: "c1",
    source_table: "public.orders",
    position: { lsn: "0/1A2B3C4" },
    created_at: "2026-09-20T10:00:00Z",
    updated_at: "2026-09-22T11:30:00Z",
    ...over,
  }
}

beforeEach(() => {
  mockFetch.mockReset()
})

describe("formatPosition", () => {
  it("flattens a nested position one level — a connector watermark", () => {
    expect(formatPosition({ watermark: { value: "2026-09-01T00:00:00Z" } })).toBe(
      "watermark.value: 2026-09-01T00:00:00Z"
    )
  })

  it("prints a flat engine position as-is — a Postgres LSN, a Mongo resume token", () => {
    expect(formatPosition({ lsn: "0/1A2B3C4" })).toBe("lsn: 0/1A2B3C4")
    expect(formatPosition({ resume_token: "82650F..." })).toBe("resume_token: 82650F...")
  })

  it("joins multiple keys rather than showing only the first", () => {
    expect(formatPosition({ lsn: "0/1A", txid: 4711 })).toBe("lsn: 0/1A · txid: 4711")
  })

  it("distinguishes an empty position object from no position at all", () => {
    // The row exists but carries nothing — a real and different state from
    // "this table has no checkpoint row".
    expect(formatPosition({})).toBe("(empty)")
    expect(formatPosition(null)).toBe("—")
    expect(formatPosition(undefined)).toBe("—")
  })

  it("labels the batch executor's row and byte counts as the whole run's, not the table's (#13)", () => {
    // executor.go stamps the run's shared running totals into every table's
    // checkpoint; a bare "rows_so_far" beside one table read as that table's.
    expect(formatPosition({ rows_so_far: 1200, bytes_so_far: 4096 })).toBe(
      "run rows so far (all tables): 1200 · run bytes so far (all tables): 4096"
    )
    // Control: engine positions keep their own names.
    expect(formatPosition({ lsn: "0/1A", txid: 4711 })).toBe("lsn: 0/1A · txid: 4711")
    expect(formatPosition({ watermark: { rows_so_far: 3 } })).toBe("watermark.rows_so_far: 3")
  })

  it("does not lose an array value to [object Object]", () => {
    expect(formatPosition({ gtids: ["a:1-5", "b:1-2"] })).toBe('gtids: ["a:1-5","b:1-2"]')
  })
})

describe("PipelineCheckpointsCard", () => {
  it("renders a row per table with its position", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        pipeline_id: "p1",
        count: 2,
        checkpoints: [
          checkpoint(),
          checkpoint({ id: "cp2", source_table: "public.customers", position: { watermark: { value: "2026-09-22" } } }),
        ],
      })
    )

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText("public.orders")).toBeInTheDocument()
    expect(screen.getByText("lsn: 0/1A2B3C4")).toBeInTheDocument()
    expect(screen.getByText("public.customers")).toBeInTheDocument()
    expect(screen.getByText("watermark.value: 2026-09-22")).toBeInTheDocument()
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
  })

  it("shows the empty state ONLY for a genuine empty list", async () => {
    mockFetch.mockResolvedValue(res(200, { pipeline_id: "p1", count: 0, checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText(EMPTY_STATE)).toBeInTheDocument()
  })

  it("on a 500 says the read failed — and does NOT claim there are no checkpoints", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "db down" }))

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText(/could not load checkpoints \(http 500\)/i)).toBeInTheDocument()
    // The assertion this whole file exists for.
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
  })

  it("on a thrown fetch says the API is unreachable — still not the empty state", async () => {
    mockFetch.mockRejectedValue(new TypeError("Failed to fetch"))

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText(/api is unreachable/i)).toBeInTheDocument()
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
  })

  it("reads the pipeline's own checkpoints endpoint", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p-42" />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    expect(String(mockFetch.mock.calls[0][0])).toContain("/api/v1/pipelines/p-42/checkpoints")
  })
})

// Only the batch export loop writes pipeline_checkpoints (executor.go
// SaveCheckpoint). On a CDC pipeline an empty list is the normal state, and the
// batch sentence "a restart would start from the beginning" is false there: the
// stream resumes from its Kafka offsets. Each CDC test has an ETL control so
// the batch wording is proven unchanged, not merely absent.
describe("PipelineCheckpointsCard — CDC pipelines", () => {
  const CDC_EMPTY = /expected for a CDC pipeline/i

  it("on a CDC pipeline an empty list is explained as expected, not as a restart from zero", async () => {
    mockFetch.mockResolvedValue(res(200, { pipeline_id: "p1", count: 0, checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText(CDC_EMPTY)).toBeInTheDocument()
    expect(screen.getByText(/does not mean a restart starts over/i)).toBeInTheDocument()
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
    expect(screen.queryByText(/start this pipeline's tables from the beginning/i)).not.toBeInTheDocument()
  })

  it("control: an ETL pipeline keeps the batch wording", async () => {
    mockFetch.mockResolvedValue(res(200, { pipeline_id: "p1", count: 0, checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="etl" />)

    expect(await screen.findByText(EMPTY_STATE)).toBeInTheDocument()
    expect(screen.getByText(/where a restart resumes from/i)).toBeInTheDocument()
    expect(screen.getByText(/whole run's running total/i)).toBeInTheDocument()
    expect(screen.queryByText(CDC_EMPTY)).not.toBeInTheDocument()
  })

  it("the CDC description says where a stream actually resumes from", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText(/written by batch runs only/i)).toBeInTheDocument()
    expect(screen.getByText(/resumes from kafka offsets/i)).toBeInTheDocument()
    expect(screen.queryByText(/where a restart resumes from/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/whole run's running total/i)).not.toBeInTheDocument()
  })

  it("a CDC pipeline that does have rows (a hybrid backfill) still shows them", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: [checkpoint()] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText("public.orders")).toBeInTheDocument()
    expect(screen.queryByText(CDC_EMPTY)).not.toBeInTheDocument()
  })

  it("a failed read on a CDC pipeline is still a failed read, not the CDC empty state", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "db down" }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText(/could not load checkpoints \(http 500\)/i)).toBeInTheDocument()
    expect(screen.queryByText(CDC_EMPTY)).not.toBeInTheDocument()
  })
})
