/**
 * The batch Reload confirm dialog must describe what a reload does to THIS destination.
 *
 * Bug class: destructive-action copy written for one destination family and shown for
 * all of them. The dialog said "drops and recreates the destination tables" for every
 * pipeline, but on gcs / aws-s3 / azure-blob / minio a reload empties each table's
 * folder (`delete_prefix`) and fails the run if the folder cannot be emptied
 * completely (executor.go, `classifyReloadDeleteResult`, #1248). The CDC dialog already
 * said this; the batch one did not.
 *
 * The postgresql case is the control: a database destination keeps the drop wording.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ refresh: vi.fn(), push: vi.fn() }),
}))
vi.mock("@/lib/api/pipelines", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api/pipelines")>()
  return { ...actual, getPipeline: vi.fn(), executePipelineWithRunMode: vi.fn() }
})

import { authFetch } from "@/lib/api/auth-fetch"
import { getPipeline } from "@/lib/api/pipelines"
import { PipelineActions } from "@/components/pipeline/PipelineActions"

const mockAuthFetch = vi.mocked(authFetch)
const mockGetPipeline = vi.mocked(getPipeline)

async function openReloadDialog(destType: string) {
  mockAuthFetch.mockResolvedValue({ ok: true, json: async () => ({ status: "completed" }) } as unknown as Response)
  mockGetPipeline.mockResolvedValue({
    default_run_mode: "resume",
    destination_namespace: "lake",
    destination_connection: { connector_type: destType },
  } as unknown as Awaited<ReturnType<typeof getPipeline>>)
  render(<PipelineActions pipelineId="7a0e5c55-9d7c-4a55-9b53-5d7f1e1c0a01" status="completed" />)
  await waitFor(() => expect(mockGetPipeline).toHaveBeenCalled())
  // Let the getPipeline answer land before the dialog renders its copy.
  await waitFor(() => expect(screen.getByRole("button", { name: /reload pipeline/i })).toBeInTheDocument())
  await new Promise((r) => setTimeout(r, 0))
  await userEvent.click(screen.getByRole("button", { name: /reload pipeline/i }))
  return screen.findByRole("alertdialog")
}

describe("batch Reload confirm copy follows the destination", () => {
  beforeEach(() => vi.clearAllMocks())

  it.each(["gcs", "aws-s3", "azure-blob", "minio", "aws_s3"])(
    "%s: says the table folders are emptied, never that tables are dropped",
    async (destType) => {
      const dialog = await openReloadDialog(destType)
      expect(dialog).toHaveTextContent(/empties each table's folder/i)
      expect(dialog).toHaveTextContent(/cannot be emptied completely, the reload fails/i)
      expect(dialog).not.toHaveTextContent(/drops and recreates/i)
    },
  )

  it("postgresql (control): keeps the drop-and-recreate wording", async () => {
    const dialog = await openReloadDialog("postgresql")
    expect(dialog).toHaveTextContent(/drops and recreates the destination tables/i)
    expect(dialog).not.toHaveTextContent(/folder/i)
  })
})
