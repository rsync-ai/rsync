import { describe, it, expect, vi, beforeEach, type Mock } from "vitest"
import { act, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

// The dialog reads each connector's namespace model from the gateway. It can
// open before they arrive: the destination field is seeded again when they do,
// unless the user has already typed in it.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"
import { namespaceModelsSnapshot, primeNamespaceModels } from "@/lib/pipeline/namespaceModel"
import { repoNamespaceModels } from "@/lib/pipeline/__tests__/repoNamespaceModels"
import { PipelineTableSelector } from "../PipelineTableSelector"

const json = (status: number, data: unknown) => ({ ok: status >= 200 && status < 300, status, json: async () => data })

let releaseModels: () => void
function routeAuthFetch() {
  const gate = new Promise<void>((r) => (releaseModels = r))
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    if (String(url).endsWith("/api/v1/connectors/namespace-models")) {
      await gate
      return json(200, repoNamespaceModels())
    }
    return json(200, {})
  })
}

function renderSelector(props: Record<string, unknown>) {
  return render(
    <PipelineTableSelector
      isOpen
      onClose={() => {}}
      pipelineId="pl_ns_models"
      sourceType="mysql"
      availableTables={[{ name: "orders", schema: "shop", row_count: 1, columns: 1 }] as never}
      suggestedTables={[]}
      destinationType="postgresql"
      destinationConfig={{ namespace: "", namespace_kind: "schema", create_if_not_exists: true }}
      {...props}
    />
  )
}

const field = () => screen.getByRole("textbox", { name: /name/i }) as HTMLInputElement

describe("PipelineTableSelector — namespace models arriving after the dialog opens", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    primeNamespaceModels(undefined)
    routeAuthFetch()
  })

  it("seeds the destination's own default once the models arrive", async () => {
    renderSelector({})
    expect(field().value).toBe("")

    await act(async () => releaseModels())
    await waitFor(() => expect(field().value).toBe("public"))
    const calls = (authFetch as Mock).mock.calls.filter(([u]) => String(u).endsWith("/connectors/namespace-models"))
    expect(calls).toHaveLength(1)
  })

  it("relabels an object-store destination stored with a stale schema kind", async () => {
    renderSelector({ sourceType: "mongodb", destinationType: "aws-s3" })

    await act(async () => releaseModels())
    await waitFor(() => expect(screen.getByLabelText(/Path prefix/i)).toBeInTheDocument())
    expect(screen.queryByLabelText(/Schema name/i)).toBeNull()
  })

  it("never overwrites what the user typed before they arrived (control)", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    renderSelector({})
    await user.type(field(), "warehouse")

    await act(async () => releaseModels())
    await waitFor(() => expect(namespaceModelsSnapshot()).toBeDefined())
    await act(async () => {
      await new Promise((r) => setTimeout(r, 20))
    })
    expect(field().value).toBe("warehouse")
  })
})
