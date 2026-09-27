import { describe, it, expect } from "vitest"
import { requireConnectionId } from "@/components/modal/ConnectorConfigModal"
import { serverReason } from "@/components/pipeline/PipelineSchedulePanel"

const jsonResponse = (status: number, body: unknown) =>
  ({ status, json: async () => body }) as unknown as Response

describe("requireConnectionId", () => {
  it("refuses to invent an id when the server returned none", () => {
    // The bug: `result.id || crypto.randomUUID()` produced a client-side uuid
    // and the modal toasted success over it.
    expect(() => requireConnectionId({})).toThrow(/no connection id/)
    expect(() => requireConnectionId({ id: "" })).toThrow(/no connection id/)
    expect(() => requireConnectionId({ id: "   " })).toThrow(/no connection id/)
    expect(() => requireConnectionId(null)).toThrow(/no connection id/)
    expect(() => requireConnectionId({ id: 42 })).toThrow(/no connection id/)
  })

  it("returns the server's id when there is one", () => {
    // Non-zero control.
    expect(requireConnectionId({ id: "conn_abc123" })).toBe("conn_abc123")
    expect(requireConnectionId({ id: " conn_abc123 " })).toBe("conn_abc123")
  })
})

describe("serverReason", () => {
  it("surfaces the server's own reason instead of discarding it", async () => {
    expect(await serverReason(jsonResponse(409, { error: "schedule not found" }))).toBe("schedule not found")
    expect(await serverReason(jsonResponse(409, { message: "pipeline is still running" }))).toBe("pipeline is still running")
    expect(await serverReason(jsonResponse(503, { detail: "temporal unreachable" }))).toBe("temporal unreachable")
  })

  it("falls back to the status when the body says nothing", async () => {
    // Non-zero control: never an empty description.
    expect(await serverReason(jsonResponse(502, {}))).toBe("The server answered 502.")
    expect(await serverReason({ status: 500, json: async () => { throw new Error("not json") } } as unknown as Response))
      .toBe("The server answered 500.")
  })
})
