import { describe, it, expect, vi, afterEach } from "vitest"
import { copyTextToClipboard } from "@/lib/clipboard"

function setClipboard(value: unknown) {
  Object.defineProperty(globalThis, "navigator", {
    value: value === undefined ? {} : { clipboard: value },
    configurable: true,
    writable: true,
  })
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe("copyTextToClipboard", () => {
  it("reports success when the write resolves", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    setClipboard({ writeText })
    await expect(copyTextToClipboard("hello")).resolves.toBe(true)
    expect(writeText).toHaveBeenCalledWith("hello")
  })

  it("reports failure when the write rejects (denied permission, unfocused document)", async () => {
    setClipboard({ writeText: vi.fn().mockRejectedValue(new DOMException("NotAllowedError")) })
    await expect(copyTextToClipboard("hello")).resolves.toBe(false)
  })

  it("reports failure when navigator.clipboard is absent (insecure origin)", async () => {
    setClipboard(undefined)
    await expect(copyTextToClipboard("hello")).resolves.toBe(false)
  })

  it("never rejects, so no call site can leak an unhandled rejection", async () => {
    setClipboard({
      writeText: () => {
        throw new TypeError("synchronous blow-up")
      },
    })
    await expect(copyTextToClipboard("hello")).resolves.toBe(false)
  })
})
