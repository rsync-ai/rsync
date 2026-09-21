import { readFileSync } from "node:fs"
import { resolve } from "node:path"

import { describe, expect, it } from "vitest"

import { LLM_NOT_CONFIGURED, llmNotConfiguredError } from "@/lib/explorer/llmNotConfigured"

// An install without an LLM is supported. Asking in plain English answers 503
// {"error":"llm_not_configured","message":"Set up an LLM first: ..."}, and the
// Explorer used to show that as "AI service unavailable" with the code itself as
// the message and a hint to check an Ollama nobody was meant to run.

const MESSAGE = "Set up an LLM first: add OPENAI_API_KEY to .env, then restart rsync."

describe("llmNotConfiguredError", () => {
  it("reads the gateway's flat body", () => {
    const e = llmNotConfiguredError({ error: LLM_NOT_CONFIGURED, message: MESSAGE })
    expect(e).toEqual({
      code: LLM_NOT_CONFIGURED,
      title: "Set up an LLM first",
      message: MESSAGE,
      hint: expect.stringContaining("SQL"),
    })
  })

  it("reads the body nested under detail", () => {
    expect(llmNotConfiguredError({ detail: { error: LLM_NOT_CONFIGURED, message: MESSAGE } })?.message).toBe(
      MESSAGE,
    )
  })

  it("never shows the code as the message", () => {
    const e = llmNotConfiguredError({ error: LLM_NOT_CONFIGURED })
    expect(e?.message).toBeTruthy()
    expect(e?.message).not.toContain(LLM_NOT_CONFIGURED)
  })

  it.each([
    ["a plain 503", { detail: "busy" }],
    ["another error code", { error: "rate_limited", message: "slow down" }],
    ["a sentence that mentions the code", { error: "llm_not_configured upstream", message: MESSAGE }],
    ["a string", "llm_not_configured"],
    ["nothing", null],
  ])("leaves %s alone", (_label, body) => {
    expect(llmNotConfiguredError(body)).toBeNull()
  })
})

describe("the Explorer's classifyApiError", () => {
  const page = readFileSync(resolve(__dirname, "../../../app/(dashboard)/explorer/page.tsx"), "utf8")
  const start = page.indexOf("function classifyApiError(")
  const body = page.slice(start, page.indexOf("\n}\n", start))

  it("checks for no LLM before any status branch", () => {
    expect(start).toBeGreaterThan(-1)
    const check = body.indexOf("llmNotConfiguredError(data)")
    const firstStatus = body.indexOf("if (status ===")
    // Control: the status branches this ordering is about are still there.
    expect(body.indexOf("if (status === 503)")).toBeGreaterThan(-1)
    expect(check).toBeGreaterThan(-1)
    expect(check).toBeLessThan(firstStatus)
  })
})
