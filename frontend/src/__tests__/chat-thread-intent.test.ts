import { describe, expect, it } from "vitest"

import {
  isSlotFillingState,
  nextActiveIntent,
  shouldStartFreshThread,
} from "@/lib/chat/thread-intent"

// Issue #3: "YOU ASKED" showed the sync-mode label. Issue #13: a second pipeline
// request continued inside the first pipeline's thread.

describe("isSlotFillingState", () => {
  it("treats connector/role questions as slot filling, but not confirmation", () => {
    expect(isSlotFillingState("awaiting_source")).toBe(true)
    expect(isSlotFillingState("awaiting_destination")).toBe(true)
    expect(isSlotFillingState("awaiting_role")).toBe(true)
    expect(isSlotFillingState("awaiting_confirmation")).toBe(false)
    expect(isSlotFillingState("idle")).toBe(false)
    expect(isSlotFillingState(undefined)).toBe(false)
  })
})

describe("nextActiveIntent", () => {
  const original = "Create a pipeline from MongoDB to GCS using CDC snapshot + streaming"

  it("keeps the original request when a sync-mode card is clicked", () => {
    expect(
      nextActiveIntent({
        current: original,
        prompt: "__syncmode__ initial_plus_cdc",
        displayText: "CDC (snapshot + changes)",
        awaitingSlot: false,
      })
    ).toBe(original)
  })

  it("keeps the original request for yes/no replies and slot answers", () => {
    expect(nextActiveIntent({ current: original, prompt: "yes", awaitingSlot: false })).toBe(original)
    expect(nextActiveIntent({ current: original, prompt: "gcs", awaitingSlot: true })).toBe(original)
  })

  it("adopts a freely typed new request", () => {
    expect(nextActiveIntent({ current: original, prompt: "mysql to aws s3", awaitingSlot: false })).toBe(
      "mysql to aws s3"
    )
    expect(nextActiveIntent({ current: "", prompt: "yes", awaitingSlot: false })).toBe("yes")
  })
})

describe("shouldStartFreshThread", () => {
  const created = ["pipeline_confirmation", "pipeline_started"]

  it("starts a fresh thread for a new pipeline request after one was created", () => {
    expect(
      shouldStartFreshThread({
        prompt: "Create a pipeline from MongoDB to GCS",
        awaitingSlot: false,
        threadResponseTypes: created,
      })
    ).toBe(true)
    expect(
      shouldStartFreshThread({
        prompt: "sync orders from mysql into bigquery",
        awaitingSlot: false,
        threadResponseTypes: ["pipeline_scheduled"],
      })
    ).toBe(true)
  })

  it("stays in the thread before a pipeline exists, for replies, UI commands and slot answers", () => {
    const prompt = "Create a pipeline from MongoDB to GCS"
    expect(
      shouldStartFreshThread({ prompt, awaitingSlot: false, threadResponseTypes: ["pipeline_confirmation"] })
    ).toBe(false)
    expect(shouldStartFreshThread({ prompt: "yes", awaitingSlot: false, threadResponseTypes: created })).toBe(false)
    expect(
      shouldStartFreshThread({ prompt, displayText: "Batch", awaitingSlot: false, threadResponseTypes: created })
    ).toBe(false)
    expect(shouldStartFreshThread({ prompt, awaitingSlot: true, threadResponseTypes: created })).toBe(false)
    expect(
      shouldStartFreshThread({ prompt: "why is it slow?", awaitingSlot: false, threadResponseTypes: created })
    ).toBe(false)
  })
})
