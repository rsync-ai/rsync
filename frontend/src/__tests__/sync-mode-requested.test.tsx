import { describe, expect, it, vi } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import { SyncModeChoiceInline } from "@/components/chat/SyncModeChoiceInline"
import { confirmCommand, requestedOptionId } from "@/components/chat/ChatMessageItem"

// Issue #13: a request that already said "CDC snapshot + streaming" was asked
// "Choose Sync Mode" again. Issue #4: the selected card had no visible or
// accessible selected state.

const options = [
  { id: "batch", label: "Batch (one-time historical load)" },
  { id: "initial_plus_cdc", label: "CDC (snapshot + changes)" },
  { id: "cdc_changes_only", label: "CDC (changes only)" },
]

function renderCard(requested?: string) {
  return render(
    <SyncModeChoiceInline
      message="How should this pipeline sync?"
      choiceType="sync_mode"
      options={options}
      sourceType="mongodb"
      destType="gcs"
      requestedOptionId={requested}
      onChoice={vi.fn()}
    />
  )
}

describe("requestedOptionId", () => {
  const ids = options.map((o) => o.id)
  it("maps the gateway's requested mode onto a card option", () => {
    expect(requestedOptionId(ids, "cdc", "initial")).toBe("initial_plus_cdc")
    expect(requestedOptionId(ids, "cdc", "streaming_only")).toBe("cdc_changes_only")
    expect(requestedOptionId(ids, "batch")).toBe("batch")
  })
  it("returns undefined when nothing was requested or the card lacks the option", () => {
    expect(requestedOptionId(ids, "", "")).toBeUndefined()
    expect(requestedOptionId(["batch"], "cdc", "initial")).toBeUndefined()
  })
})

describe("SyncModeChoiceInline", () => {
  it("pre-selects the requested mode and confirms instead of asking", () => {
    renderCard("initial_plus_cdc")
    expect(screen.queryByText("Choose Sync Mode")).toBeNull()
    expect(screen.getByText("Confirm Pipeline")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /CDC \(snapshot \+ changes\)/ })).toHaveAttribute(
      "aria-pressed",
      "true"
    )
    expect(screen.getByRole("button", { name: /Batch.*historical/ })).toHaveAttribute("aria-pressed", "false")
    expect(screen.getByRole("button", { name: /Start pipeline/ })).toBeEnabled()
  })

  it("asks for a mode when the request named none, and marks the pick as selected", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    renderCard()
    expect(screen.getByText("Choose Sync Mode")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /Start pipeline/ })).toBeDisabled()
    const batch = screen.getByRole("button", { name: /Batch.*historical/ })
    expect(batch).toHaveAttribute("aria-pressed", "false")
    await user.click(batch)
    expect(batch).toHaveAttribute("aria-pressed", "true")
    expect(batch).toHaveAttribute("data-selected", "true")
    expect(screen.getByRole("group", { name: "Sync mode" })).toBeInTheDocument()
  })
})

// Issue #45: the card had no destination-database field, so a request that named
// the destination database gave no sign of where the rows would land.
describe("SyncModeChoiceInline destination field", () => {
  function renderWithNamespace(onChoice = vi.fn(), requested = "datingapp_pg3") {
    render(
      <SyncModeChoiceInline
        message="Confirm pipeline"
        choiceType="sync_mode"
        options={options}
        sourceType="postgresql"
        destType="mongodb"
        requestedOptionId="batch"
        destinationNamespace={{ kind: "database", requested, defaultName: "public" }}
        onChoice={onChoice}
      />
    )
    return onChoice
  }

  it("shows the requested database and sends it with Start", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    const onChoice = renderWithNamespace()
    const field = screen.getByLabelText("Destination database")
    expect(field).toHaveValue("datingapp_pg3")
    await user.click(screen.getByRole("button", { name: /Start pipeline/ }))
    expect(onChoice).toHaveBeenCalledTimes(1)
    const [choiceId, ctx] = onChoice.mock.calls[0]
    expect(choiceId).toBe("batch")
    expect(ctx.destination_namespace).toBe("datingapp_pg3")
  })

  it("shows the default as a placeholder and blocks names that need quoting", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    renderWithNamespace(vi.fn(), "")
    const field = screen.getByLabelText("Destination database")
    expect(field).toHaveAttribute("placeholder", "Default: public")
    await user.type(field, "my-db")
    expect(field).toHaveAttribute("aria-invalid", "true")
    expect(screen.getByRole("button", { name: /Start pipeline/ })).toBeDisabled()
  })

  it("is absent when the gateway sent no namespace hint", () => {
    renderCard("batch")
    expect(screen.queryByLabelText(/Destination /)).toBeNull()
  })
})

// Prod 2026-09-26: on a GCS destination the chat card said "Leave empty to use
// the default" for the path prefix, but layout v2 (gcs / aws-s3 / azure-blob)
// has no default and the gateway refused the empty prefix after Start.
describe("SyncModeChoiceInline path prefix on a layout-v2 object store", () => {
  function renderPrefix(destType: string, onChoice = vi.fn()) {
    render(
      <SyncModeChoiceInline
        message="Confirm pipeline"
        choiceType="sync_mode"
        options={options}
        sourceType="postgresql"
        destType={destType}
        requestedOptionId="batch"
        destinationNamespace={{ kind: "path", requested: "", defaultName: "" }}
        onChoice={onChoice}
      />
    )
    return onChoice
  }

  it.each(["gcs", "aws-s3", "azure-blob"])("%s: the prefix is required, says so, and Start waits for it", async (destType) => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    const onChoice = renderPrefix(destType)
    const field = screen.getByLabelText(/Destination path prefix \(required\)/)
    expect(field).toHaveAttribute("aria-required", "true")
    expect(field.getAttribute("placeholder")).toMatch(/sales_orders/)
    expect(screen.queryByText(/Leave empty/)).toBeNull()

    const start = screen.getByRole("button", { name: /Start pipeline/ })
    expect(start).toBeDisabled()
    const help = document.getElementById(start.getAttribute("aria-describedby") || "")
    expect(help?.textContent).toMatch(/^Required/)

    await user.type(field, "Sales")
    expect(field).toHaveAttribute("aria-invalid", "true")
    expect(help?.textContent).toMatch(/lowercase letters/i)
    expect(start).toBeDisabled()

    await user.clear(field)
    await user.type(field, "sales_eu")
    expect(start).toBeEnabled()
    await user.click(start)
    expect(onChoice).toHaveBeenCalledTimes(1)
    expect(onChoice.mock.calls[0][1].destination_namespace).toBe("sales_eu")
  })

  // Control: minio stays on layout v1, where an empty prefix has a default.
  it("minio: the prefix stays optional", () => {
    renderPrefix("minio")
    expect(screen.getByLabelText("Destination path prefix")).not.toHaveAttribute("aria-required")
    expect(screen.getByText(/Leave empty to use the default\./)).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /Start pipeline/ })).toBeEnabled()
  })
})

describe("confirmCommand", () => {
  it("appends the destination name to the mode command", () => {
    expect(confirmCommand("initial_plus_cdc", "datingapp_pg3")).toBe(
      "Yes sync_mode=cdc cdc_mode=initial destination_namespace=datingapp_pg3"
    )
    expect(confirmCommand("batch", " ")).toBe("Yes sync_mode=batch")
    expect(confirmCommand("unknown")).toBe("Yes")
  })
})
