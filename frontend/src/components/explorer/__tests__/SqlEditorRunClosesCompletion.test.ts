import { describe, expect, it, vi } from "vitest"
import { EditorView } from "@codemirror/view"
import { EditorState } from "@codemirror/state"
import { autocompletion, completionStatus, startCompletion } from "@codemirror/autocomplete"

import { runFromEditor } from "@/components/explorer/SqlEditor"

// #54: after Cmd+Enter the autocomplete list stayed open over the results.

describe("runFromEditor", () => {
  it("closes an open suggestion list and runs", async () => {
    const view = new EditorView({
      state: EditorState.create({
        doc: "SELECT * FROM or",
        selection: { anchor: 16 },
        extensions: [
          autocompletion({
            override: [(ctx) => ({ from: ctx.pos - 2, options: [{ label: "orders" }] })],
          }),
        ],
      }),
      parent: document.body,
    })
    startCompletion(view)
    await vi.waitFor(() => expect(completionStatus(view.state)).toBe("active"))

    const onSubmit = vi.fn()
    expect(runFromEditor(view, onSubmit)).toBe(true)

    expect(completionStatus(view.state)).toBeNull()
    expect(onSubmit).toHaveBeenCalledTimes(1)
    view.destroy()
  })
})
