import { describe, it, expect, vi } from "vitest"
import { render, screen, fireEvent } from "@testing-library/react"
import { NlExamplePrompts } from "../NlExamplePrompts"

describe("NlExamplePrompts", () => {
  it("renders a schema-aware chip for each example", () => {
    render(<NlExamplePrompts tables={[{ name: "orders" }]} onSelect={() => {}} />)
    expect(screen.getByRole("button", { name: /count orders/i })).toBeInTheDocument()
  })

  it("calls onSelect with the full prompt when a chip is clicked", () => {
    const onSelect = vi.fn()
    render(<NlExamplePrompts tables={[{ name: "orders" }]} onSelect={onSelect} />)
    fireEvent.click(screen.getByRole("button", { name: /count orders/i }))
    expect(onSelect).toHaveBeenCalledWith("How many rows are in orders?")
  })

  it("falls back to generic examples when no tables are provided", () => {
    render(<NlExamplePrompts onSelect={() => {}} />)
    expect(screen.getByRole("button", { name: /count rows/i })).toBeInTheDocument()
  })
})
