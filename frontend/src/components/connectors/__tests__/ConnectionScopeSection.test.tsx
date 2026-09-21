import { describe, it, expect, vi } from "vitest"
import { useState } from "react"
import { render, screen, fireEvent, waitFor } from "@testing-library/react"
import {
  ConnectionScopeSection,
  scopeConfigError,
  scopeFromConfig,
  type NamespaceListing,
  type ScopeValue,
} from "../ConnectionScopeSection"
import { NAMESPACE_NO_MATCH_WARNING } from "@/lib/pipeline/namespaceFilter"

function Harness({
  initial,
  load,
  pinned,
  onValue,
}: {
  initial: ScopeValue
  load: () => Promise<NamespaceListing>
  pinned?: string
  onValue?: (v: ScopeValue) => void
}) {
  const [value, setValue] = useState(initial)
  return (
    <ConnectionScopeSection
      namespaceKind="database"
      value={value}
      onChange={(v) => {
        setValue(v)
        onValue?.(v)
      }}
      pinnedDatabase={pinned}
      loadNamespaces={load}
    />
  )
}

const listing = (...namespaces: string[]) => vi.fn(async () => ({ namespaces }))

describe("ConnectionScopeSection", () => {
  it("offers All / Only these / All except and writes the choice", () => {
    const onValue = vi.fn()
    render(<Harness initial={{ mode: "all", patterns: "" }} load={listing()} onValue={onValue} />)

    expect(screen.getByRole("radio", { name: "All databases" })).toHaveAttribute("aria-checked", "true")
    expect(screen.queryByLabelText("Databases to read")).toBeNull()

    fireEvent.click(screen.getByRole("radio", { name: "Only these" }))
    expect(onValue).toHaveBeenLastCalledWith({ mode: "include", patterns: "" })
    // Only these with no pattern is what the server refuses: say so.
    expect(screen.getByTestId("connection-scope-error")).toHaveTextContent("needs at least one pattern")

    fireEvent.change(screen.getByLabelText("Databases to read"), { target: { value: "sales, crm_*" } })
    expect(onValue).toHaveBeenLastCalledWith({ mode: "include", patterns: "sales, crm_*" })
    expect(screen.queryByTestId("connection-scope-error")).toBeNull()

    fireEvent.click(screen.getByRole("radio", { name: "All except" }))
    expect(screen.getByLabelText("Databases to leave out")).toHaveValue("sales, crm_*")
  })

  it("previews what the patterns keep from the names the connector lists", async () => {
    const load = listing("sales", "crm_eu", "hr", "CRM_US")
    render(<Harness initial={{ mode: "include", patterns: "sales, crm_*" }} load={load} />)

    fireEvent.click(screen.getByRole("button", { name: /Preview databases/ }))

    await waitFor(() => expect(screen.getByTestId("connection-scope-preview")).toBeInTheDocument())
    expect(load).toHaveBeenCalledTimes(1)
    expect(screen.getByText("Reads 3 of 4 databases")).toBeInTheDocument()
    expect(screen.getByTestId("connection-scope-kept")).toHaveTextContent("sales, crm_eu, CRM_US")
    expect(screen.getByTestId("connection-scope-excluded")).toHaveTextContent("hr")

    // The preview follows the patterns without listing again.
    fireEvent.change(screen.getByLabelText("Databases to read"), { target: { value: "hr" } })
    expect(screen.getByText("Reads 1 of 4 databases")).toBeInTheDocument()
    expect(load).toHaveBeenCalledTimes(1)
  })

  it("warns, without failing, when the patterns match nothing", async () => {
    render(<Harness initial={{ mode: "include", patterns: "missing_db" }} load={listing("sales")} />)
    fireEvent.click(screen.getByRole("button", { name: /Preview databases/ }))
    await waitFor(() =>
      expect(screen.getByTestId("connection-scope-warning")).toHaveTextContent(NAMESPACE_NO_MATCH_WARNING),
    )
  })

  it("shows why a listing failed", async () => {
    const load = vi.fn(async () => {
      throw new Error("Listing namespaces failed: access denied")
    })
    render(<Harness initial={{ mode: "all", patterns: "" }} load={load} />)
    fireEvent.click(screen.getByRole("button", { name: /Preview databases/ }))
    await waitFor(() =>
      expect(screen.getByTestId("connection-scope-load-error")).toHaveTextContent("access denied"),
    )
  })

  it("a connection pinned to one database has no Scope to choose", () => {
    render(<Harness initial={{ mode: "all", patterns: "" }} load={listing()} pinned="shop" />)
    expect(screen.getByTestId("connection-scope-pinned")).toHaveTextContent("shop")
    expect(screen.queryByRole("radiogroup")).toBeNull()
  })
})

describe("scope config helpers", () => {
  it("reads a config's Scope, defaulting to All", () => {
    expect(scopeFromConfig({})).toEqual({ mode: "all", patterns: "" })
    expect(scopeFromConfig({ namespace_filter_mode: " Exclude ", namespace_filter_patterns: "tmp_*" })).toEqual({
      mode: "exclude",
      patterns: "tmp_*",
    })
  })

  it("names what a save must stop on", () => {
    expect(scopeConfigError({})).toBe("")
    expect(scopeConfigError({ namespace_filter_mode: "include" })).toMatch(/at least one pattern/)
    expect(scopeConfigError({ namespace_filter_mode: "only" })).toMatch(/must be one of/)
  })
})
