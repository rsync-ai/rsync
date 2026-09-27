import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, expect, it } from "vitest"

// The minifier drops every source comment, elkjs's own licence header included, so the
// EPL-2.0 notice the app ships with elkjs is this served file. It has to follow the
// installed elkjs: its licence text, its version, and a link to that version's source.

const read = (path: string) => readFileSync(resolve(process.cwd(), path), "utf8")

describe("the served elkjs licence", () => {
  const served = read("public/third-party/elkjs-LICENSE.txt")
  const { version } = JSON.parse(read("node_modules/elkjs/package.json")) as { version: string }

  it("carries the installed elkjs's licence text, unchanged", () => {
    const licence = read("node_modules/elkjs/LICENSE.md")
    expect(licence).toMatch(/^# Eclipse Public License - v 2\.0/)
    expect(served).toContain(licence)
  })

  it("names the installed version and links to its source", () => {
    expect(served).toContain(`elkjs ${version} `)
    expect(served).toContain(`https://github.com/kieler/elkjs/tree/${version}`)
  })

  it("keeps the upstream copyright notice", () => {
    expect(served).toContain("Kiel University and others")
  })
})
