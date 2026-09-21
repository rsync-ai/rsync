import { describe, expect, it } from "vitest"

import { stringLiterals } from "./class-literals"

// #48: `text-zinc-500` is 4.1:1 on the dark page background and 3.1:1 on dark
// cards, under WCAG AA's 4.5:1 for body text. 505 class lists used it with no dark
// variant. Every class list that sets it must also set a dark text color
// (`dark:text-zinc-400` reads 7.8:1 / 5.9:1 on the same surfaces).
//
// #48 prod retest: the guard only knew zinc-500, so the Executions "Details" button
// (`text-zinc-600`, no dark color) shipped at 2.58:1. A grey from 500 up is darker
// still on a dark surface, in every grey family, and a hover grey is the same text
// one pointer move later — so all of them need their dark counterpart.

const GREY = "(?:zinc|gray|slate|neutral|stone)-(?:500|600|700|800|900|950)"
const BASE_GREY_TEXT = new RegExp(`(?<![\\w:-])text-${GREY}(?![\\w/-])`)
const HOVER_GREY_TEXT = new RegExp(`(?<![\\w:-])hover:text-${GREY}(?![\\w/-])`)

describe("dark-mode grey text contrast", () => {
  it("never sets a grey text color from 500 up without a dark text color", () => {
    const offenders: string[] = []
    for (const { where, text } of stringLiterals()) {
      if (BASE_GREY_TEXT.test(text) && !text.includes("dark:text-")) {
        offenders.push(`${where} ${text.slice(0, 80)}`)
      }
    }
    expect(offenders).toEqual([])
  })

  it("never sets a grey hover text color from 500 up without a dark hover color", () => {
    const offenders: string[] = []
    for (const { where, text } of stringLiterals()) {
      if (HOVER_GREY_TEXT.test(text) && !text.includes("dark:hover:text-")) {
        offenders.push(`${where} ${text.slice(0, 80)}`)
      }
    }
    expect(offenders).toEqual([])
  })
})
