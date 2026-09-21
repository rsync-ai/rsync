import { describe, expect, it } from "vitest"
import { readFileSync } from "node:fs"
import { createRequire } from "node:module"

import { renderedClassLists, stringLiterals } from "./class-literals"

// #48 prod retest: two small count badges measured 3.81:1 (white on bg-red-500, the
// notification bell) and 2.13:1 (white on bg-amber-500, the Assessment "high" count),
// under WCAG AA's 4.5:1. Nothing checked white text on a coloured fill.
//
// The ratio is computed, not listed: Tailwind v4 defines each palette colour in
// oklch in `tailwindcss/theme.css`, so this reads that file, converts to sRGB and
// applies the WCAG formula. A new `bg-*-* text-white` pair is measured the day it
// is written, whatever its hue.

const require = createRequire(import.meta.url)
const THEME = readFileSync(require.resolve("tailwindcss/theme.css"), "utf8")
const AA_BODY_TEXT = 4.5

const palette = new Map<string, [number, number, number]>()
for (const m of THEME.matchAll(/--color-([a-z]+-\d+):\s*oklch\(([\d.]+)%\s+([\d.]+)\s+([\d.]+)\)/g)) {
  palette.set(m[1], [Number(m[2]) / 100, Number(m[3]), Number(m[4])])
}

// OKLab -> linear sRGB (Björn Ottosson's matrices), clipped to the sRGB gamut the
// way a browser paints it.
function linearRGB([L, C, h]: [number, number, number]): number[] {
  const a = C * Math.cos((h * Math.PI) / 180)
  const b = C * Math.sin((h * Math.PI) / 180)
  const l = (L + 0.3963377774 * a + 0.2158037573 * b) ** 3
  const m = (L - 0.1055613458 * a - 0.0638541728 * b) ** 3
  const s = (L - 0.0894841775 * a - 1.291485548 * b) ** 3
  return [
    4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s,
    -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s,
    -0.0041960863 * l - 0.7034186147 * m + 1.707614701 * s,
  ].map((v) => Math.min(1, Math.max(0, v)))
}

function whiteContrastOn(color: string): number | undefined {
  const oklch = palette.get(color)
  if (!oklch) return undefined
  const [r, g, b] = linearRGB(oklch)
  return 1.05 / (0.2126 * r + 0.7152 * g + 0.0722 * b + 0.05)
}

const WHITE_TEXT = /(?<![\w:-])text-white(?![\w/-])/
const BASE_BG = /(?<![\w:-])bg-([a-z]+-\d+)(?![\w/-])/g

describe("white text on a coloured fill", () => {
  it("reads the real palette (the two retest measurements)", () => {
    expect(palette.size).toBeGreaterThan(200)
    expect(whiteContrastOn("red-500")).toBeCloseTo(3.82, 1)
    expect(whiteContrastOn("amber-500")).toBeCloseTo(2.15, 1)
    expect(whiteContrastOn("violet-600")!).toBeGreaterThan(AA_BODY_TEXT)
  })

  it("keeps every text-white class list at 4.5:1 or better", () => {
    const offenders: string[] = []
    for (const { where, text } of stringLiterals()) {
      for (const list of renderedClassLists(text)) {
        if (!WHITE_TEXT.test(list)) continue
        for (const [, color] of list.matchAll(BASE_BG)) {
          const ratio = whiteContrastOn(color)
          if (ratio !== undefined && ratio < AA_BODY_TEXT) {
            offenders.push(`${where} white on bg-${color} = ${ratio.toFixed(2)}:1`)
          }
        }
      }
    }
    expect(offenders).toEqual([])
  })

  it("pairs a template's static classes with one branch at a time", () => {
    const lists = renderedClassLists('`px-4 ${isUser ? "bg-violet-600 text-white" : "bg-zinc-100"}`')
    expect(lists).toHaveLength(2)
    expect(lists.some((l) => l.includes("text-white") && l.includes("bg-zinc-100"))).toBe(false)
  })
})
