import { readdirSync, readFileSync, statSync } from "node:fs"
import { join, relative, resolve } from "node:path"

// Shared by the static contrast guards: walks src/ and yields every string literal
// that could hold a Tailwind class list, with the file:line it came from.

export const SRC = resolve(__dirname, "..")
const STRING_LITERAL = /"[^"\n]*"|'[^'\n]*'|`[^`]*`/g
const QUOTED = /"[^"\n]*"|'[^'\n]*'/g

/** Every `.ts`/`.tsx` under `src/`, tests excluded. Also the census guards' walker. */
export function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name)
    if (name === "__tests__" || name === "node_modules") return []
    if (statSync(path).isDirectory()) return sourceFiles(path)
    return name.endsWith(".tsx") || name.endsWith(".ts") ? [path] : []
  })
}

export interface StringLiteral {
  where: string
  text: string
}

export function* stringLiterals(): Generator<StringLiteral> {
  for (const file of sourceFiles(SRC)) {
    const src = readFileSync(file, "utf8")
    for (const m of src.matchAll(STRING_LITERAL)) {
      const line = src.slice(0, m.index).split("\n").length
      yield { where: `${relative(SRC, file)}:${line}`, text: m[0] }
    }
  }
}

/**
 * The class lists one literal can render. A plain string is one list. A template
 * literal renders its static text plus ONE branch of each `${a ? "x" : "y"}`, so
 * each nested string is paired with the static text on its own — never with the
 * other branch, which is never on the element at the same time.
 */
export function renderedClassLists(literal: string): string[] {
  if (!literal.startsWith("`")) return [literal]
  let text = ""
  let expr = ""
  let depth = 0
  for (let i = 0; i < literal.length; i++) {
    if (depth === 0 && literal[i] === "$" && literal[i + 1] === "{") {
      depth = 1
      i++
      text += " "
      continue
    }
    if (depth > 0) {
      if (literal[i] === "{") depth++
      if (literal[i] === "}") depth--
      if (depth > 0) expr += literal[i]
      else expr += "\n"
      continue
    }
    text += literal[i]
  }
  const branches = expr.match(QUOTED) ?? []
  return branches.length ? branches.map((b) => `${text} ${b}`) : [text]
}
