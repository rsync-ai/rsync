import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, expect, it } from "vitest"

/**
 * An npm `overrides` entry REPLACES the range its dependents asked for. That is
 * the point of it -- and it is also why a security override is a liability the
 * day after it lands.
 *
 * #679 added `overrides.sharp: "^0.35.0"` to raise sharp off a libvips CVE. It
 * was the correct floor at the time. Two months later Dependabot #951 bumped
 * next 16.3.3 -> 16.3.4, and next 16.3.4 raises its own floor to sharp
 * `^0.35.4` -- because 0.35.4 is the fix for GHSA-rgj7-g3m4-5g8c. The lockfile's
 * sharp lines did not move. 0.35.3 still satisfies `^0.35.0`, so npm had no
 * reason to touch an entry that already resolved, and next's floor-raise -- the
 * thing carrying the security fix -- was discarded in silence. No error, no
 * warning, no failing job: the only `npm audit` in the repo is
 * `scripts/security-audit.sh:57`, which is report-only behind `|| true`.
 *
 * The general shape: an override whose floor sits BELOW a dependent's own floor
 * makes that dependent's floor non-binding. The override outlives the
 * vulnerability it was written for and then suppresses the fix for the next one.
 *
 * So this guard does not check for any particular version of any particular
 * package -- a pin would go stale the same way the override did. It checks the
 * invariant the override broke:
 *
 *     for every package an override targets, the version npm actually resolved
 *     must be at least the floor of every range declared for it in the tree.
 *
 * Floor, not full range satisfaction, and deliberately so. `overrides.postcss`
 * is `>=8.5.10` while next pins postcss to an exact `8.5.23`; the tree resolves
 * 8.5.28. Floating an exact transitive pin UPWARD is what a `>=` security
 * override is for, and a strict satisfies() check would fail on it. Being below
 * a declared floor is the defect; being above one is the intent.
 *
 * Range shapes this file cannot parse raise instead of being skipped. A guard
 * that silently drops the edges it does not understand is the failure mode that
 * produced the bug it is guarding.
 */

type OverrideTree = { [key: string]: string | OverrideTree }
type LockNode = {
  version?: string
  dependencies?: Record<string, string>
  optionalDependencies?: Record<string, string>
  peerDependencies?: Record<string, string>
  devDependencies?: Record<string, string>
}
type Lockfile = { packages: Record<string, LockNode> }

const DEP_FIELDS = ["dependencies", "optionalDependencies", "peerDependencies", "devDependencies"] as const

/**
 * The lowest version a range admits, as a comparable triple.
 *
 * Handles the shapes npm writes for a dependency edge: `^1.2.3`, `~1.2.3`,
 * `>=1.2.3`, `>1.2.3`, `=1.2.3`, a bare `1.2.3`, and partial forms (`^9`,
 * `^1.2`) where the absent components are zero. `>` is treated as `>=`, which
 * only ever makes the guard more permissive -- it cannot manufacture a
 * violation.
 *
 * Anything else -- a union (`||`), a wildcard, a hyphen range, a prerelease, a
 * url, `workspace:` -- throws. The set of override targets is small and
 * deliberate, so an unparseable range here means someone added an override over
 * a package whose dependents use a shape this predicate has not been taught.
 * That should stop CI and get the shape added, not pass quietly.
 */
export function floorOf(range: string): [number, number, number] {
  const m = /^(?:\^|~|>=|>|=|v)?\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?$/.exec(range.trim())
  if (!m) throw new Error(`unhandled range shape: ${JSON.stringify(range)}`)
  return [Number(m[1]), Number(m[2] ?? 0), Number(m[3] ?? 0)]
}

/** A concrete resolved version as a comparable triple. Prereleases throw. */
export function versionTriple(version: string): [number, number, number] {
  const m = /^(\d+)\.(\d+)\.(\d+)$/.exec(version.trim())
  if (!m) throw new Error(`unhandled resolved version: ${JSON.stringify(version)}`)
  return [Number(m[1]), Number(m[2]), Number(m[3])]
}

/** <0, 0, >0 — ordinary triple ordering. */
export function compareTriples(a: [number, number, number], b: [number, number, number]): number {
  return a[0] - b[0] || a[1] - b[1] || a[2] - b[2]
}

/**
 * The packages an overrides tree actually overrides.
 *
 * A key whose value is an object is a SCOPE, not a target: `{"eslint":
 * {"brace-expansion": "^1.1.13"}}` overrides brace-expansion inside eslint's
 * subtree and leaves eslint itself alone. npm's escape hatch for overriding the
 * scope package too is a `"."` key, so that -- and only that -- makes the key a
 * target as well.
 */
export function overrideTargets(tree: OverrideTree, acc = new Set<string>()): Set<string> {
  for (const [name, value] of Object.entries(tree)) {
    if (typeof value === "string") {
      acc.add(name)
      continue
    }
    if ("." in value) acc.add(name)
    const nested = Object.fromEntries(Object.entries(value).filter(([k]) => k !== "."))
    overrideTargets(nested as OverrideTree, acc)
  }
  return acc
}

/**
 * Which lock entry a requester at `requesterPath` actually gets for `name` --
 * node resolution, walking its own `node_modules` and then each ancestor's out
 * to the root. This is what makes nested duplicates (`brace-expansion` is in the
 * tree at both 1.1.18 and 5.0.9) compare against the copy their own requester
 * sees rather than whichever one happens to sit at the top.
 */
export function resolveFrom(
  packages: Record<string, LockNode>,
  requesterPath: string,
  name: string,
): string | null {
  let base = requesterPath
  for (;;) {
    const candidate = `${base ? `${base}/` : ""}node_modules/${name}`
    if (candidate in packages) return candidate
    if (!base) return null
    const i = base.lastIndexOf("/node_modules/")
    base = i === -1 ? "" : base.slice(0, i)
  }
}

export type FloorAudit = {
  targets: string[]
  edgesChecked: number
  violations: string[]
}

/** The predicate, over an arbitrary manifest + lockfile pair so it can be fed fixtures. */
export function auditOverrideFloors(overrides: OverrideTree, lock: Lockfile): FloorAudit {
  const targets = overrideTargets(overrides)
  const violations: string[] = []
  let edgesChecked = 0

  for (const [path, node] of Object.entries(lock.packages)) {
    for (const field of DEP_FIELDS) {
      for (const [name, range] of Object.entries(node[field] ?? {})) {
        if (!targets.has(name)) continue
        const resolvedPath = resolveFrom(lock.packages, path, name)
        if (resolvedPath === null) continue
        const got = lock.packages[resolvedPath].version
        if (got === undefined) continue
        edgesChecked += 1
        if (compareTriples(versionTriple(got), floorOf(range)) < 0) {
          violations.push(
            `${path || "<root>"} declares ${name}@${range} but the tree resolves ${got} (${resolvedPath})`,
          )
        }
      }
    }
  }

  return { targets: [...targets].sort(), edgesChecked, violations }
}

// ---------------------------------------------------------------------------
// the comparator, checked against worked cases
//
// The predicate above is hand-rolled rather than pulled from `semver`, which is
// present in the lockfile only as an undeclared transitive. A hand-rolled
// comparator is exactly the kind of thing that passes for the wrong reason, so
// it is exercised directly here instead of being trusted.
// ---------------------------------------------------------------------------

describe("the floor predicate itself", () => {
  it.each([
    ["^0.35.4", [0, 35, 4]],
    ["~8.5.16", [8, 5, 16]],
    [">=8.5.10", [8, 5, 10]],
    [">1.0.0", [1, 0, 0]],
    ["=1.2.3", [1, 2, 3]],
    ["8.5.23", [8, 5, 23]],
    ["^9", [9, 0, 0]],
    ["^1.2", [1, 2, 0]],
  ])("reads the floor of %s", (range, expected) => {
    expect(floorOf(range as string)).toEqual(expected)
  })

  it.each(["^1.0.0 || ^2.0.0", "*", "1.2.3 - 2.0.0", "latest", "workspace:*", ">=1.0.0-beta.1"])(
    "refuses to guess at %s rather than skipping it",
    (range) => {
      expect(() => floorOf(range)).toThrow(/unhandled range shape/)
    },
  )

  it("orders versions by component, not lexically", () => {
    // 0.35.3 < 0.35.4 is the case in hand; 0.9.0 < 0.10.0 is the one a string
    // comparison gets backwards.
    expect(compareTriples(versionTriple("0.35.3"), floorOf("^0.35.4"))).toBeLessThan(0)
    expect(compareTriples(versionTriple("0.35.4"), floorOf("^0.35.4"))).toBe(0)
    expect(compareTriples(versionTriple("0.35.5"), floorOf("^0.35.4"))).toBeGreaterThan(0)
    expect(compareTriples(versionTriple("0.10.0"), versionTriple("0.9.0"))).toBeGreaterThan(0)
  })

  it("treats an object-valued override key as a scope, not a target", () => {
    expect([...overrideTargets({ eslint: { "brace-expansion": "^1.1.13" } })]).toEqual(["brace-expansion"])
    // ...unless it opts itself in with npm's "." key.
    expect([...overrideTargets({ eslint: { ".": "^9.0.0", "brace-expansion": "^1.1.13" } })].sort()).toEqual([
      "brace-expansion",
      "eslint",
    ])
  })

  it("resolves a nested copy from the requester that sees it", () => {
    const packages = {
      "node_modules/brace-expansion": { version: "1.1.18" },
      "node_modules/glob/node_modules/brace-expansion": { version: "5.0.9" },
      "node_modules/glob/node_modules/minimatch": { dependencies: { "brace-expansion": "^5.0.5" } },
      "node_modules/minimatch": { dependencies: { "brace-expansion": "^1.1.7" } },
    }
    expect(resolveFrom(packages, "node_modules/glob/node_modules/minimatch", "brace-expansion")).toBe(
      "node_modules/glob/node_modules/brace-expansion",
    )
    expect(resolveFrom(packages, "node_modules/minimatch", "brace-expansion")).toBe(
      "node_modules/brace-expansion",
    )
  })
})

// ---------------------------------------------------------------------------
// the predicate against the shape of the bug, so "green" cannot mean "blind"
// ---------------------------------------------------------------------------

describe("the predicate catches the defect it was written for", () => {
  // The tree as it stood at 33779a10, reduced to the edges that matter: next
  // asks for sharp ^0.35.4, the override says ^0.35.0, and the lockfile keeps
  // the 0.35.3 it already had because that still resolves.
  const BROKEN: Lockfile = {
    packages: {
      "": { dependencies: { next: "16.3.4" } },
      "node_modules/next": { version: "16.3.4", optionalDependencies: { sharp: "^0.35.4" } },
      "node_modules/sharp": { version: "0.35.3" },
    },
  }

  it("reports the discarded floor", () => {
    const audit = auditOverrideFloors({ sharp: "^0.35.0" }, BROKEN)
    expect(audit.violations).toEqual([
      "node_modules/next declares sharp@^0.35.4 but the tree resolves 0.35.3 (node_modules/sharp)",
    ])
  })

  it("is quiet once the resolved version meets the floor", () => {
    const fixed: Lockfile = JSON.parse(JSON.stringify(BROKEN))
    fixed.packages["node_modules/sharp"].version = "0.35.4"
    const audit = auditOverrideFloors({ sharp: "^0.35.0" }, fixed)
    expect(audit.violations).toEqual([])
    expect(audit.edgesChecked).toBe(1)
  })

  it("does not flag a resolved version ABOVE an exact transitive pin", () => {
    // The postcss case. Without this the guard would fail on the intended
    // behaviour of a `>=` security override and get weakened back out again.
    const lock: Lockfile = {
      packages: {
        "node_modules/next": { version: "16.3.4", dependencies: { postcss: "8.5.23" } },
        "node_modules/postcss": { version: "8.5.28" },
      },
    }
    expect(auditOverrideFloors({ postcss: ">=8.5.10" }, lock).violations).toEqual([])
  })

  it("goes silent on a package no override targets", () => {
    // The scope of the guard is override targets. Drop the override and the
    // edge stops being checked -- which is why the real test below asserts its
    // denominator rather than just its verdict.
    const audit = auditOverrideFloors({}, BROKEN)
    expect(audit.edgesChecked).toBe(0)
    expect(audit.violations).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// the repo
// ---------------------------------------------------------------------------

describe("frontend/package.json overrides", () => {
  const manifest = JSON.parse(readFileSync(resolve(process.cwd(), "package.json"), "utf8"))
  const lock: Lockfile = JSON.parse(readFileSync(resolve(process.cwd(), "package-lock.json"), "utf8"))
  const audit = auditOverrideFloors(manifest.overrides ?? {}, lock)

  it("is actually looking at something", () => {
    // Without this, deleting the overrides block -- or a lockfile format change
    // that moved the dependency edges -- would leave a guard that passes by
    // examining nothing at all. That is how the sharp pin survived: the check
    // that should have caught it was not failing, it was absent.
    expect(audit.targets.length).toBeGreaterThan(0)
    expect(audit.edgesChecked).toBeGreaterThan(0)
  })

  it("never holds a package below a floor one of its dependents asked for", () => {
    expect(audit.violations).toEqual([])
  })
})
