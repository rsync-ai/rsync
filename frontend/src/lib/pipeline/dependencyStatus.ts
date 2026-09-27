// Label for a runtime dependency row (Runtime dependencies panel / Monitor tab).
//
// The gateway reports "unknown" for a dependency the orchestrator's prober has
// not checked yet (pipeline_runtime.go loadRuntimeDeps: COALESCE(h.status,
// 'unknown') over a LEFT JOIN). A batch run registers its dependencies only when
// the sink starts, so for its first probe interval every row read "Unknown",
// which looks like a failure. Never checked means "Checking…"; a probe that ran
// and could not decide still reads "unknown".
export function dependencyStatusLabel(dep: { status: string; last_checked_at?: string | null }): string {
  if (dep.status === "unknown" && !dep.last_checked_at) return "Checking…"
  return dep.status
}

// How long a dependency has been unwell, for a row that is not healthy.
//
// Both fields come from the orchestrator's dependency probe, which runs every 15s
// (dependency_probe.go writeHealth): consecutive_failures is reset to 0 by a
// healthy probe and incremented by any other result, and last_healthy_at is
// stamped only by a healthy probe and kept through the failures after it. A
// missing last_healthy_at is not read as "never healthy": a row written before
// the column existed has none either.
//
// Returns null for a healthy row, and for a row with neither reading, so the
// caller renders nothing rather than an empty clause.
export function dependencyStreak(
  dep: { status: string; consecutive_failures?: number | null; last_healthy_at?: string | null },
  age: (iso: string) => string,
): string | null {
  if (dep.status === "healthy") return null
  const n = dep.consecutive_failures
  const parts: string[] = []
  if (typeof n === "number" && n > 0) parts.push(`not healthy for ${n} check${n === 1 ? "" : "s"} in a row`)
  if (dep.last_healthy_at) parts.push(`last healthy ${age(dep.last_healthy_at)}`)
  return parts.length > 0 ? parts.join(" · ") : null
}
