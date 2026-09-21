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
