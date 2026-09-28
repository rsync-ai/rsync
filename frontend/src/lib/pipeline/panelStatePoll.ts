// How often a run panel (Overview's live state, Monitoring) re-reads /state for a
// given status; null = it does not poll. Shared so the two panels cannot drift.
//
// "running" is a streaming CDC pipeline's steady state. Leaving it out stopped
// both panels polling for the whole life of a stream, so after Stop → Resume they
// showed the previous execution and its verdict until a hard reload (U-18). It
// polls slower than an active run: a stream's status rarely changes, and the
// refresh bus covers the changes this page makes itself.
const PANEL_STATE_POLL_MS: Record<string, number> = {
  processing: 2500,
  waiting_for_user: 5000,
  pending: 5000,
  running: 10_000,
}

export function panelStatePollMs(status: string | undefined | null): number | null {
  return PANEL_STATE_POLL_MS[String(status || "")] ?? null
}

// How often a panel whose first /state read failed tries again. Nothing else
// re-reads a finished run's /state, so without it the panel kept its empty first
// answer until a reload.
export const PANEL_STATE_RETRY_MS = 5000

// The retry above backs off by doubling and stops growing here.
export const PANEL_STATE_RETRY_MAX_MS = 60_000
