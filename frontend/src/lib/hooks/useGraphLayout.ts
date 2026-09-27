"use client"

import { useEffect, useEffectEvent, useState } from "react"

export interface GraphLayoutState<I, L> {
  /** The newest finished layout, with the key and input it was computed for. Draw from these. */
  settled: { key: string; input: I; layout: L } | null
  /** A layout for the current key is still running; `settled` is an older one. */
  pending: boolean
  /** The layout for the current key failed. */
  error: Error | null
}

/**
 * Runs an async layout of `input` whenever `key` changes; `key` names what is laid out,
 * so a refresh that rebuilds an equal input does not lay it out again. Until the new
 * layout lands, the previous one stays on screen as a consistent pair: cards drawn from
 * `settled.input` at `settled.layout`'s positions, never a new set of cards at old
 * positions. A result for a key that has since changed is dropped.
 */
export function useGraphLayout<I, L>(key: string, input: I, run: (input: I) => Promise<L>): GraphLayoutState<I, L> {
  const [settled, setSettled] = useState<{ key: string; input: I; layout: L } | null>(null)
  const [failure, setFailure] = useState<{ key: string; error: Error } | null>(null)

  const start = useEffectEvent(() => ({ input, done: run(input) }))
  useEffect(() => {
    let current = true
    const { input: laidOut, done } = start()
    done.then(
      (layout) => {
        if (current) setSettled({ key, input: laidOut, layout })
      },
      (err: unknown) => {
        if (current) setFailure({ key, error: err instanceof Error ? err : new Error(String(err)) })
      },
    )
    return () => {
      current = false
    }
  }, [key])

  const error = failure?.key === key ? failure.error : null
  return { settled, pending: settled?.key !== key && error === null, error }
}
