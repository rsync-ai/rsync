/**
 * Same-tab signal that the active workspace's pipeline count may have changed
 * (a pipeline was created or deleted). The plan banner lives in the persistent
 * dashboard layout, so without this it would keep showing the count it fetched
 * when the layout first mounted (issues #7/#21).
 */
export const PIPELINES_CHANGED_EVENT = "rsync:pipelines-changed"

export function notifyPipelinesChanged(): void {
  if (typeof window === "undefined") return
  try {
    window.dispatchEvent(new Event(PIPELINES_CHANGED_EVENT))
  } catch {
    // Event constructor unavailable in exotic environments; non-fatal.
  }
}

export function onPipelinesChanged(handler: () => void): () => void {
  if (typeof window === "undefined") return () => {}
  const listener = () => handler()
  window.addEventListener(PIPELINES_CHANGED_EVENT, listener)
  return () => window.removeEventListener(PIPELINES_CHANGED_EVENT, listener)
}
