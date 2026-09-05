import "@testing-library/jest-dom/vitest"
import { afterEach, vi } from "vitest"
import { cleanup } from "@testing-library/react"

// jsdom does not implement scrollIntoView; components that auto-scroll (e.g.
// PipelineAccordionView's cardRef.current?.scrollIntoView) would throw an
// unhandled error under the test environment. Stub it globally.
if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = vi.fn()
}

// jsdom implements no part of the Pointer Capture API. Radix's Select calls
// hasPointerCapture/releasePointerCapture from the trigger's pointer handlers, so
// without these a click on a <SelectTrigger> throws instead of opening the listbox —
// which reads in a test as "the option does not exist" rather than as a missing DOM
// feature. Guarded, so a jsdom that grows a real implementation keeps it.
if (!Element.prototype.hasPointerCapture) {
  Element.prototype.hasPointerCapture = vi.fn(() => false)
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
}

// window.localStorage arrives as an empty object here (Node's own localStorage
// shim shadows jsdom's Storage — see the `--localstorage-file` warning on every
// run), so getItem/setItem are undefined. Anything reading the active workspace
// depends on it, so install a minimal in-memory Storage when the real one is
// missing. Guarded: a working implementation is left untouched.
if (typeof window !== "undefined" && typeof window.localStorage?.getItem !== "function") {
  const store = new Map<string, string>()
  const memoryStorage: Storage = {
    get length() {
      return store.size
    },
    key: (i: number) => Array.from(store.keys())[i] ?? null,
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, String(v)),
    removeItem: (k: string) => void store.delete(k),
    clear: () => store.clear(),
  }
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: memoryStorage,
  })
}

// Ensure the jsdom DOM is reset between tests so rendered components
// never leak state across cases.
afterEach(() => cleanup())
