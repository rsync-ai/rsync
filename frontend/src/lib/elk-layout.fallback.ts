// elkjs is used under EPL-2.0; its licence ships as public/third-party/elkjs-LICENSE.txt.

// elk-layout.ts on a page with no worker: the same engine, run on the main thread.
// A module of its own so the engine loads only when that happens.
export { default } from "elkjs/lib/elk.bundled.js"
