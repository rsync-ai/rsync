// elkjs is used under EPL-2.0; its licence ships as public/third-party/elkjs-LICENSE.txt.

// The layout worker for elk-layout.ts. Loading the engine inside a worker makes it answer
// that worker's messages (it sets `self.onmessage`).
import "elkjs/lib/elk-worker.min.js"
