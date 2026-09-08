"use client"

import ReactMarkdown, { type Components } from "react-markdown"
import remarkGfm from "remark-gfm"
import remarkBreaks from "remark-breaks"

/**
 * Renders assistant chat text as Markdown.
 *
 * The backend has always written Markdown -- every chat prompt under
 * llm-service/prompts/chat/ asks for it, and the Go handlers assemble replies
 * with `**bold**` headings and `- ` bullets by hand. The chat pane displayed
 * that string verbatim inside `whitespace-pre-wrap`, so the product's own
 * asterisks and backticks were read as punctuation by every user. react-markdown
 * and both remark plugins were already declared in package.json and imported
 * nowhere; this is the missing half.
 *
 * Three deliberate choices:
 *
 *  - `remarkBreaks` maps a single newline to a line break. Chat replies are
 *    written like chat, not like a document, and without it CommonMark folds
 *    consecutive lines into one paragraph -- a visible regression against the
 *    `whitespace-pre-wrap` this replaces.
 *  - No `rehype-raw`. react-markdown drops embedded HTML by default and that
 *    default is load-bearing here: the text being rendered is model output, and
 *    a reply that happens to contain a `<script>` or an `onerror=` attribute
 *    must stay inert text. The same default sanitises `javascript:` hrefs.
 *  - Only the assistant's own text goes through here. User messages are echoed
 *    back as plain text; nothing the user types should acquire markup on the way
 *    to the screen.
 *
 * Tailwind v4 with no @tailwindcss/typography plugin in this app, so every
 * element is styled explicitly rather than via `prose`.
 */

const components: Components = {
  // Chat is a stream of short replies, so paragraphs are tight and only spaced
  // between siblings -- a leading margin would push the first line off the
  // "Assistant" label it belongs to.
  p: ({ children }) => <p className="mb-2 last:mb-0 leading-relaxed">{children}</p>,
  strong: ({ children }) => <strong className="font-semibold">{children}</strong>,
  em: ({ children }) => <em className="italic">{children}</em>,
  ul: ({ children }) => <ul className="mb-2 last:mb-0 list-disc pl-5 space-y-1">{children}</ul>,
  ol: ({ children }) => <ol className="mb-2 last:mb-0 list-decimal pl-5 space-y-1">{children}</ol>,
  li: ({ children }) => <li className="leading-relaxed">{children}</li>,
  // The pane is narrow; a reply's headings are subheads within a message, not
  // page titles, so they stay close to body size and lean on weight instead.
  h1: ({ children }) => <h1 className="mt-3 first:mt-0 mb-1.5 text-base font-semibold">{children}</h1>,
  h2: ({ children }) => <h2 className="mt-3 first:mt-0 mb-1.5 text-sm font-semibold">{children}</h2>,
  h3: ({ children }) => <h3 className="mt-3 first:mt-0 mb-1 text-sm font-semibold">{children}</h3>,
  h4: ({ children }) => <h4 className="mt-2 first:mt-0 mb-1 text-sm font-semibold">{children}</h4>,
  h5: ({ children }) => <h5 className="mt-2 first:mt-0 mb-1 text-sm font-semibold">{children}</h5>,
  h6: ({ children }) => <h6 className="mt-2 first:mt-0 mb-1 text-sm font-semibold">{children}</h6>,
  a: ({ href, children }) => (
    // Every link in a reply is off-app. rel is not optional with target=_blank:
    // without noopener the opened tab gets a live window.opener handle back.
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="underline underline-offset-2 text-violet-700 hover:text-violet-900 dark:text-violet-400 dark:hover:text-violet-300"
    >
      {children}
    </a>
  ),
  // react-markdown 10 renders a fenced block as <pre><code>. Styling both means
  // the inline case (no <pre> parent) can keep its own chip treatment while the
  // block case scrolls sideways instead of widening the chat column.
  code: ({ children, className }) => {
    const isBlock = typeof className === "string" && className.startsWith("language-")
    if (isBlock) {
      return <code className="font-mono text-xs">{children}</code>
    }
    return (
      <code className="rounded bg-zinc-100 dark:bg-zinc-800 px-1 py-0.5 font-mono text-[0.85em]">
        {children}
      </code>
    )
  },
  pre: ({ children }) => (
    <pre className="mb-2 last:mb-0 overflow-x-auto rounded-md bg-zinc-100 dark:bg-zinc-800 p-2.5 text-xs">
      {children}
    </pre>
  ),
  // Tables come from remark-gfm. The wrapper scrolls so a wide table never makes
  // the whole chat pane scroll sideways.
  table: ({ children }) => (
    <div className="mb-2 last:mb-0 overflow-x-auto">
      <table className="w-full border-collapse text-xs">{children}</table>
    </div>
  ),
  thead: ({ children }) => <thead className="border-b border-zinc-300 dark:border-zinc-700">{children}</thead>,
  th: ({ children }) => <th className="px-2 py-1 text-left font-semibold align-top">{children}</th>,
  td: ({ children }) => (
    <td className="border-t border-zinc-200 dark:border-zinc-800 px-2 py-1 align-top">{children}</td>
  ),
  blockquote: ({ children }) => (
    <blockquote className="mb-2 last:mb-0 border-l-2 border-zinc-300 dark:border-zinc-700 pl-3 text-zinc-700 dark:text-zinc-300">
      {children}
    </blockquote>
  ),
  hr: () => <hr className="my-3 border-zinc-200 dark:border-zinc-800" />,
}

export function AssistantMarkdown({ content }: { content: string }) {
  return (
    <div className="text-zinc-900 dark:text-zinc-100 break-words">
      <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]} components={components}>
        {content}
      </ReactMarkdown>
    </div>
  )
}
