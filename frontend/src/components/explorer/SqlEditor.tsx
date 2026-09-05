"use client"

import { forwardRef, useImperativeHandle, useMemo, useRef } from "react"
import CodeMirror, { EditorView, keymap, type ReactCodeMirrorRef } from "@uiw/react-codemirror"
import { sql, PostgreSQL, MySQL, StandardSQL, type SQLDialect } from "@codemirror/lang-sql"
import { autocompletion } from "@codemirror/autocomplete"
import { Prec } from "@codemirror/state"
import { oneDark } from "@codemirror/theme-one-dark"
import { useTheme } from "next-themes"
import { buildSchemaLookup, type ForeignKeyLike } from "@/lib/explorer/sqlCompletions"
import {
  buildSqlCompletionSources,
  createSchemaCompletionSource,
} from "@/lib/explorer/sqlCompletionSources"

// Public types that match the existing TableMetadata / ColumnMetadata
// shapes in `(dashboard)/explorer/page.tsx` so we can pass them through
// directly without remapping.
export interface SqlEditorColumn {
  name: string
  type?: string
  is_primary_key?: boolean
}

export interface SqlEditorTable {
  name: string
  schema?: string
  columns?: SqlEditorColumn[]
}

export interface SqlEditorProps {
  value: string
  onChange: (value: string) => void
  /** Schema metadata for autocomplete. Pass an empty array while loading. */
  tables?: SqlEditorTable[]
  /** Foreign keys for join-key hints offered after `ON` in a JOIN. */
  foreignKeys?: ForeignKeyLike[]
  /** Connector dialect — switches CodeMirror's SQL parser per-engine. */
  dialect?: "postgresql" | "mysql" | "redshift" | "databricks" | "generic"
  /** Triggered on Cmd/Ctrl+Enter so callers can run the query without
   *  clicking the button. */
  onSubmit?: () => void
  /** Triggered on Cmd/Ctrl+. so callers can cancel an in-flight query. */
  onCancel?: () => void
  /** Fired when the editor gains focus. */
  onFocus?: () => void
  /** Fired when the editor loses focus. */
  onBlur?: () => void
  /** Fired when the caret moves or the selection changes, with document offsets
   *  (`from === to` means a bare caret). The Explorer uses this to resolve which
   *  single statement a Run action targets — see lib/explorer/sqlStatements. Only
   *  fired when the range actually changes, so it is safe to hold in state. */
  onSelectionChange?: (selection: { from: number; to: number }) => void
  placeholder?: string
  /** When true, the editor renders read-only (e.g. while a query runs). */
  readOnly?: boolean
  /** Fixed pixel height. When omitted, the editor auto-grows between
   *  `minHeight` and `maxHeight` so short queries don't leave empty space. */
  height?: number
  /** Min auto-grow height in px (default 72 ≈ 3 lines). Ignored when `height` is set. */
  minHeight?: number
  /** Max auto-grow height in px before the editor scrolls (default 400). */
  maxHeight?: number
  /** Optional className applied to the wrapper div. */
  className?: string
  /** Hides line numbers + the gutter for compact use. */
  minimal?: boolean
}

/**
 * Imperative handle exposed via React ref. Lets callers insert text
 * at the cursor (DX-ClickToInsert) and focus the editor — without
 * leaking CodeMirror internals to the parent.
 */
export interface SqlEditorHandle {
  /** Insert `text` at the current cursor position, replacing any
   *  selection. The editor receives focus and the caret is moved to
   *  the end of the inserted text. */
  insertAtCursor: (text: string) => void
  /** Move keyboard focus to the editor. */
  focus: () => void
  /** Current caret/selection as document offsets, or null when the view is not
   *  mounted. `from === to` means a bare caret with no selection. */
  getSelection: () => { from: number; to: number } | null
}

/**
 * SqlEditor — CodeMirror 6 SQL editor with schema-aware autocomplete.
 *
 * Why CodeMirror 6 (vs Monaco):
 *  - ~150KB gzipped vs Monaco's ~600KB+
 *  - Native React integration via @uiw/react-codemirror
 *  - Good SQL dialect support out of the box
 *  - Mobile-friendly (Monaco doesn't ship a touch keyboard story)
 *
 * Autocomplete strategy:
 *  - After FROM / JOIN: suggest tables (schema-qualified if available)
 *  - After "tablename." or "alias.": suggest that table's columns
 *  - Anywhere else: tables + columns + SQL keywords (keywords come from
 *    the dialect keyword source, paired in via buildSqlCompletionSources)
 *
 * Keyboard shortcuts:
 *  - Cmd/Ctrl+Enter → onSubmit (run query)
 *  - Cmd/Ctrl+.     → onCancel (kill running query)
 *  - Ctrl+Space     → trigger autocomplete manually
 */
export const SqlEditor = forwardRef<SqlEditorHandle, SqlEditorProps>(function SqlEditor(props, ref) {
  const {
    value,
    onChange,
    tables = [],
    foreignKeys = [],
    dialect = "postgresql",
    onSubmit,
    onCancel,
    onFocus,
    onBlur,
    onSelectionChange,
    placeholder,
    readOnly,
    height,
    minHeight = 72,
    maxHeight = 400,
    className,
    minimal,
  } = props

  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === "dark"

  // The @uiw wrapper exposes a ref to the underlying editor view. We
  // use it to implement the imperative handle without exposing
  // CodeMirror types to the parent.
  const cmRef = useRef<ReactCodeMirrorRef>(null)

  useImperativeHandle(
    ref,
    () => ({
      insertAtCursor: (text: string) => {
        const view = cmRef.current?.view
        if (!view) return
        const { from, to } = view.state.selection.main
        view.dispatch({
          changes: { from, to, insert: text },
          // Move the caret to immediately after the inserted text.
          selection: { anchor: from + text.length },
          scrollIntoView: true,
        })
        view.focus()
      },
      focus: () => {
        cmRef.current?.view?.focus()
      },
      getSelection: () => {
        const view = cmRef.current?.view
        if (!view) return null
        const { from, to } = view.state.selection.main
        return { from, to }
      },
    }),
    [],
  )

  // Last range handed to onSelectionChange. CodeMirror fires an update on every
  // keystroke; without this the parent would re-render on pure document edits
  // that left the range alone.
  const lastSelectionRef = useRef<{ from: number; to: number }>({ from: 0, to: 0 })

  // Resolve a CodeMirror SQL dialect from our string union. Falls back
  // to StandardSQL for engines we don't have a dedicated parser for
  // (databricks, redshift) — keywords + syntax highlighting still work.
  const sqlDialect: SQLDialect = useMemo(() => {
    switch (dialect) {
      case "mysql":
        return MySQL
      case "postgresql":
      case "redshift":
        return PostgreSQL
      default:
        return StandardSQL
    }
  }, [dialect])

  // Schema-aware completion is computed by the pure `sqlCompletions` /
  // `sqlCompletionSources` modules (unit-tested independently). The source
  // resolves alias/table/schema-qualified columns after a dot, a schema's
  // tables after `schema.`, and tables + columns + FK join-keys at a word
  // boundary. Memoized so we don't rebuild it on every keystroke.
  const schemaLookup = useMemo(() => buildSchemaLookup(tables), [tables])
  const schemaCompletions = useMemo(
    () => createSchemaCompletionSource(schemaLookup, foreignKeys),
    [schemaLookup, foreignKeys],
  )

  // Use the dialect-aware sql() language extension and inject our
  // schema-aware completion source alongside the dialect's built-in
  // keyword completer.
  const extensions = useMemo(() => {
    const exts = [
      sql({
        dialect: sqlDialect,
        upperCaseKeywords: true,
      }),
      autocompletion({
        // Completion sources: our schema-aware source (tables / columns /
        // FK-join hints) plus the dialect keyword completer. `override`
        // REPLACES the language-data sources, so the keyword source that
        // sql() registers must be re-added explicitly — otherwise SELECT /
        // WHERE / JOIN / … never complete. See buildSqlCompletionSources.
        override: buildSqlCompletionSources(schemaCompletions, sqlDialect),
        defaultKeymap: true,
      }),
      EditorView.lineWrapping,
    ]

    // Focus / blur — wired through CodeMirror's editor DOM rather than
    // the wrapping div so we observe the actual contenteditable
    // gaining/losing focus, not stray clicks on padding.
    if (onFocus || onBlur) {
      exts.push(
        EditorView.domEventHandlers({
          focus: () => {
            onFocus?.()
            return false
          },
          blur: () => {
            onBlur?.()
            return false
          },
        }),
      )
    }

    // Caret / selection tracking. The Explorer needs it to decide which single
    // statement Run should execute when the buffer holds several.
    if (onSelectionChange) {
      exts.push(
        EditorView.updateListener.of((update) => {
          const { from, to } = update.state.selection.main
          const last = lastSelectionRef.current
          if (last.from === from && last.to === to) return
          lastSelectionRef.current = { from, to }
          onSelectionChange({ from, to })
        }),
      )
    }

    // High-priority keybindings so Cmd/Ctrl+Enter beats the default
    // newline binding from @codemirror/commands.
    if (onSubmit || onCancel) {
      exts.push(
        Prec.highest(
          keymap.of([
            {
              key: "Mod-Enter",
              preventDefault: true,
              run: () => {
                onSubmit?.()
                return true
              },
            },
            {
              key: "Mod-.",
              preventDefault: true,
              run: () => {
                onCancel?.()
                return true
              },
            },
          ]),
        ),
      )
    }

    return exts
  }, [sqlDialect, schemaCompletions, onSubmit, onCancel, onFocus, onBlur, onSelectionChange])

  return (
    <div className={className}>
      <CodeMirror
        ref={cmRef}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        height={height != null ? `${height}px` : undefined}
        minHeight={`${minHeight}px`}
        maxHeight={`${maxHeight}px`}
        theme={isDark ? oneDark : "light"}
        editable={!readOnly}
        readOnly={readOnly}
        extensions={extensions}
        basicSetup={{
          lineNumbers: !minimal,
          foldGutter: !minimal,
          highlightActiveLine: true,
          highlightActiveLineGutter: !minimal,
          autocompletion: false, // we wire our own above
          bracketMatching: true,
          closeBrackets: true,
          indentOnInput: true,
          tabSize: 2,
        }}
      />
    </div>
  )
})
