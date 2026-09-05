package validators

import "strings"

// Extracts the tables a query READS — the FROM and JOIN sources — so a model can be
// told which pipelines produce its inputs without anyone typing them in.
//
// This is deterministic parsing, not an LLM, and that is the point: the result feeds a
// SUGGESTION in the schedule dialog that a person confirms. An inference that is
// occasionally wrong is fine when a human approves it and can see what it found; the
// same inference silently rewiring a schedule would not be.
//
// The failure that actually matters here is a FALSE POSITIVE, not a miss. A missed
// table means the dialog suggests nothing and the user picks the pipeline themselves —
// which is exactly what they do today. An invented table resolves to some unrelated
// pipeline and offers to hang a schedule off it. So every ambiguous construct below
// resolves toward "not a table":
//
//   - a WITH name is a CTE, never a table, even when a real table shares its name
//     (SQL scoping says the CTE wins, so dropping it is also the correct read);
//   - an identifier immediately followed by `(` is a function call, not a table —
//     `FROM generate_series(1, 10)` names no table;
//   - `FROM` inside EXTRACT / SUBSTRING / TRIM / OVERLAY is that function's argument
//     separator. `EXTRACT(MONTH FROM created_at)` reads no table called created_at;
//   - `DELETE FROM t` names what the statement WRITES. Reporting it as an input would
//     claim a model depends on the table it empties.
//
// Comment stripping is shared with the rest of this package rather than reimplemented.
// A second, slightly different comment stripper is how `-- FROM real_table` starts
// being read as a dependency by one caller and not another.

// TableRef is one table named as a source in a query.
//
// Parts holds the identifier as written and unquoted, outermost first: one element for
// `orders`, two for `analytics.orders`, three for `warehouse.analytics.orders`. Keeping
// the parts rather than a single string lets the resolver match against whatever
// qualification the catalog happens to store without re-splitting on a dot that might
// have been inside quotes.
type TableRef struct {
	Parts []string
	// Quoted records that at least one part arrived quoted, so a caller comparing
	// case-insensitively knows it is doing so against an identifier whose case was
	// significant to the engine.
	Quoted bool
}

// Name is the table's own identifier, without any schema or database qualification.
func (r TableRef) Name() string {
	if len(r.Parts) == 0 {
		return ""
	}
	return r.Parts[len(r.Parts)-1]
}

// Qualified is the reference as written, dot-joined: "analytics.orders".
func (r TableRef) Qualified() string {
	return strings.Join(r.Parts, ".")
}

// SchemaQualified is the last two parts, "analytics.orders", or just the name when the
// reference carried no schema. This is the shape table catalogs usually store.
func (r TableRef) SchemaQualified() string {
	if len(r.Parts) < 2 {
		return r.Name()
	}
	return r.Parts[len(r.Parts)-2] + "." + r.Parts[len(r.Parts)-1]
}

// Scalar functions whose argument list uses FROM as a separator. A FROM directly inside
// one of these introduces no table.
var fromTakingFunctions = map[string]bool{
	"extract":   true,
	"substring": true,
	"trim":      true,
	"overlay":   true,
}

// Words that end a FROM clause's table list. Hitting one stops the comma walk, so
// `FROM a, b WHERE x` does not read WHERE as a third table.
var endsTableList = map[string]bool{
	"WHERE": true, "GROUP": true, "ORDER": true, "HAVING": true, "LIMIT": true,
	"OFFSET": true, "FETCH": true, "WINDOW": true, "UNION": true, "INTERSECT": true,
	"EXCEPT": true, "ON": true, "USING": true, "JOIN": true, "INNER": true,
	"LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true, "NATURAL": true,
	"SET": true, "RETURNING": true, "QUALIFY": true, "FOR": true, "INTO": true,
	"TABLESAMPLE": true, "WITH": true, "LATERAL": true, "AS": true,
}

// ExtractTableReferences returns the tables a statement reads, in the order written and
// with duplicates removed. CTE names are excluded. It never returns an error: the worst
// outcome for an unparseable statement is an empty slice, which costs a suggestion and
// nothing else.
func ExtractTableReferences(sqlText string) []TableRef {
	tokens := lexSQLTokens(removeComments(sqlText))
	if len(tokens) == 0 {
		return nil
	}

	ctes := collectCTENames(tokens)

	var refs []TableRef
	seen := map[string]bool{}
	// The innermost enclosing function name at each paren depth, so a FROM can ask
	// what it is nested in. "" for a paren that follows something other than an
	// identifier — a subquery or a plain grouping.
	var parenFuncs []string

	for i := 0; i < len(tokens); i++ {
		t := tokens[i]

		if t.punct == '(' {
			fn := ""
			if i > 0 && tokens[i-1].isIdent() && !tokens[i-1].quoted {
				fn = strings.ToLower(tokens[i-1].text)
			}
			parenFuncs = append(parenFuncs, fn)
			continue
		}
		if t.punct == ')' {
			if len(parenFuncs) > 0 {
				parenFuncs = parenFuncs[:len(parenFuncs)-1]
			}
			continue
		}
		if !t.isIdent() || t.quoted {
			continue
		}

		switch strings.ToUpper(t.text) {
		case "FROM":
			if len(parenFuncs) > 0 && fromTakingFunctions[parenFuncs[len(parenFuncs)-1]] {
				continue // an argument separator, not a clause
			}
			if i > 0 && tokens[i-1].isIdent() && strings.EqualFold(tokens[i-1].text, "DELETE") {
				continue // the statement's write target, not one of its inputs
			}
			found, next := parseTableList(tokens, i+1)
			appendRefs(&refs, seen, ctes, found)
			// Deliberately not jumping to `next`: the skipped span can contain a
			// derived table whose own FROM is a real dependency. Re-walking it costs
			// nothing and missing it would be a silent gap.
			_ = next
		case "JOIN":
			if ref, ok, _ := parseTableRef(tokens, i+1); ok {
				appendRefs(&refs, seen, ctes, []TableRef{ref})
			}
		case "USING":
			// MERGE INTO tgt USING src ON ... — `src` is read, and a model in
			// "statement" mode is exactly where a MERGE shows up.
			//
			// The same word is also JOIN's column list, `JOIN b USING (id)`. Nothing
			// here separates the two because nothing has to: a column list always
			// opens with `(`, and parseTableRef refuses any token that is not an
			// identifier, so `id` is never reached. An explicit paren check was tried
			// and removed — it was unreachable, and a mutation test proved it. A
			// parenthesized MERGE source, `USING (SELECT ...)`, is refused for the
			// same reason and is not lost: the walk continues into those tokens and
			// finds the subquery's own FROM.
			if ref, ok, _ := parseTableRef(tokens, i+1); ok {
				appendRefs(&refs, seen, ctes, []TableRef{ref})
			}
		}
	}

	return refs
}

func appendRefs(refs *[]TableRef, seen map[string]bool, ctes map[string]bool, found []TableRef) {
	for _, r := range found {
		// A single-part name matching a CTE is that CTE. A qualified name never is:
		// CTE names cannot be schema-qualified, so `analytics.daily` is a real table
		// even when a CTE is also called `daily`.
		if len(r.Parts) == 1 && ctes[strings.ToLower(r.Name())] {
			continue
		}
		key := strings.ToLower(r.Qualified())
		if seen[key] {
			continue
		}
		seen[key] = true
		*refs = append(*refs, r)
	}
}

// parseTableList reads the comma-separated sources of one FROM clause, starting at i.
// Returns the refs and the index just past the clause.
func parseTableList(tokens []sqlToken, i int) ([]TableRef, int) {
	var out []TableRef
	for i < len(tokens) {
		ref, ok, next := parseTableRef(tokens, i)
		if ok {
			out = append(out, ref)
		}
		i = next
		// Skip an alias so the comma walk lands on the comma: `FROM a x, b y`.
		if i < len(tokens) && tokens[i].isIdent() && strings.EqualFold(tokens[i].text, "AS") {
			i++
		}
		if i < len(tokens) && tokens[i].isIdent() &&
			(tokens[i].quoted || !endsTableList[strings.ToUpper(tokens[i].text)]) {
			i++ // the alias itself
		}
		if i < len(tokens) && tokens[i].punct == ',' {
			i++
			continue
		}
		return out, i
	}
	return out, i
}

// parseTableRef reads one dotted identifier at i. ok is false when i is not a table:
// a subquery paren, a function call, or a keyword.
func parseTableRef(tokens []sqlToken, i int) (TableRef, bool, int) {
	if i >= len(tokens) || !tokens[i].isIdent() {
		return TableRef{}, false, i
	}
	// LATERAL and ONLY decorate the source that follows them.
	if !tokens[i].quoted {
		switch strings.ToUpper(tokens[i].text) {
		case "LATERAL", "ONLY":
			return parseTableRef(tokens, i+1)
		}
	}
	if !tokens[i].quoted && endsTableList[strings.ToUpper(tokens[i].text)] {
		return TableRef{}, false, i
	}

	ref := TableRef{Parts: []string{tokens[i].text}, Quoted: tokens[i].quoted}
	i++
	for i+1 < len(tokens) && tokens[i].punct == '.' && tokens[i+1].isIdent() {
		ref.Parts = append(ref.Parts, tokens[i+1].text)
		if tokens[i+1].quoted {
			ref.Quoted = true
		}
		i += 2
	}
	// `name(` is a call. Table-valued or scalar, it is not a table this query reads.
	if i < len(tokens) && tokens[i].punct == '(' {
		return TableRef{}, false, i
	}
	return ref, true, i
}

// collectCTENames reads the names bound by a leading WITH clause. Chained and RECURSIVE
// CTEs are handled; a name is recorded before its body is scanned so a recursive
// self-reference is excluded too.
func collectCTENames(tokens []sqlToken) map[string]bool {
	names := map[string]bool{}

	i := 0
	// The WITH must be the statement's first word. A `WITH` appearing later is
	// something else — a WITH clause of a subquery is found by the same scan when we
	// recurse into it, and `CREATE TABLE ... WITH (fillfactor=70)` is storage options.
	for i < len(tokens) && tokens[i].punct == '(' {
		i++
	}
	if i >= len(tokens) || !tokens[i].isIdent() || tokens[i].quoted ||
		!strings.EqualFold(tokens[i].text, "WITH") {
		return names
	}
	i++
	if i < len(tokens) && tokens[i].isIdent() && strings.EqualFold(tokens[i].text, "RECURSIVE") {
		i++
	}

	for i < len(tokens) {
		if !tokens[i].isIdent() {
			return names
		}
		names[strings.ToLower(tokens[i].text)] = true
		i++

		if i < len(tokens) && tokens[i].punct == '(' {
			i = skipParens(tokens, i) // the optional column list
		}
		if i < len(tokens) && tokens[i].isIdent() && strings.EqualFold(tokens[i].text, "AS") {
			i++
		}
		// PostgreSQL 12+ inlining hints sit between AS and the body.
		for i < len(tokens) && tokens[i].isIdent() &&
			(strings.EqualFold(tokens[i].text, "NOT") || strings.EqualFold(tokens[i].text, "MATERIALIZED")) {
			i++
		}
		if i >= len(tokens) || tokens[i].punct != '(' {
			return names
		}
		i = skipParens(tokens, i)

		if i < len(tokens) && tokens[i].punct == ',' {
			i++
			continue
		}
		return names
	}
	return names
}

// skipParens returns the index just past the `)` matching the `(` at i.
func skipParens(tokens []sqlToken, i int) int {
	depth := 0
	for ; i < len(tokens); i++ {
		if tokens[i].punct == '(' {
			depth++
		} else if tokens[i].punct == ')' {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return i
}

// sqlToken is either an identifier/keyword, a string literal, or one punctuation byte.
// Numbers and operators land in punct as their first byte, which is enough: nothing
// here needs to tell `+` from `>=`.
type sqlToken struct {
	text    string // identifiers with their quotes removed; empty for punct
	quoted  bool
	literal bool // a single-quoted string, kept opaque so its contents parse as nothing
	punct   byte // 0 for identifiers and literals
}

func (t sqlToken) isIdent() bool { return t.punct == 0 && !t.literal }

// lexSQLTokens splits SQL into the tokens above. It assumes comments are already gone.
//
// Quoted identifiers are unwrapped rather than dropped, which is the whole reason this
// exists instead of reusing stripStringLiterals: that function blanks double-quoted and
// backticked text, and `FROM "daily orders"` is precisely the text we are here to read.
func lexSQLTokens(sql string) []sqlToken {
	var out []sqlToken

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			continue

		case ch == '\'':
			// Opaque. A literal's contents are data; `WHERE note = 'from orders'`
			// names no table.
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			out = append(out, sqlToken{literal: true})

		case ch == '"' || ch == '`' || ch == '[':
			closer := ch
			if ch == '[' {
				closer = ']' // T-SQL
			}
			var b strings.Builder
			i++
			for i < len(sql) {
				if sql[i] == closer {
					// Doubling escapes the closer: "a""b", [a]]b].
					if i+1 < len(sql) && sql[i+1] == closer {
						b.WriteByte(closer)
						i += 2
						continue
					}
					break
				}
				b.WriteByte(sql[i])
				i++
			}
			out = append(out, sqlToken{text: b.String(), quoted: true})

		case isIdentByte(ch) && !(ch >= '0' && ch <= '9'):
			start := i
			for i < len(sql) && isIdentByte(sql[i]) {
				i++
			}
			out = append(out, sqlToken{text: sql[start:i]})
			i-- // the loop's i++ will step past the byte that ended the identifier

		case ch >= '0' && ch <= '9':
			for i < len(sql) && (isIdentByte(sql[i]) || sql[i] == '.') {
				i++
			}
			i--
			out = append(out, sqlToken{punct: '0'})

		default:
			out = append(out, sqlToken{punct: ch})
		}
	}

	return out
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '$' || c == '#' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
