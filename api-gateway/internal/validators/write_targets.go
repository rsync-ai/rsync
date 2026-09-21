package validators

import "strings"

// Extracts the tables a statement WRITES, so a model that runs its SQL as written
// (materialization='statement') can be recognised as the producer of those tables. It is
// the counterpart of ExtractTableReferences, which finds what a query reads, and it
// shares that file's lexer, comment stripping and CTE rules rather than growing its own.
//
// The result feeds the same two places the read side does: an upstream SUGGESTION a
// person confirms, and the asset graph. So the same rule governs every choice below: a
// missed target costs one suggestion, an invented one offers to hang a schedule off a
// model that never touches the table. Anything this file cannot read with certainty
// resolves toward "names nothing":
//
//   - Only a statement's leading verb is read, plus the bodies of its WITH list. A word
//     like INSERT inside a string, a comment, a dollar-quoted body or a subquery is not
//     where a statement starts and names nothing.
//   - A CTE name is never a target. `WITH s AS (...) INSERT INTO s` is refused by the
//     engines anyway, and a qualified name is still a table, as on the read side.
//   - Temporary tables (`CREATE TEMP TABLE`, T-SQL `#staging`), table variables
//     (`@rows`), remote rowsets (`OPENQUERY(...)`) and four-part linked-server names
//     are not tables of this connection that any other model could read.
//   - Only statements that leave rows behind count: INSERT, REPLACE, UPDATE, MERGE,
//     CREATE TABLE ... AS, CREATE [MATERIALIZED] VIEW and REFRESH MATERIALIZED VIEW.
//     DELETE and TRUNCATE only remove rows, and DROP / ALTER / a bare CREATE TABLE
//     leave none, so a model running them produces nothing a reader could depend on.
//   - SELECT ... INTO is not read. It classifies as a read, and a statement model must
//     be a write, so a model built on it never runs.
//   - A multi-table UPDATE (MySQL `UPDATE a JOIN b ... SET`) names nothing: which of
//     its tables the SET writes is not decidable without resolving column qualifiers.
//
// Known limitation, shared with removeComments and hasMultipleStatements: a backslash
// escape inside a string (MySQL's default, PostgreSQL's E'...') is read with standard
// SQL rules, so such a literal can end early. The runner's single-statement rule
// refuses SQL where that matters, which is also why none of it would run.

// Words that cannot be a write target's name when they appear unquoted. Reaching one
// means the statement has a shape this file does not read — Oracle's `INSERT ALL`, a
// clause keyword after a missing name — and the honest answer is no target.
var notAWriteTarget = map[string]bool{
	"SELECT": true, "VALUES": true, "DEFAULT": true, "ALL": true, "FIRST": true,
	"TOP": true, "IF": true, "OR": true, "TABLE": true, "VIEW": true, "ONLY": true,
}

// ExtractWriteTargets returns the tables a SQL text writes rows into, in the order
// written and with duplicates removed. Several statements separated by `;` are each
// read. Like ExtractTableReferences it never returns an error: SQL it cannot read
// yields an empty slice.
func ExtractWriteTargets(sqlText string) []TableRef {
	tokens := lexSQLTokens(maskDollarQuotes(removeComments(sqlText)))
	if len(tokens) == 0 {
		return nil
	}

	var refs []TableRef
	seen := map[string]bool{}
	for _, stmt := range splitStatements(tokens) {
		for _, r := range statementWriteTargets(stmt) {
			key := strings.ToLower(r.Qualified())
			if seen[key] {
				continue
			}
			seen[key] = true
			refs = append(refs, r)
		}
	}
	return refs
}

// IsSingleStatement reports whether sqlText is one statement by the rule that decides
// whether a model may run at all: ValidateExplorerStatement's, which refuses SQL with
// a `;` anywhere but at its end once comments and string literals are set aside. A
// caller that offers a model as a table's producer checks this first, because a model
// the runner refuses writes nothing, and SQL the two readers could split differently
// — a backslash inside a string, a `;` inside a dollar-quoted body — is exactly SQL
// this rule refuses. Empty and comment-only text is no statement.
func IsSingleStatement(sqlText string) bool {
	stripped := strings.TrimSpace(removeComments(sqlText))
	return stripped != "" && !hasMultipleStatements(stripped)
}

// splitStatements cuts the token stream at every `;` outside parentheses. Empty
// statements (`;;`, a leading `;WITH`) are dropped.
func splitStatements(tokens []sqlToken) [][]sqlToken {
	var out [][]sqlToken
	depth, start := 0, 0
	for i, t := range tokens {
		switch t.punct {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ';':
			if depth == 0 {
				if i > start {
					out = append(out, tokens[start:i])
				}
				start = i + 1
			}
		}
	}
	if start < len(tokens) {
		out = append(out, tokens[start:])
	}
	return out
}

// statementWriteTargets reads one statement: the bodies of a leading WITH list (a
// PostgreSQL data-modifying CTE writes from there) and then the main verb.
func statementWriteTargets(tokens []sqlToken) []TableRef {
	var found []TableRef
	i := 0
	ctes := map[string]bool{}
	if keywordAt(tokens, 0, "WITH") {
		var bodies [][]sqlToken
		ctes, bodies, i = parseWithList(tokens, 1)
		for _, body := range bodies {
			found = append(found, statementWriteTargets(body)...)
		}
	}
	found = append(found, verbWriteTargets(tokens, i)...)

	kept := found[:0]
	for _, r := range found {
		if len(r.Parts) == 1 && ctes[strings.ToLower(r.Name())] {
			continue
		}
		kept = append(kept, r)
	}
	return kept
}

// parseWithList reads the CTE list that starts at i (just past WITH). It returns every
// name bound, each body's tokens, and the index of the statement the list introduces.
// A list it cannot read returns no bodies and an index past the end, so a malformed
// WITH names nothing rather than letting a guess at where the main verb starts through.
func parseWithList(tokens []sqlToken, i int) (map[string]bool, [][]sqlToken, int) {
	names := map[string]bool{}
	var bodies [][]sqlToken
	if keywordAt(tokens, i, "RECURSIVE") {
		i++
	}
	for i < len(tokens) && tokens[i].isIdent() {
		names[strings.ToLower(tokens[i].text)] = true
		i++
		if i < len(tokens) && tokens[i].punct == '(' {
			i = skipParens(tokens, i) // the optional column list
		}
		if keywordAt(tokens, i, "AS") {
			i++
		}
		for keywordAt(tokens, i, "NOT") || keywordAt(tokens, i, "MATERIALIZED") {
			i++
		}
		closeAt, ok := matchingParen(tokens, i)
		if !ok {
			return names, nil, len(tokens)
		}
		bodies = append(bodies, tokens[i+1:closeAt])
		i = closeAt + 1
		if i < len(tokens) && tokens[i].punct == ',' {
			i++
			continue
		}
		return names, bodies, i
	}
	return names, nil, len(tokens)
}

// verbWriteTargets reads the statement whose verb is at i.
func verbWriteTargets(tokens []sqlToken, i int) []TableRef {
	if i >= len(tokens) || !tokens[i].isIdent() || tokens[i].quoted {
		return nil
	}
	switch strings.ToUpper(tokens[i].text) {
	case "INSERT", "REPLACE":
		// INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE] [INTO] t   (MySQL)
		// INSERT [TOP (n) [PERCENT]] [INTO] t                                  (T-SQL)
		// INSERT {INTO | OVERWRITE} [INTO | TABLE] t
		i = skipWords(tokens, i+1, "LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE")
		i = skipTop(tokens, i)
		if keywordAt(tokens, i, "OVERWRITE") {
			i++
			// INSERT OVERWRITE [LOCAL] DIRECTORY 'path' writes files, not a table.
			if keywordAt(tokens, i, "LOCAL") || keywordAt(tokens, i, "DIRECTORY") {
				return nil
			}
		}
		i = skipWords(tokens, i, "INTO")
		// Oracle's `INSERT INTO TABLE(subquery)` then reaches `(` and names nothing.
		i = skipWords(tokens, i, "TABLE")
		return oneWriteTarget(tokens, i)
	case "MERGE":
		// MERGE [TOP (n) [PERCENT]] [INTO] [ONLY] t [[AS] alias] USING ...
		i = skipTop(tokens, i+1)
		i = skipWords(tokens, i, "INTO")
		i = skipWords(tokens, i, "ONLY") // PostgreSQL: not the table's inheritance children
		return oneWriteTarget(tokens, i)
	case "UPDATE":
		return updateWriteTarget(tokens, i+1)
	case "CREATE":
		return createWriteTarget(tokens, i+1)
	case "REFRESH":
		// PostgreSQL: REFRESH MATERIALIZED VIEW [CONCURRENTLY] name
		if keywordAt(tokens, i+1, "MATERIALIZED") && keywordAt(tokens, i+2, "VIEW") {
			return oneWriteTarget(tokens, skipWords(tokens, i+3, "CONCURRENTLY"))
		}
	}
	return nil
}

func oneWriteTarget(tokens []sqlToken, i int) []TableRef {
	if ref, ok, _ := parseWriteTarget(tokens, i); ok {
		return []TableRef{ref}
	}
	return nil
}

// updateWriteTarget reads `UPDATE [LOW_PRIORITY] [IGNORE] [ONLY] [TOP (n)] t
// [WITH (hints)] [[AS] alias] SET ...`.
func updateWriteTarget(tokens []sqlToken, i int) []TableRef {
	i = skipTop(tokens, i)
	i = skipWords(tokens, i, "LOW_PRIORITY", "IGNORE", "ONLY")
	ref, ok, next := parseWriteTarget(tokens, i)
	if !ok {
		return nil
	}
	if keywordAt(tokens, next, "WITH") && next+1 < len(tokens) && tokens[next+1].punct == '(' {
		next = skipParens(tokens, next+1) // T-SQL table hints
	}
	if keywordAt(tokens, next, "AS") {
		next++
	}
	if next < len(tokens) && tokens[next].isIdent() &&
		(tokens[next].quoted || !endsTableList[strings.ToUpper(tokens[next].text)]) {
		next++ // the alias
	}
	// Anything but SET here is a table list — `UPDATE a JOIN b`, `UPDATE a, b` — whose
	// written table only the SET's column qualifiers could tell apart.
	if !keywordAt(tokens, next, "SET") {
		return nil
	}

	// T-SQL names the target by an alias the FROM clause binds:
	// `UPDATE o SET ... FROM dbo.orders o`. Reporting `o` would name a table that does
	// not exist and miss the one that does.
	if len(ref.Parts) == 1 {
		bound, found, isTable := fromClauseBinding(tokens, next, ref.Name())
		if found {
			if !isTable {
				return nil // bound to a derived table or a function, not a table
			}
			ref = bound
		}
	}
	return []TableRef{ref}
}

// fromClauseBinding looks for the depth-0 FROM clause after i and reports what it
// binds to name. found=false means FROM does not bind the name, so the name is the
// table itself (PostgreSQL's `UPDATE t SET ... FROM s`). found=true with isTable=false
// means it binds a subquery or a function call.
func fromClauseBinding(tokens []sqlToken, i int, name string) (ref TableRef, found, isTable bool) {
	depth := 0
	for ; i < len(tokens); i++ {
		switch tokens[i].punct {
		case '(':
			depth++
			continue
		case ')':
			depth--
			continue
		}
		// `a IS DISTINCT FROM b` in a SET expression is a comparison, not a clause.
		if depth == 0 && keywordAt(tokens, i, "FROM") && !keywordAt(tokens, i-1, "DISTINCT") {
			return bindingInFromList(tokens, i+1, name)
		}
	}
	return TableRef{}, false, false
}

// bindingInFromList walks one FROM clause's sources — the first, and each one after a
// comma or JOIN — and returns the one whose exposed name is name: its alias when it has
// one, otherwise its own table name.
//
// It stops at WHERE and RETURNING. Past them a comma separates expressions, and
// `RETURNING t.id, s.name AS t` binds nothing.
//
// The name turning up anywhere else in the list means some part of it this walk does
// not read exposes that name — `CROSS APPLY (SELECT ...) x`, `PIVOT (...) AS x`,
// `FROM @rows x` — so it is reported as bound to something other than a table. So is
// a function call with no alias, which is known by its function's name. A column
// after a dot (`ON a.status = b.status`) and a variable (`@status`) expose nothing and
// are passed over.
func bindingInFromList(tokens []sqlToken, i int, name string) (TableRef, bool, bool) {
	expectSource := true
	for i < len(tokens) {
		t := tokens[i]
		if expectSource {
			expectSource = false
			var ref TableRef
			isTable := false
			call := "" // the function a call source with no alias is known by
			switch r, ok, next := parseTableRef(tokens, i); {
			case t.punct == '(':
				i = skipParens(tokens, i) // derived table
			case ok:
				ref, isTable, i = r, true, next
			case next > i && next < len(tokens) && tokens[next].punct == '(':
				call = tokens[next-1].text
				i = skipParens(tokens, next) // table-valued function call
			default:
				continue // not a source at all; read this token as clause text
			}
			if keywordAt(tokens, i, "AS") {
				i++
			}
			if i < len(tokens) && tokens[i].isIdent() &&
				(tokens[i].quoted || !endsTableList[strings.ToUpper(tokens[i].text)]) {
				if strings.EqualFold(tokens[i].text, name) {
					return ref, true, isTable
				}
				i++
				continue
			}
			if isTable && strings.EqualFold(ref.Name(), name) {
				return ref, true, true
			}
			if call != "" && strings.EqualFold(call, name) {
				return TableRef{}, true, false
			}
			continue
		}

		switch {
		case t.punct == '(':
			i = skipParens(tokens, i)
			continue
		case t.punct == ',', keywordAt(tokens, i, "JOIN"):
			expectSource = true
		case keywordAt(tokens, i, "WHERE"), keywordAt(tokens, i, "RETURNING"):
			return TableRef{}, false, false
		// tokens[i-1] exists: FROM itself comes before the list.
		case t.isIdent() && strings.EqualFold(t.text, name) &&
			tokens[i-1].punct != '.' && tokens[i-1].punct != '@':
			return TableRef{}, true, false
		}
		i++
	}
	return TableRef{}, false, false
}

// createWriteTarget reads the CREATE forms that leave rows behind:
//
//	CREATE [OR REPLACE | OR ALTER] [UNLOGGED] TABLE [IF NOT EXISTS] t [(cols)] ... AS query
//	CREATE [OR REPLACE | OR ALTER] [MATERIALIZED] VIEW [IF NOT EXISTS] v ...
//
// A prefix word it does not know — TEMP, GLOBAL, an INDEX, FUNCTION or PUBLICATION
// being created — names nothing. An allowlist is the point: `CREATE PUBLICATION p FOR
// TABLE t` contains the word TABLE and creates no table.
func createWriteTarget(tokens []sqlToken, i int) []TableRef {
	for i < len(tokens) && tokens[i].isIdent() && !tokens[i].quoted {
		switch strings.ToUpper(tokens[i].text) {
		case "OR", "REPLACE", "ALTER", "UNLOGGED", "MATERIALIZED":
			i++
		case "TABLE":
			i = skipIfNotExists(tokens, i+1)
			ref, ok, next := parseWriteTarget(tokens, i)
			if !ok || !populatesTable(tokens, next) {
				return nil
			}
			return []TableRef{ref}
		case "VIEW":
			return oneWriteTarget(tokens, skipIfNotExists(tokens, i+1))
		default:
			return nil
		}
	}
	return nil
}

// populatesTable reports whether a CREATE TABLE fills the table from a query. Without
// one it creates an empty table — `CREATE TABLE t (id int)`, and equally T-SQL's
// `CREATE TABLE t (id int) AS NODE` — and nothing downstream reads rows from that.
func populatesTable(tokens []sqlToken, i int) bool {
	depth := 0
	for ; i < len(tokens); i++ {
		switch tokens[i].punct {
		case '(':
			depth++
			continue
		case ')':
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		// MySQL allows the query without AS: `CREATE TABLE t SELECT ...`.
		if keywordAt(tokens, i, "SELECT") {
			return true
		}
		if keywordAt(tokens, i, "AS") && i+1 < len(tokens) &&
			(tokens[i+1].punct == '(' || keywordAt(tokens, i+1, "WITH") ||
				keywordAt(tokens, i+1, "VALUES") || keywordAt(tokens, i+1, "TABLE") ||
				keywordAt(tokens, i+1, "EXECUTE")) {
			return true
		}
	}
	return false
}

// parseWriteTarget reads the table name at i. Unlike parseTableRef it accepts a column
// list after the name — `INSERT INTO t (a, b)` — and tells that apart from a call such
// as `OPENQUERY(srv, '...')` by what is inside the parentheses.
func parseWriteTarget(tokens []sqlToken, i int) (TableRef, bool, int) {
	if i >= len(tokens) || !tokens[i].isIdent() {
		return TableRef{}, false, i // `@table_variable`, a literal, a subquery
	}
	if isClauseWord(tokens[i]) {
		return TableRef{}, false, i
	}

	ref := TableRef{Parts: []string{tokens[i].text}, Quoted: tokens[i].quoted}
	i++
	for i < len(tokens) && tokens[i].punct == '.' {
		// `db..table` (T-SQL default schema) or a trailing dot: which schema is meant
		// is not written down, so do not guess one. `warehouse. SELECT` is the same
		// trailing dot followed by the next clause, not a table named SELECT.
		if i+1 >= len(tokens) || !tokens[i+1].isIdent() || isClauseWord(tokens[i+1]) {
			return TableRef{}, false, i
		}
		ref.Parts = append(ref.Parts, tokens[i+1].text)
		if tokens[i+1].quoted {
			ref.Quoted = true
		}
		i += 2
	}
	// server.database.schema.table is a linked server: another warehouse entirely.
	if len(ref.Parts) > 3 {
		return TableRef{}, false, i
	}
	// #staging and ##shared are T-SQL temporary tables.
	if strings.HasPrefix(ref.Name(), "#") {
		return TableRef{}, false, i
	}
	if i < len(tokens) && tokens[i].punct == '(' && !isColumnListOrQuery(tokens, i) {
		return TableRef{}, false, i
	}
	return ref, true, i
}

// isClauseWord reports whether an unquoted word starts or ends a clause rather than
// naming a table. Quoting makes any word a name.
func isClauseWord(tok sqlToken) bool {
	if tok.quoted {
		return false
	}
	upper := strings.ToUpper(tok.text)
	return notAWriteTarget[upper] || endsTableList[upper]
}

// isColumnListOrQuery reports whether the parentheses at open hold a column list —
// only identifiers separated by commas — or a parenthesized query, as in
// `INSERT INTO t (SELECT ...)`. A call's arguments hold literals, variables or nothing.
func isColumnListOrQuery(tokens []sqlToken, open int) bool {
	if keywordAt(tokens, open+1, "SELECT") || keywordAt(tokens, open+1, "WITH") {
		return true
	}
	wantIdent := true
	for j := open + 1; j < len(tokens); j++ {
		if wantIdent {
			if !tokens[j].isIdent() {
				return false
			}
			wantIdent = false
			continue
		}
		switch tokens[j].punct {
		case ',':
			wantIdent = true
		case ')':
			return true
		default:
			return false
		}
	}
	return false
}

// matchingParen returns the index of the `)` closing the `(` at open.
func matchingParen(tokens []sqlToken, open int) (int, bool) {
	if open >= len(tokens) || tokens[open].punct != '(' {
		return 0, false
	}
	depth := 0
	for j := open; j < len(tokens); j++ {
		switch tokens[j].punct {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return j, true
			}
		}
	}
	return 0, false
}

// keywordAt reports whether the token at i is the unquoted word kw. Out of range is
// simply false.
func keywordAt(tokens []sqlToken, i int, kw string) bool {
	return i >= 0 && i < len(tokens) && tokens[i].isIdent() && !tokens[i].quoted &&
		strings.EqualFold(tokens[i].text, kw)
}

// skipWords steps past any run of the given unquoted words, in any order.
func skipWords(tokens []sqlToken, i int, words ...string) int {
	for {
		matched := false
		for _, w := range words {
			if keywordAt(tokens, i, w) {
				i++
				matched = true
			}
		}
		if !matched {
			return i
		}
	}
}

// skipTop steps past T-SQL's `TOP (n) [PERCENT]`.
func skipTop(tokens []sqlToken, i int) int {
	if !keywordAt(tokens, i, "TOP") {
		return i
	}
	i++
	if i < len(tokens) && tokens[i].punct == '(' {
		i = skipParens(tokens, i)
	} else if i < len(tokens) && tokens[i].punct == '0' {
		i++
	}
	return skipWords(tokens, i, "PERCENT")
}

func skipIfNotExists(tokens []sqlToken, i int) int {
	if keywordAt(tokens, i, "IF") && keywordAt(tokens, i+1, "NOT") && keywordAt(tokens, i+2, "EXISTS") {
		return i + 3
	}
	return i
}

// maskDollarQuotes replaces each PostgreSQL dollar-quoted string ($$...$$, $tag$...$tag$)
// with an empty literal. The shared lexer reads `$` as an identifier character, so
// without this a function body or a string like $$x; INSERT INTO t ...$$ would lex as
// statements. An unterminated one masks everything after it: its end is unknowable,
// and the engine refuses the SQL anyway.
//
// It runs after removeComments, which knows nothing of dollar quotes and so can only
// ever delete text from inside one. Quoted strings and identifiers are copied as they
// are, so a `$` inside them opens nothing.
func maskDollarQuotes(sql string) string {
	if !strings.Contains(sql, "$") {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql))
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case ch == '\'' || ch == '"' || ch == '`' || ch == '[':
			closer := ch
			if ch == '[' {
				closer = ']'
			}
			end := strings.IndexByte(sql[i+1:], closer)
			if end < 0 {
				b.WriteString(sql[i:])
				return b.String()
			}
			b.WriteString(sql[i : i+end+2])
			i += end + 1
		case ch == '$' && (i == 0 || !isIdentByte(sql[i-1])):
			tagEnd := dollarTagEnd(sql, i)
			if tagEnd < 0 {
				b.WriteByte(ch) // `$1`, `$action`: a parameter or a name, not a quote
				continue
			}
			tag := sql[i : tagEnd+1]
			b.WriteString("''")
			closeAt := strings.Index(sql[tagEnd+1:], tag)
			if closeAt < 0 {
				return b.String()
			}
			i = tagEnd + closeAt + len(tag)
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}

// dollarTagEnd returns the index of the `$` that closes an opening dollar-quote tag
// starting at i, or -1 when i does not open one. A tag is empty or an identifier that
// does not start with a digit.
func dollarTagEnd(sql string, i int) int {
	for j := i + 1; j < len(sql); j++ {
		c := sql[j]
		switch {
		case c == '$':
			return j
		case c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		case c >= '0' && c <= '9' && j > i+1:
		default:
			return -1
		}
	}
	return -1
}
