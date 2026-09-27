package transforms

import (
	"fmt"
	"strconv"
	"strings"
)

// A filter condition is a boolean expression over one row. This file owns the
// whole of it: splitting the text into leaf comparisons, naming the columns it
// reads, and evaluating it. engine.go keeps only the leaf comparison itself
// (evaluateLeaf / compareValue), because that is where the type coercions live.
//
// WHY THERE IS A PARSER AT ALL. The engine used to hold one regex,
//
//	^(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$
//
// whose third group is greedy to end-of-string, so the Filter Condition field's
// OWN placeholder in the UI -- "amount > 100 AND status = 'active'"
// (frontend/src/app/(dashboard)/transforms/page.tsx) -- parsed as the single
// comparison `amount > "100 AND status = 'active"`. That right-hand side never
// equals anything, so the step emitted zero rows for every batch, with no error
// and no warning. The applyFilter comment promising that an unparseable
// condition fails loudly was true only of a condition the regex REJECTS; this
// one it accepts, with the wrong meaning.
//
// So the product already advertised AND/OR and the engine silently deleted the
// data instead. Supporting them is what makes the promise true, not an
// extension of it.

// conditionExpr is a parsed filter condition in disjunctive normal form: the
// OR of a list of AND-groups, matching SQL's precedence (AND binds tighter).
type conditionExpr struct {
	orGroups [][]string
}

// parseCondition splits a condition into its leaf comparisons.
//
// Splitting is quote-aware, so the literal in `note = 'read AND write'` is not
// mistaken for a conjunction. Parentheses are REJECTED rather than ignored:
// without grouping support, "a = 1 AND (b = 2 OR c = 3)" would silently be read
// with the wrong precedence, and a wrong answer delivered confidently is the
// failure mode this whole file exists to remove.
func parseCondition(condition string) (conditionExpr, error) {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return conditionExpr{}, nil
	}
	if idx := indexOutsideQuotes(condition, "("); idx >= 0 {
		return conditionExpr{}, fmt.Errorf(
			"parenthesised conditions are not supported: %q. Write it as an OR of AND-groups, or use two filter steps", condition)
	}

	expr := conditionExpr{}
	for _, orPart := range splitKeyword(condition, "OR") {
		group := []string{}
		for _, andPart := range splitKeyword(orPart, "AND") {
			leaf := strings.TrimSpace(andPart)
			if leaf == "" {
				return conditionExpr{}, fmt.Errorf("empty operand in condition %q", condition)
			}
			group = append(group, leaf)
		}
		if len(group) == 0 {
			return conditionExpr{}, fmt.Errorf("empty operand in condition %q", condition)
		}
		expr.orGroups = append(expr.orGroups, group)
	}
	return expr, nil
}

// splitKeyword splits s on a whitespace-delimited, case-insensitive keyword that
// occurs outside quotes. A keyword inside a quoted literal, or embedded in an
// identifier ("brand" contains "and"), is not a separator.
func splitKeyword(s, keyword string) []string {
	parts := []string{}
	start := 0
	inQuote := rune(0)
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case inQuote != 0:
			if c == inQuote {
				inQuote = 0
			}
			continue
		case c == '\'' || c == '"':
			inQuote = c
			continue
		}
		if !isSpace(c) {
			continue
		}
		// A separator is <space><keyword><space>. Anchoring on the leading space
		// is what keeps "brand = 'x'" and "a = 1 AND b = 2" apart.
		j := i + 1
		for j < len(runes) && isSpace(runes[j]) {
			j++
		}
		if j+len(keyword) > len(runes) {
			continue
		}
		if !strings.EqualFold(string(runes[j:j+len(keyword)]), keyword) {
			continue
		}
		after := j + len(keyword)
		if after >= len(runes) || !isSpace(runes[after]) {
			continue
		}
		parts = append(parts, string(runes[start:i]))
		start = after
		i = after
	}
	parts = append(parts, string(runes[start:]))
	return parts
}

// indexOutsideQuotes reports the first index of sub in s that is not inside a
// quoted literal, or -1.
func indexOutsideQuotes(s, sub string) int {
	inQuote := rune(0)
	runes := []rune(s)
	subRunes := []rune(sub)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		if inQuote != 0 {
			if c == inQuote {
				inQuote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			inQuote = c
			continue
		}
		if i+len(subRunes) <= len(runes) && string(runes[i:i+len(subRunes)]) == sub {
			return i
		}
	}
	return -1
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

// conditionColumns names every column a condition reads, in order of first
// appearance. It is how applyFilter can ask, BEFORE dropping a single row,
// whether the batch even carries the columns the operator filtered on.
//
// A leaf whose shape the engine does not recognise contributes no column rather
// than an error: this is used for diagnosis, and evaluateCondition is the one
// that gets to reject a condition.
func conditionColumns(condition string) []string {
	expr, err := parseCondition(condition)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	out := []string{}
	for _, group := range expr.orGroups {
		for _, leaf := range group {
			col := leafColumn(leaf)
			if col == "" || seen[col] {
				continue
			}
			seen[col] = true
			out = append(out, col)
		}
	}
	return out
}

// leafColumn returns the column a single comparison reads, or "".
func leafColumn(leaf string) string {
	if m := likeRe.FindStringSubmatch(leaf); len(m) == 3 {
		return m[1]
	}
	if m := condRe.FindStringSubmatch(leaf); m != nil {
		return m[1]
	}
	return ""
}

// likeToRegex converts a SQL LIKE pattern to an anchored regexp.
//
// Every character except the two wildcards is escaped. Interpolating the raw
// pattern -- which is what this replaced -- made every regexp metacharacter
// live: 'a.b' matched "axb", '(' was a syntax error that failed the whole
// filter, and '.*' silently matched everything. A LIKE pattern is not a regex,
// and the user did not type one.
func likeToRegex(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(quoteMetaRune(r))
		}
	}
	b.WriteString("$")
	return b.String()
}

func quoteMetaRune(r rune) string {
	if strings.ContainsRune(`\.+*?()|[]{}^$`, r) {
		return `\` + string(r)
	}
	return string(r)
}

// integerValue reports v as an int64 when it is an integer the comparison can
// use without loss.
//
// The float64 path this sits in front of is exact only below 2^53. Above it,
// adjacent int64s share a float64, so `id = 9007199254740993` matched row
// 9007199254740992 -- the filter selected the WRONG row and reported a match.
// Snowflake ids, Twitter-style ids and bigserial columns past ~9e15 all live in
// that range.
func integerValue(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		if n > 1<<63-1 {
			return 0, false
		}
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		if n > 1<<63-1 {
			return 0, false
		}
		return int64(n), true
	case string:
		return integerString(n)
	}
	// json.Number and the float types go through integerString/float on purpose:
	// a float64 that has already lost precision cannot regain it here.
	if s, ok := v.(interface{ String() string }); ok {
		return integerString(s.String())
	}
	return 0, false
}

func integerString(s string) (int64, bool) {
	i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return i, err == nil
}
