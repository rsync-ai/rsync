package execrows

import (
	"strings"
	"testing"
)

// The fragments are spliced into a query with a bare `AND ` in front of them. That
// makes two edits silently catastrophic in ways no call-site test would catch:
//
//   - a leading `AND`/`OR` produces `AND AND …`, a syntax error at runtime on the
//     healer's background ticker, where the only symptom is a warning in a log
//     nobody is reading;
//   - an unparenthesised `OR` binds looser than the surrounding `AND`s and silently
//     WIDENS every query it is spliced into — the zombie sweep would start reaping
//     rows it was never meant to see, and every existing test stays green because
//     the fragment is still "present".
//
// The second is the one worth a test: it fails open, quietly, everywhere at once.
func TestFragmentsAreSafeToAND(t *testing.T) {
	for name, frag := range map[string]string{"NotSynthetic": NotSynthetic, "IsSynthetic": IsSynthetic} {
		f := strings.TrimSpace(frag)

		if f != frag {
			t.Errorf("%s has surrounding whitespace (%q) — keep it bare so call sites control layout", name, frag)
		}
		for _, lead := range []string{"AND", "OR", "WHERE"} {
			if strings.HasPrefix(strings.ToUpper(f), lead+" ") {
				t.Errorf("%s starts with %q — call sites already write `AND `, so this yields `AND %s …`, "+
					"a syntax error thrown on a background ticker", name, lead, lead)
			}
		}
		if strings.Contains(f, ";") {
			t.Errorf("%s contains a semicolon — it is spliced mid-statement", name)
		}
		// An OR is only safe if the whole fragment is wrapped, since `a AND b OR c`
		// parses as `(a AND b) OR c`.
		if strings.Contains(strings.ToUpper(f), " OR ") && !(strings.HasPrefix(f, "(") && strings.HasSuffix(f, ")")) {
			t.Errorf("%s contains an unparenthesised OR — spliced after AND it widens every query "+
				"that uses it, silently. Wrap the whole fragment in parentheses", name)
		}
		if strings.Count(f, "(") != strings.Count(f, ")") {
			t.Errorf("%s has unbalanced parentheses", name)
		}
	}
}

// The fragments must stay exact complements: every executions row satisfies exactly
// one of them. If they drift apart — a different alias, a different column, a NULL-able
// operand — then "rows that are runs" plus "rows that are anchors" stops covering the
// table, and a row can fall through both.
func TestFragmentsAreComplements(t *testing.T) {
	if IsSynthetic == NotSynthetic {
		t.Fatal("IsSynthetic and NotSynthetic are identical")
	}
	if strings.Replace(IsSynthetic, "=", "<>", 1) != NotSynthetic {
		t.Errorf("the fragments are no longer negations of each other:\n  IsSynthetic  = %q\n  NotSynthetic = %q\n"+
			"They must compare the same two columns; otherwise a row can satisfy neither and fall "+
			"out of both the run queries and the anchor queries.", IsSynthetic, NotSynthetic)
	}
	// Both operands are NOT NULL (executions.id is the PK, pipeline_id is a NOT NULL
	// FK), so plain `<>` is total here — no row evaluates to NULL and gets dropped by
	// an AND. A nullable operand would need IS DISTINCT FROM instead.
	for _, col := range []string{"e.id", "e.pipeline_id"} {
		if !strings.Contains(NotSynthetic, col) {
			t.Errorf("NotSynthetic no longer references %s — see the package doc for why these two "+
				"columns are the discriminator", col)
		}
	}
}
