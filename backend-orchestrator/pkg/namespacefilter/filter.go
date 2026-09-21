// Package namespacefilter decides which databases or schemas a connection may
// see (issue #31): all, include (only these) or exclude (all except).
//
// It is the Go twin of shared/mcp-connectors/public/namespace_filter.py; the
// connection modal previews with a TypeScript copy,
// frontend/src/lib/pipeline/namespaceFilter.ts. All three are pinned to the
// same cases in shared/mcp-connectors/public/namespace_filter_vectors.json;
// change the algorithm in all of them, or in none. It lives under pkg/ so
// api-gateway can import it the same way it imports pkg/llmscrub.
//
// Config keys (connection config values are strings):
//   - namespace_filter_mode: "all" (default when missing or empty), "include"
//     or "exclude". Trimmed and case-insensitive. Any other value is an error:
//     never silently "all".
//   - namespace_filter_patterns: comma-separated. Entries are trimmed and empty
//     entries dropped. At most 100 patterns, each at most 128 bytes. '*' matches
//     any run of characters, including none. Every other character is literal.
//     A pattern must match the whole name, case-insensitively.
//
// "include" with no patterns is an error. "exclude" with no patterns keeps
// everything. System namespaces are always rejected first, whatever the mode.
// Nothing matching is a warning for the caller, not an error.
//
// Error and warning messages are fixed text with no names or patterns in them.
package namespacefilter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	ModeKey     = "namespace_filter_mode"
	PatternsKey = "namespace_filter_patterns"

	ModeAll     = "all"
	ModeInclude = "include"
	ModeExclude = "exclude"

	MaxPatterns     = 100
	MaxPatternBytes = 128

	NoMatchWarning = "The namespace filter matched no databases or schemas."
)

// ErrInvalid wraps every parse failure. The caller must fail closed.
var ErrInvalid = errors.New("invalid namespace filter")

// Filter is a parsed namespace filter. The zero value is not valid; use Parse.
type Filter struct {
	Mode     string
	Patterns []string
	compiled []*regexp.Regexp
}

// Result is the outcome of Apply.
type Result struct {
	Kept     []string
	Matched  int
	Excluded int
	// Warning is empty when there is nothing to warn about.
	Warning string
}

func invalid(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalid, msg)
}

func compile(pattern string) *regexp.Regexp {
	parts := strings.Split(pattern, "*")
	for i, p := range parts {
		parts[i] = regexp.QuoteMeta(p)
	}
	return regexp.MustCompile(`(?is)^(?:` + strings.Join(parts, ".*") + `)$`)
}

// Parse reads the filter from a connection config.
func Parse(config map[string]string) (Filter, error) {
	mode := strings.ToLower(strings.TrimSpace(config[ModeKey]))
	if mode == "" {
		mode = ModeAll
	}
	if mode != ModeAll && mode != ModeInclude && mode != ModeExclude {
		return Filter{}, invalid("namespace_filter_mode must be one of: all, include, exclude")
	}

	patterns := []string{}
	for _, p := range strings.Split(config[PatternsKey], ",") {
		if p = strings.TrimSpace(p); p != "" {
			patterns = append(patterns, p)
		}
	}
	if len(patterns) > MaxPatterns {
		return Filter{}, invalid(fmt.Sprintf("namespace_filter_patterns allows at most %d patterns", MaxPatterns))
	}
	for _, p := range patterns {
		if len(p) > MaxPatternBytes {
			return Filter{}, invalid(fmt.Sprintf("each namespace_filter_patterns entry must be at most %d bytes", MaxPatternBytes))
		}
	}
	if mode == ModeInclude && len(patterns) == 0 {
		return Filter{}, invalid("namespace_filter_mode 'include' needs at least one pattern")
	}

	compiled := make([]*regexp.Regexp, len(patterns))
	for i, p := range patterns {
		compiled[i] = compile(p)
	}
	return Filter{Mode: mode, Patterns: patterns, compiled: compiled}, nil
}

// Active reports whether the filter can drop a non-system name.
func (f Filter) Active() bool {
	return f.Mode == ModeInclude || (f.Mode == ModeExclude && len(f.Patterns) > 0)
}

func (f Filter) matchesAny(name string) bool {
	for _, rx := range f.compiled {
		if rx.MatchString(name) {
			return true
		}
	}
	return false
}

// Allowed reports whether name passes: not a system namespace, then the mode.
func (f Filter) Allowed(name string, systemNames []string) bool {
	lowered := strings.ToLower(name)
	for _, s := range systemNames {
		if lowered == strings.ToLower(s) {
			return false
		}
	}
	switch f.Mode {
	case ModeInclude:
		return f.matchesAny(name)
	case ModeExclude:
		return !f.matchesAny(name)
	}
	return true
}

// Apply keeps the allowed names in their input order. Warning is set when an
// active filter leaves nothing, so the caller can tell the user instead of
// failing.
func (f Filter) Apply(names, systemNames []string) Result {
	kept := []string{}
	for _, n := range names {
		if f.Allowed(n, systemNames) {
			kept = append(kept, n)
		}
	}
	r := Result{Kept: kept, Matched: len(kept), Excluded: len(names) - len(kept)}
	if f.Active() && len(kept) == 0 {
		r.Warning = NoMatchWarning
	}
	return r
}
