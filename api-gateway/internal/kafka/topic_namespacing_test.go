package kafka

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// This file is the structural guard for TOPIC namespacing in api-gateway, the
// sibling of consumer_group_namespacing_test.go and built on the same scan.
//
// Why it exists here and not in the orchestrator: the orchestrator has a
// chokepoint. Its kafka.Manager calls kafkaclient.Topic() at Produce,
// ProduceWithHeaders and Consume (manager.go:321, :396, :920), so a topic named
// anywhere in that service lands in the namespace whether or not the author
// thought about it. api-gateway has no such point -- UnifiedProducer, the
// sarama consumer group and the kafka-go readers all take the topic verbatim --
// so here the CALL SITE is the only place qualification can happen, and "the
// author remembered" is the entire mechanism. That is what this test replaces.
//
// Two call sites had already forgotten:
//
//   - internal/notifier/notifier.go subscribed to the bare consts
//     rsync.notifications / rsync.healer.actions / rsync.healer.results. Their
//     producers (healer.go:1318, :1383; cdc_wal_watchdog.go:369) publish through
//     the Manager, so under KAFKA_TOPIC_PREFIX=acme. they wrote acme.rsync.*
//     while this consumer sat on rsync.*. Every Slack and email alert stopped,
//     including the one that would have reported the outage.
//   - internal/handlers/schema_evolution.go published approvals to a bare
//     "rsync.healer.approved-changes" while the healer read the qualified name,
//     so an approved schema change was recorded, reported to the user as
//     applied, and never executed.
//
// Neither is reachable at the default prefix, because Topic() is idempotent by
// prefix match and "rsync." + "rsync.notifications" is a no-op. A unit test
// cannot catch that: the bug exists only in a configuration the suite does not
// run in, and it presents as silence rather than as an error. A structural test
// is the one kind that still sees it.
//
// ---------------------------------------------------------------------------
// What this test decides, and what it trusts
//
// It decides the two shapes the real defects took, both of which are local
// facts a scan can settle without guessing:
//
//	a string literal handed to a Kafka client, and
//	an identifier resolving to a const or package-level var in this module
//	whose declaration is not qualified.
//
// It trusts a topic that arrives as a function PARAMETER, and follows the
// obligation outward to the callers instead -- which is where such a topic is
// actually spelled, and where one of the two shapes above applies again.
//
// The outward walk stops at a call this scan cannot resolve to a declaration:
// deriverKey refuses method calls, because nothing here resolves a receiver's
// type and keying on a bare name would let one package's method qualify
// another's. Methods are handled below by a narrower rule that can only make
// the scan MORE permissive (recognizing a helper as qualifying), never less --
// so an unresolvable call can cost a missed report, never a false one.
// TestTopicScanSeesEveryKafkaBoundary is what keeps that cost visible: it pins
// the files the scan must reach, so a boundary that stops being recognized
// reddens here rather than quietly reducing this file to a no-op.
// ---------------------------------------------------------------------------

// topicQualifierNames are the kafkaclient functions that apply the namespace.
var topicQualifierNames = map[string]bool{"Topic": true, "Topics": true}

// isTopicQualifier reports whether this call IS kafkaclient.Topic/Topics,
// resolved through the file's imports rather than merely spelled that way --
// the same care isQualifier takes, so that a local helper named Topic cannot
// launder a bare literal past the scan.
func isTopicQualifier(call *ast.CallExpr, ctx fileCtx) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !topicQualifierNames[sel.Sel.Name] {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return ctx.imports[pkg.Name] == kafkaclientPath
}

// samePackageMethodKey names a method call as "<this package>.<Method>".
//
// deriverKey returns "" for method calls on purpose. This relaxes that for one
// use only -- recognizing that a helper qualifies -- because api-gateway's
// helpers are methods: the websocket bridge calls b.consumeTopic(topic), the
// notifier calls n.topics.all(), the producer calls p.sendJSON(topic, ...).
// Assuming the receiver's type lives in the calling package is a heuristic, and
// is wrong for a method on an imported type; the direction of that error is
// what makes it acceptable. Being wrong can only make the scan accept a call it
// should have examined, never reject a correct one, and the pinned-boundary
// test below bounds the damage.
func samePackageMethodKey(call *ast.CallExpr, ctx fileCtx) string {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	if id, ok := sel.X.(*ast.Ident); ok {
		if _, isPkg := ctx.imports[id.Name]; isPkg {
			return "" // package-qualified; deriverKey already handles it
		}
	}
	return ctx.pkgPath + "." + sel.Sel.Name
}

// qualifyingFuncs is the set of functions and methods whose body applies the
// namespace, so a CALL to one counts as qualified at the call site. A call site
// is free to name its derivation rather than inline it, and
// resolveNotifierTopics() reads better than the expression wedged into a
// struct literal.
func qualifyingFuncs(files map[string]*ast.File, ctxs map[string]fileCtx) map[string]bool {
	out := map[string]bool{}
	for rel, f := range files {
		ctx := ctxs[rel]
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if callsTopicQualifier(fn.Body, ctx) {
				out[ctx.pkgPath+"."+fn.Name.Name] = true
			}
		}
	}
	return out
}

// callsTopicQualifier reports whether the subtree calls kafkaclient.Topic/Topics
// directly. Deliberately one level deep: it seeds qualifyingFuncs, and taking a
// transitive closure there would let a function that merely mentions a topic
// somewhere count as qualifying every topic it touches.
func callsTopicQualifier(n ast.Node, ctx fileCtx) bool {
	found := false
	ast.Inspect(n, func(node ast.Node) bool {
		if found {
			return false
		}
		if call, ok := node.(*ast.CallExpr); ok && isTopicQualifier(call, ctx) {
			found = true
			return false
		}
		return true
	})
	return found
}

// constSource is a const or package-level var declared in this module, so an
// identifier at a Kafka boundary can be traced back to the expression it was
// declared with. Keyed "<import path>.<Name>".
type constSource struct {
	value ast.Expr
	ctx   fileCtx
	rel   string
	line  int
}

func collectConstSources(files map[string]*ast.File, ctxs map[string]fileCtx, fset *token.FileSet) map[string]constSource {
	out := map[string]constSource{}
	for rel, f := range files {
		ctx := ctxs[rel]
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || (gen.Tok != token.CONST && gen.Tok != token.VAR) {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					out[ctx.pkgPath+"."+name.Name] = constSource{
						value: vs.Values[i], ctx: ctx, rel: rel,
						line: fset.Position(vs.Values[i].Pos()).Line,
					}
				}
			}
		}
	}
	return out
}

// topicSite is one expression handed to a Kafka client as a topic name.
type topicSite struct {
	pos  token.Position
	file string
	how  string
	expr ast.Expr
	fn   *ast.FuncDecl
	ctx  fileCtx
}

// topicFieldNames are the struct fields through which a topic reaches a client
// library: kafka-go's ReaderConfig/WriterConfig/Message and sarama's
// ProducerMessage all spell it Topic; kafka-go's multi-topic reader spells it
// GroupTopics.
var topicFieldNames = map[string]bool{"Topic": true, "GroupTopics": true}

// producerTopicArg maps each api-gateway producer entry point to the position
// of its topic parameter. These are methods, which deriverKey will not resolve,
// so they are named here rather than inferred -- and
// TestTopicScanCoversTheProducerSurface fails if one is renamed away, because a
// producer the scan silently stopped watching is indistinguishable from a
// producer that is clean.
var producerTopicArg = map[string]int{
	"SendPipelineRequest":            0,
	"SendPipelineRequestWithContext": 1,
	"SendPipelineRequestAvro":        1,
	"SendAgentMessage":               1,
	"SendIntentTask":                 1,
}

func collectTopicSites(files map[string]*ast.File, ctxs map[string]fileCtx, fset *token.FileSet) []topicSite {
	var sites []topicSite
	rels := make([]string, 0, len(files))
	for rel := range files {
		rels = append(rels, rel)
	}
	sort.Strings(rels)

	for _, rel := range rels {
		ctx := ctxs[rel]
		for _, decl := range files[rel].Decls {
			fn, _ := decl.(*ast.FuncDecl)
			add := func(how string, e ast.Expr) {
				sites = append(sites, topicSite{
					pos: fset.Position(e.Pos()), file: rel, how: how, expr: e, fn: fn, ctx: ctx,
				})
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.KeyValueExpr:
					if key, ok := n.Key.(*ast.Ident); ok && topicFieldNames[key.Name] {
						add(key.Name+": in a Kafka client config literal", n.Value)
					}
				case *ast.CallExpr:
					sel, ok := n.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					// sarama ConsumerGroup.Consume(ctx, topics, handler)
					if sel.Sel.Name == "Consume" && len(n.Args) == 3 {
						add("the topic list passed to ConsumerGroup.Consume", n.Args[1])
					}
					if idx, ok := producerTopicArg[sel.Sel.Name]; ok && idx < len(n.Args) {
						add(sel.Sel.Name+"'s topic argument", n.Args[idx])
					}
					// api-gateway's own NewConsumer(brokers, topics, groupID),
					// whose loop puts each element straight into a
					// kafka-go ReaderConfig.Topic.
					// Matched by suffix because deriverKey resolves this
					// through the caller's imports: from cmd/server it is
					// <module>/internal/kafka.NewConsumer, from inside the
					// package it is the bare package path.
					if strings.HasSuffix(deriverKey(n, ctx), "internal/kafka.NewConsumer") && len(n.Args) == 3 {
						add("the topic list passed to NewConsumer", n.Args[1])
					}
				}
				return true
			})
		}
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].file != sites[j].file {
			return sites[i].file < sites[j].file
		}
		return sites[i].pos.Line < sites[j].pos.Line
	})
	return sites
}

// unqualified describes one topic name that reaches Kafka without the namespace.
type unqualified struct {
	what   string // how it was written, e.g. `the literal "rsync.notifications"`
	origin string // where it was written, when that differs from the site
}

// findUnqualified walks a sink expression and reports the topic names in it
// that never passed through the qualifier.
//
// It descends until something settles the question. A qualifier call, or a call
// to a function whose body qualifies, ends the walk for that subtree because
// everything below it is namespaced. A string literal or an unqualified module
// const is reported. An identifier naming a parameter, or one assigned in this
// function from a qualified expression, is accepted -- the first because the
// obligation belongs to the caller, the second because it is discharged here.
func findUnqualified(s topicSite, qualifiers map[string]bool, consts map[string]constSource, depth int) []unqualified {
	var out []unqualified
	if depth > 4 { // consts referring to consts; bounded so a cycle cannot hang the test
		return nil
	}
	ast.Inspect(s.expr, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CallExpr:
			if isTopicQualifier(n, s.ctx) {
				return false // namespaced from here down
			}
			if key := deriverKey(n, s.ctx); key != "" && qualifiers[key] {
				return false
			}
			if key := samePackageMethodKey(n, s.ctx); key != "" && qualifiers[key] {
				return false
			}
			return true

		case *ast.BasicLit:
			if n.Kind == token.STRING && strings.Trim(n.Value, `"`) != "" {
				out = append(out, unqualified{what: "the literal " + n.Value})
			}
			return false

		case *ast.Ident:
			// A parameter: the obligation is the caller's, and the caller is
			// itself a site (NewConsumer, or one of the Send* methods).
			if paramIndex(s.fn, n.Name) >= 0 {
				return false
			}
			// A local assigned from something qualified, in this function.
			if assignedQualified(s.fn, n.Name, s.ctx, qualifiers) {
				return false
			}
			// A local assigned from something else: follow it to whatever it
			// was built from. This is the shape the notifier bug took --
			// topics := []string{notifyTopic, ...} handed to Consume -- so
			// stopping at the identifier here would decide nothing and report
			// nothing, which is the failure this whole file exists to prevent.
			if rhs := localAssignments(s.fn, n.Name); len(rhs) > 0 {
				for _, r := range rhs {
					inner := topicSite{expr: r, fn: s.fn, ctx: s.ctx}
					for _, u := range findUnqualified(inner, qualifiers, consts, depth+1) {
						origin := u.origin
						if origin == "" {
							origin = "via " + n.Name
						}
						out = append(out, unqualified{what: u.what, origin: origin})
					}
				}
				return false
			}
			src, ok := consts[s.ctx.pkgPath+"."+n.Name]
			if !ok {
				return false // not resolvable in this module; nothing to decide
			}
			inner := topicSite{expr: src.value, fn: nil, ctx: src.ctx}
			for _, u := range findUnqualified(inner, qualifiers, consts, depth+1) {
				out = append(out, unqualified{
					what:   u.what,
					origin: fmt.Sprintf("%s declared at %s:%d", n.Name, src.rel, src.line),
				})
			}
			return false
		}
		return true
	})
	return out
}

// localAssignments returns the right-hand sides this function assigns to name,
// including the ranged expression of a "for _, name := range x". Flow-
// insensitive, matching assignedQualified: every assignment is a candidate
// source, so a name built once correctly and once not is reported rather than
// excused.
func localAssignments(fn *ast.FuncDecl, name string) []ast.Expr {
	if fn == nil || fn.Body == nil {
		return nil
	}
	var out []ast.Expr
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		var lhs, rhs []ast.Expr
		switch n := node.(type) {
		case *ast.AssignStmt:
			lhs, rhs = n.Lhs, n.Rhs
		case *ast.RangeStmt:
			if n.Value == nil {
				return true
			}
			lhs, rhs = []ast.Expr{n.Value}, []ast.Expr{n.X}
		default:
			return true
		}
		for i, l := range lhs {
			id, ok := l.(*ast.Ident)
			if !ok || id.Name != name {
				continue
			}
			src := rhs[0]
			if i < len(rhs) {
				src = rhs[i]
			}
			// Self-reference (x = append(x, ...)) would recurse forever; the
			// depth bound catches it, but skipping is cheaper and clearer.
			if inner, ok := src.(*ast.Ident); ok && inner.Name == name {
				continue
			}
			out = append(out, src)
		}
		return true
	})
	return out
}

// assignedQualified reports whether name was assigned, anywhere in fn, from an
// expression that qualifies. Flow-insensitive on purpose: it matches the
// group-id scanner, and a function that qualifies a name on one line and not
// another is a shape worth reading rather than one worth modelling.
func assignedQualified(fn *ast.FuncDecl, name string, ctx fileCtx, qualifiers map[string]bool) bool {
	if fn == nil || fn.Body == nil {
		return false
	}
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if found {
			return false
		}
		var lhs, rhs []ast.Expr
		switch n := node.(type) {
		case *ast.AssignStmt:
			lhs, rhs = n.Lhs, n.Rhs
		case *ast.RangeStmt:
			// for _, topic := range topics -- the element inherits whatever the
			// ranged expression was, which is how NewConsumer's loop reads.
			if n.Value == nil {
				return true
			}
			lhs, rhs = []ast.Expr{n.Value}, []ast.Expr{n.X}
		default:
			return true
		}
		for i, l := range lhs {
			id, ok := l.(*ast.Ident)
			if !ok || id.Name != name {
				continue
			}
			src := rhs[0]
			if i < len(rhs) {
				src = rhs[i]
			}
			if exprIsQualified(src, fn, ctx, qualifiers) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// exprIsQualified reports whether an assignment's right-hand side is already
// namespaced -- by calling a qualifier, by calling a helper that does, or by
// naming a parameter whose caller qualified it.
func exprIsQualified(e ast.Expr, fn *ast.FuncDecl, ctx fileCtx, qualifiers map[string]bool) bool {
	ok := false
	ast.Inspect(e, func(node ast.Node) bool {
		if ok {
			return false
		}
		switch n := node.(type) {
		case *ast.CallExpr:
			if isTopicQualifier(n, ctx) {
				ok = true
				return false
			}
			if key := deriverKey(n, ctx); key != "" && qualifiers[key] {
				ok = true
				return false
			}
			if key := samePackageMethodKey(n, ctx); key != "" && qualifiers[key] {
				ok = true
				return false
			}
		case *ast.Ident:
			if paramIndex(fn, n.Name) >= 0 {
				ok = true
				return false
			}
		}
		return true
	})
	return ok
}

// paramIndex returns the position of the named parameter, or -1. Grouped
// parameters (a, b string) are counted individually, matching call-arg order.
func paramIndex(fn *ast.FuncDecl, name string) int {
	if fn == nil || fn.Type == nil || fn.Type.Params == nil {
		return -1
	}
	i := 0
	for _, field := range fn.Type.Params.List {
		if len(field.Names) == 0 {
			i++
			continue
		}
		for _, n := range field.Names {
			if n.Name == name {
				return i
			}
			i++
		}
	}
	return -1
}

// TestEveryTopicIsNamespaced is the property: no topic name reaches a Kafka
// client from api-gateway without passing through kafkaclient.Topic/Topics.
func TestEveryTopicIsNamespaced(t *testing.T) {
	fset, files, ctxs := parseModuleSources(t)
	qualifiers := qualifyingFuncs(files, ctxs)
	consts := collectConstSources(files, ctxs, fset)

	for _, s := range collectTopicSites(files, ctxs, fset) {
		for _, u := range findUnqualified(s, qualifiers, consts, 0) {
			where := ""
			if u.origin != "" {
				where = " (" + u.origin + ")"
			}
			t.Errorf("%s:%d: %s reaches Kafka as %s without kafkaclient.Topic()%s.\n"+
				"    The orchestrator qualifies centrally in kafka.Manager, so under a non-default "+
				"KAFKA_TOPIC_PREFIX this end and that one name different topics. Kafka reports nothing "+
				"for a subscription nobody produces to: the producer writes, the consumer waits, and the "+
				"feature is silently dead in exactly the deployment that configured a prefix.",
				s.file, s.pos.Line, u.what, s.how, where)
		}
	}
}

// TestTopicScanSeesEveryKafkaBoundary keeps the scan from passing vacuously.
//
// Every structural test's real failure mode is matching nothing: a renamed
// field, a swapped client library or a new indirection reduces it to a no-op
// that still reports success. These are the files that talk to Kafka today, and
// this fails if the scan stops reaching one of them.
func TestTopicScanSeesEveryKafkaBoundary(t *testing.T) {
	fset, files, ctxs := parseModuleSources(t)
	sites := collectTopicSites(files, ctxs, fset)

	const wantAtLeast = 6
	if len(sites) < wantAtLeast {
		t.Fatalf("the scan found %d topic site(s), want at least %d — it no longer matches how this "+
			"module talks to Kafka and would pass without checking anything. Found in: %v",
			len(sites), wantAtLeast, topicSiteFiles(sites))
	}
	for _, want := range []string{
		"cmd/server/main.go",                    // the agent/PII consumer topics
		"internal/notifier/notifier.go",         // the alert inbox subscription
		"internal/handlers/schema_evolution.go", // approved-DDL publish
		"internal/websocket/kafka_bridge.go",    // one reader per bridged topic
		"internal/kafka/consumer.go",            // the shared kafka-go reader
	} {
		if !hasTopicSiteIn(sites, want) {
			t.Errorf("no topic site found in %s — either it stopped talking to Kafka (delete this "+
				"expectation) or the scan stopped recognizing its shape (fix the scan). Sites seen: %v",
				want, topicSiteFiles(sites))
		}
	}
}

// TestTopicScanCoversTheProducerSurface pins producerTopicArg to reality, since
// a producer method renamed without updating that map becomes invisible to the
// scan -- and invisible is indistinguishable from clean.
func TestTopicScanCoversTheProducerSurface(t *testing.T) {
	_, files, _ := parseModuleSources(t)

	declared := map[string]bool{}
	for _, f := range files {
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv != nil {
				declared[fn.Name.Name] = true
			}
		}
	}
	for name := range producerTopicArg {
		if !declared[name] {
			t.Errorf("producerTopicArg lists %q but api-gateway declares no method by that name — "+
				"it was renamed or removed, and the scan silently stopped watching that producer path",
				name)
		}
	}
}

func topicSiteFiles(sites []topicSite) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range sites {
		if !seen[s.file] {
			seen[s.file] = true
			out = append(out, s.file)
		}
	}
	sort.Strings(out)
	return out
}

func hasTopicSiteIn(sites []topicSite, file string) bool {
	for _, s := range sites {
		if s.file == file {
			return true
		}
	}
	return false
}

// TestTopicScannerRejectsTheShapesThatShipped is the negative control: a green
// structural test proves nothing until it is shown to redden on the defect.
//
// The fixture reproduces both shapes that reached production -- a bare literal
// handed to a producer, and a local slice built from bare consts handed to
// Consume -- alongside the correct spellings that must keep passing. Without
// this, a scan that quietly stopped deciding anything would look exactly like a
// codebase that is clean. (An earlier draft of this file did precisely that: it
// stopped at the identifier `topics` and reported nothing at all for the
// notifier, which is the bug it was written to catch.)
func TestTopicScannerRejectsTheShapesThatShipped(t *testing.T) {
	fset := token.NewFileSet()
	const src = `package p

import (
	"github.com/rsync-ai/shared/kafkaclient"
	"github.com/segmentio/kafka-go"
)

const (
	notifyTopic   = "rsync.notifications"
	healerActions = "rsync.healer.actions"
)

// The schema_evolution.go shape: a literal straight into a producer.
func strandedLiteral(p *prod) {
	p.SendPipelineRequest("rsync.healer.approved-changes", id, payload)
}

// The notifier.go shape: consts collected into a local, then subscribed.
func strandedConstList(group sarama.ConsumerGroup) {
	topics := []string{notifyTopic, healerActions}
	_ = group.Consume(ctx, topics, handler)
}

func qualifiedInline(p *prod) {
	p.SendPipelineRequest(kafkaclient.Topic("rsync.healer.approved-changes"), id, payload)
}

func qualifiedViaLocal(group sarama.ConsumerGroup) {
	topics := kafkaclient.Topics(notifyTopic, healerActions)
	_ = group.Consume(ctx, topics, handler)
}

func resolveTopics() []string { return kafkaclient.Topics(notifyTopic) }

func qualifiedViaHelper(group sarama.ConsumerGroup) {
	_ = group.Consume(ctx, resolveTopics(), handler)
}

// A parameter is the caller's obligation, not this function's.
func qualifiedViaParam(topic string) {
	_ = kafka.NewReader(kafka.ReaderConfig{Topic: topic})
}
`
	f, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	files := map[string]*ast.File{"fixture.go": f}
	ctxs := buildFileCtxs(files, "example.test")
	qualifiers := qualifyingFuncs(files, ctxs)
	consts := collectConstSources(files, ctxs, fset)
	sites := collectTopicSites(files, ctxs, fset)

	const wantSites = 6
	if len(sites) != wantSites {
		t.Fatalf("found %d sites in the fixture, want %d — the scan no longer recognizes these "+
			"shapes, so the controls below would pass without exercising anything", len(sites), wantSites)
	}

	// Which enclosing function each verdict belongs to, so a miscount cannot be
	// mistaken for the right answer arrived at wrongly.
	got := map[string]int{}
	for _, s := range sites {
		if s.fn == nil {
			t.Fatalf("site at line %d has no enclosing function", s.pos.Line)
		}
		got[s.fn.Name.Name] = len(findUnqualified(s, qualifiers, consts, 0))
	}

	want := map[string]int{
		"strandedLiteral":    1, // the bare literal
		"strandedConstList":  2, // both consts, each traced to its declaration
		"qualifiedInline":    0,
		"qualifiedViaLocal":  0,
		"qualifiedViaHelper": 0,
		"qualifiedViaParam":  0,
	}
	for fn, wantN := range want {
		gotN, ok := got[fn]
		if !ok {
			t.Errorf("no site collected in %s — the scan stopped seeing this shape", fn)
			continue
		}
		if gotN != wantN {
			verb := "accepted"
			if wantN > 0 {
				verb = "rejected"
			}
			t.Errorf("%s: got %d unqualified report(s), want %d — this shape must be %s",
				fn, gotN, wantN, verb)
		}
	}
}

// TestTopicScannerDoesNotAcceptLookalikeQualifiers pins that qualification is
// resolved through the import path, not the spelling. A local helper named
// Topic on some other package must not launder a bare literal, or the guard
// could be disabled by an import that merely reads plausibly.
func TestTopicScannerDoesNotAcceptLookalikeQualifiers(t *testing.T) {
	fset := token.NewFileSet()
	const src = `package p

import (
	kafkaclient "example.test/notthereal/kafkaclient"
)

func lookalike(p *prod) {
	p.SendPipelineRequest(kafkaclient.Topic("rsync.notifications"), id, payload)
}
`
	f, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	files := map[string]*ast.File{"fixture.go": f}
	ctxs := buildFileCtxs(files, "example.test")
	sites := collectTopicSites(files, ctxs, fset)
	if len(sites) != 1 {
		t.Fatalf("found %d sites, want 1", len(sites))
	}
	if n := len(findUnqualified(sites[0], qualifyingFuncs(files, ctxs), collectConstSources(files, ctxs, fset), 0)); n != 1 {
		t.Errorf("got %d unqualified report(s), want 1: a Topic() from an import path that is not %s "+
			"must not count as qualification", n, kafkaclientPath)
	}
}
