package kafka

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// This file is the structural guard for consumer-group namespacing in
// api-gateway.
//
// Why a source scan rather than a list of expected group ids: the failure this
// prevents is a NEW consumer, added later, that builds its group id as a string
// literal the way all five of these used to. A test that asserts the five known
// ids keeps passing when a sixth is added — the stranded one is precisely the
// one it does not know about. Scanning the module's own sources means the
// invariant is checked against the code rather than against a copy of it.
//
// The failure being prevented is silent. On a customer-managed cluster the
// operator grants ACLs; a PREFIXED grant on "rsync." covers every qualified
// group id and nothing else. Kafka answers an unauthorized JoinGroup with an
// authorization error that kafka-go and sarama both surface as a retrying
// consumer, not a crash — so a stranded group id presents as a consumer that
// simply never receives anything while the process stays healthy.

// findAPIGatewayModuleRoot walks up from the test's working directory to the
// directory holding go.mod, which is the api-gateway module root.
func findAPIGatewayModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find go.mod above %q", dir)
		}
		dir = parent
	}
}

// modulePathOf reads the module path out of go.mod. Import paths inside the
// module are built from it, and a call can only be resolved to a deriver once
// the package it names is known.
func modulePathOf(t *testing.T, root string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module "))
		}
	}
	t.Fatalf("no module line in %s/go.mod", root)
	return ""
}

// parseModuleSources parses every non-test .go file in the module.
func parseModuleSources(t *testing.T) (*token.FileSet, map[string]*ast.File, map[string]fileCtx) {
	t.Helper()
	root := findAPIGatewayModuleRoot(t)
	fset := token.NewFileSet()
	files := make(map[string]*ast.File)

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "vendor" || info.Name() == ".git" || info.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			return perr
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			rel = path
		}
		files[rel] = f
		return nil
	})
	if err != nil {
		t.Fatalf("parsing the api-gateway module: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("parsed no source files; the scan below would pass vacuously")
	}
	return fset, files, buildFileCtxs(files, modulePathOf(t, root))
}

// fileCtx is what a call's SPELLING has to be resolved against before it means
// anything: which package the file is in, and which import each local name
// refers to. Without it "Group" is just a word — and it is a word this module
// already uses for something else, since gin spells route registration
// r.Group("/api/v1") in eight places.
type fileCtx struct {
	pkgPath string            // import path of the package this file declares
	imports map[string]string // local name -> import path
}

// buildFileCtxs derives a resolution context per file. Package names come from
// the parsed sources where the import is inside the module, and from the last
// path element otherwise — which is the same guess the compiler's default is,
// and wrong only for an external package whose name differs from its directory.
// Such an import cannot be a deriver anyway: derivers are only ever found in
// files this scan parsed.
func buildFileCtxs(files map[string]*ast.File, modulePath string) map[string]fileCtx {
	pkgNameByPath := map[string]string{}
	for rel, f := range files {
		pkgNameByPath[pkgPathOf(rel, modulePath)] = f.Name.Name
	}

	ctxs := make(map[string]fileCtx, len(files))
	for rel, f := range files {
		ctx := fileCtx{pkgPath: pkgPathOf(rel, modulePath), imports: map[string]string{}}
		for _, spec := range f.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				continue
			}
			local := ""
			switch {
			case spec.Name != nil:
				local = spec.Name.Name
			case pkgNameByPath[path] != "":
				local = pkgNameByPath[path]
			default:
				local = path[strings.LastIndex(path, "/")+1:]
			}
			if local == "_" || local == "." {
				continue
			}
			ctx.imports[local] = path
		}
		ctxs[rel] = ctx
	}
	return ctxs
}

func pkgPathOf(rel, modulePath string) string {
	dir := filepath.ToSlash(filepath.Dir(rel))
	if dir == "." {
		return modulePath
	}
	return modulePath + "/" + dir
}

// calleeName is the name a call is written with: "Group" for Group(x),
// "Group" for kafkaclient.Group(x). Everything else is "".
func calleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	}
	return ""
}

// kafkaclientPath is the one package whose Group/Groups mean namespacing.
const kafkaclientPath = "github.com/rsync-ai/shared/kafkaclient"

// isQualifier reports whether this call IS the namespacing helper — resolved,
// not merely spelled like it.
//
// The bare-name version of this check accepted gin's r.Group("/api/v1"), which
// appears eight times in this module. Every function containing one was then
// registered as a deriver, and a call to any same-named function anywhere would
// have laundered a bare literal past the whole scan.
func isQualifier(call *ast.CallExpr, ctx fileCtx) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if sel.Sel.Name != "Group" && sel.Sel.Name != "Groups" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return ctx.imports[pkg.Name] == kafkaclientPath
}

// deriverKey names the function a call refers to as "<import path>.<Func>", or
// "" when the callee is not a package-level function of a package this scan can
// see (a method on a value, a func literal, a field). Resolving through the
// file's imports is what makes the deriver set package-aware: `Start` is
// declared by both internal/projector and internal/notifier, and both call the
// helper, so keying by bare name made the name `Start` qualify code in every
// other package too.
func deriverKey(call *ast.CallExpr, ctx fileCtx) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return ctx.pkgPath + "." + fn.Name
	case *ast.SelectorExpr:
		id, ok := fn.X.(*ast.Ident)
		if !ok {
			return ""
		}
		path, ok := ctx.imports[id.Name]
		if !ok {
			// Not a package qualifier, so this is a method call on a value.
			// Methods are deliberately not derivers: nothing here resolves a
			// receiver's type, so accepting them would key on a bare name again.
			return ""
		}
		return path + "." + fn.Sel.Name
	}
	return ""
}

// containsQualifier reports whether the expression's subtree calls the
// namespacing helper directly, or calls a module function that does.
//
// The indirection is allowed deliberately: a call site is free to name its
// derivation (bridgeGroupID(topic)) instead of inlining it, and that reads
// better than a long expression wedged into a struct literal. What stays
// forbidden is the shape this test exists to catch — a group id that reaches
// the Kafka client without passing through the helper at all, which is what
// every bare string literal is.
func containsQualifier(n ast.Node, derivers map[string]bool, ctx fileCtx) bool {
	found := false
	ast.Inspect(n, func(node ast.Node) bool {
		if found {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if isQualifier(call, ctx) {
			found = true
			return false
		}
		if key := deriverKey(call, ctx); key != "" && derivers[key] {
			found = true
			return false
		}
		return true
	})
	return found
}

// deriverFuncs is the set of package-level functions in this module whose body
// calls the namespacing helper, so a call to one of them counts as qualified.
//
// Keyed by "<import path>.<Func>", never by bare name — see deriverKey. Methods
// are skipped for the same reason: a call to one cannot be resolved to its
// declaration here, so including them would put a bare name back in the map.
func deriverFuncs(files map[string]*ast.File, ctxs map[string]fileCtx) map[string]bool {
	direct := map[string]bool{}
	for rel, f := range files {
		ctx := ctxs[rel]
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Recv != nil {
				continue
			}
			if containsQualifier(fn.Body, nil, ctx) {
				direct[ctx.pkgPath+"."+fn.Name.Name] = true
			}
		}
	}
	return direct
}

// groupIDSite is one place where a group id reaches a Kafka client library.
type groupIDSite struct {
	pos   token.Position
	file  string
	how   string // the shape that matched, for the failure message
	value ast.Expr
	fn    *ast.FuncDecl // enclosing function, nil at package scope
	ctx   fileCtx       // how names in this file resolve
}

// collectGroupIDSites finds every expression this module hands to a Kafka
// client library as a consumer group id.
//
// Two shapes carry one in Go: a GroupID / ID field in a client config literal
// (kafka-go's ReaderConfig and ConsumerGroupConfig) and the id argument of
// sarama's NewConsumerGroup / NewConsumerGroupFromClient. A third shape —
// api-gateway's own NewConsumer(brokers, topics, groupID) — is deliberately NOT
// a site: it is an internal constructor that qualifies what it is given, and
// its own ReaderConfig is scanned like any other.
func collectGroupIDSites(files map[string]*ast.File, ctxs map[string]fileCtx, fset *token.FileSet) []groupIDSite {
	var sites []groupIDSite

	for rel, f := range files {
		// Scoped per top-level declaration rather than by walking the whole
		// file: it is what lets a site resolve a bare identifier against
		// assignments in the function that actually encloses it.
		for _, decl := range f.Decls {
			var fn *ast.FuncDecl
			if fd, ok := decl.(*ast.FuncDecl); ok {
				fn = fd
			}
			sites = append(sites, sitesInNode(decl, rel, ctxs[rel], fn, fset)...)
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

// sitesInNode finds the group-id expressions inside one declaration.
func sitesInNode(root ast.Node, rel string, ctx fileCtx, fn *ast.FuncDecl, fset *token.FileSet) []groupIDSite {
	var sites []groupIDSite
	ast.Inspect(root, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CompositeLit:
			name := typeNameOf(n.Type)
			// kafka-go spells the field GroupID on ReaderConfig and ID on
			// ConsumerGroupConfig; sarama's config has no id field at all (it
			// is an argument, handled below).
			field := "GroupID"
			if name == "ConsumerGroupConfig" {
				field = "ID"
			}
			for _, elt := range n.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok || key.Name != field {
					continue
				}
				sites = append(sites, groupIDSite{
					pos:   fset.Position(kv.Value.Pos()),
					file:  rel,
					how:   name + "." + field,
					value: kv.Value,
					fn:    fn,
					ctx:   ctx,
				})
			}
		case *ast.CallExpr:
			switch calleeName(n) {
			case "NewConsumerGroup":
				// sarama.NewConsumerGroup(brokers, groupID, config)
				if len(n.Args) >= 2 {
					sites = append(sites, groupIDSite{
						pos:   fset.Position(n.Args[1].Pos()),
						file:  rel,
						how:   "sarama.NewConsumerGroup arg 2",
						value: n.Args[1],
						fn:    fn,
						ctx:   ctx,
					})
				}
			case "NewConsumerGroupFromClient":
				// sarama.NewConsumerGroupFromClient(groupID, client)
				if len(n.Args) >= 1 {
					sites = append(sites, groupIDSite{
						pos:   fset.Position(n.Args[0].Pos()),
						file:  rel,
						how:   "sarama.NewConsumerGroupFromClient arg 1",
						value: n.Args[0],
						fn:    fn,
						ctx:   ctx,
					})
				}
			}
		}
		return true
	})
	return sites
}

// typeNameOf renders the composite-literal type as its bare name
// (kafka.ReaderConfig -> ReaderConfig).
func typeNameOf(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return t.Sel.Name
	}
	return ""
}

// isQualifiedGroupID decides whether a site's value expression was namespaced.
//
// A bare identifier is resolved against the enclosing function: writing
// `group := kafkaclient.Group(groupID)` once and using `group` in the literal
// is the same thing as inlining the call, and forbidding it would push call
// sites into repeating the call just to satisfy a test.
func isQualifiedGroupID(s groupIDSite, derivers map[string]bool) bool {
	if containsQualifier(s.value, derivers, s.ctx) {
		return true
	}
	ident, ok := s.value.(*ast.Ident)
	if !ok || s.fn == nil || s.fn.Body == nil {
		return false
	}
	qualified := false
	ast.Inspect(s.fn.Body, func(node ast.Node) bool {
		if qualified {
			return false
		}
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, lhs := range assign.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok || id.Name != ident.Name {
				continue
			}
			// x, y := f() — one RHS feeding several names still counts if it
			// is the qualifier.
			rhs := assign.Rhs[0]
			if i < len(assign.Rhs) {
				rhs = assign.Rhs[i]
			}
			if containsQualifier(rhs, derivers, s.ctx) {
				qualified = true
				return false
			}
		}
		return true
	})
	return qualified
}

// TestEveryConsumerGroupIDIsNamespaced is the property: no consumer group id
// reaches a Kafka client without passing through kafkaclient.Group.
//
// Adding a consumer that builds its group id as a literal fails here with the
// file:line of the literal, which is the only moment the mistake is cheap — on
// a customer's cluster it costs a consumer that never reports an error.
func TestEveryConsumerGroupIDIsNamespaced(t *testing.T) {
	fset, files, ctxs := parseModuleSources(t)
	derivers := deriverFuncs(files, ctxs)
	sites := collectGroupIDSites(files, ctxs, fset)

	// The scan must actually find the consumers this service is known to run.
	// Without this, a scanner that silently matches nothing — a renamed field,
	// a client library swap — passes while checking zero call sites.
	const wantAtLeast = 5
	if len(sites) < wantAtLeast {
		t.Fatalf("found %d consumer group id site(s), want at least %d: the scan is not matching the "+
			"code any more and would pass without checking anything. Sites: %v", len(sites), wantAtLeast, siteFiles(sites))
	}
	for _, want := range []string{
		"internal/kafka/consumer.go",            // agent responses + PII scan
		"internal/websocket/kafka_bridge.go",    // one group per bridged topic
		"internal/projector/event_projector.go", // pipeline.domain.events projection
		"internal/notifier/notifier.go",         // Slack/email alert inbox
		"internal/handlers/domain_events.go",    // HITL checkpoints to the browser
	} {
		if !hasSiteIn(sites, want) {
			t.Errorf("no consumer group id site found in %s — either that consumer was removed "+
				"(delete this expectation) or the scan stopped recognizing its shape", want)
		}
	}

	for _, s := range sites {
		if !isQualifiedGroupID(s, derivers) {
			t.Errorf("%s:%d: %s is not namespaced — it must go through kafkaclient.Group(), "+
				"otherwise a customer's PREFIXED %q ACL does not cover this group and the consumer "+
				"silently never receives a record",
				s.file, s.pos.Line, s.how, "rsync.")
		}
	}
}

func siteFiles(sites []groupIDSite) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range sites {
		if !seen[s.file] {
			seen[s.file] = true
			out = append(out, s.file)
		}
	}
	return out
}

func hasSiteIn(sites []groupIDSite, file string) bool {
	for _, s := range sites {
		if filepath.ToSlash(s.file) == file {
			return true
		}
	}
	return false
}

// The guard has to be able to fail. A scanner that accepts a bare literal is
// worth nothing, and nothing else in this file demonstrates that it does not —
// every real call site is qualified, so the loop above never takes its failing
// branch.
func TestScannerRejectsABareStringLiteralGroupID(t *testing.T) {
	fset := token.NewFileSet()
	const src = `package p

import (
	"github.com/rsync-ai/shared/kafkaclient"
	"github.com/segmentio/kafka-go"
	"github.com/IBM/sarama"
)

func stranded() {
	_ = kafka.NewReader(kafka.ReaderConfig{GroupID: "websocket-bridge-" + topic})
	_, _ = sarama.NewConsumerGroup(brokers, "api-gateway-notifier", nil)
}

func qualifiedInline() {
	_ = kafka.NewReader(kafka.ReaderConfig{GroupID: kafkaclient.Group("api-gateway-projector")})
}

func qualifiedViaLocal() {
	group := kafkaclient.Group("api-gateway-consumer-group")
	_, _ = sarama.NewConsumerGroup(brokers, group, nil)
}

func helper(topic string) string { return kafkaclient.Group("websocket-bridge-" + topic) }

func qualifiedViaHelper() {
	_ = kafka.NewReader(kafka.ReaderConfig{GroupID: helper(topic)})
}
`
	f, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	files := map[string]*ast.File{"fixture.go": f}
	ctxs := buildFileCtxs(files, "example.test")
	derivers := deriverFuncs(files, ctxs)
	sites := collectGroupIDSites(files, ctxs, fset)

	if len(sites) != 5 {
		t.Fatalf("found %d sites in the fixture, want 5", len(sites))
	}

	var rejected, accepted int
	for _, s := range sites {
		if isQualifiedGroupID(s, derivers) {
			accepted++
		} else {
			rejected++
		}
	}
	if rejected != 2 {
		t.Errorf("rejected %d sites, want the 2 bare ones: the scanner cannot detect a stranded call site", rejected)
	}
	if accepted != 3 {
		t.Errorf("accepted %d sites, want the 3 qualified ones (inline, via local, via helper): the scanner "+
			"is too strict and would push call sites into contortions", accepted)
	}
}

// A deriver is resolved by NAME, and names are not unique across a module. Two
// collisions are already present in this codebase, and both used to make the
// scan above accept a bare literal:
//
//   - gin spells route registration r.Group("/api/v1"), in eight places. Matching
//     the qualifier by the bare name "Group" made every one of those functions a
//     deriver, and made the expression r.Group(x) itself count as namespacing.
//   - internal/projector and internal/notifier both declare Start, and both call
//     kafkaclient.Group. Keying by bare name registered "Start" for the whole
//     module, so a call to any other Start counted as qualified.
//
// Neither is hypothetical and neither would have failed anything: the scan would
// have gone on reporting that every group id is namespaced.
func TestScannerDoesNotAcceptLookalikeQualifiers(t *testing.T) {
	fset := token.NewFileSet()
	const derivingPkg = `package projector

import "github.com/rsync-ai/shared/kafkaclient"

// The real deriver. Its NAME is the collision.
func Start() string { return kafkaclient.Group("api-gateway-projector") }
`
	const collidingPkg = `package other

import (
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

// Same name, different package, does NOT namespace anything.
func Start() string { return "api-gateway-other" }

func strandedViaNameCollision() {
	_ = kafka.NewReader(kafka.ReaderConfig{GroupID: Start()})
}

func strandedViaGinRouteGroup(r *gin.Engine) {
	_ = kafka.NewReader(kafka.ReaderConfig{GroupID: r.Group("/api/v1").BasePath()})
}
`
	files := map[string]*ast.File{}
	for rel, src := range map[string]string{
		"internal/projector/p.go": derivingPkg,
		"internal/other/o.go":     collidingPkg,
	} {
		f, err := parser.ParseFile(fset, rel, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parsing %s: %v", rel, err)
		}
		files[rel] = f
	}

	ctxs := buildFileCtxs(files, "example.test")
	derivers := deriverFuncs(files, ctxs)
	sites := collectGroupIDSites(files, ctxs, fset)

	if len(sites) != 2 {
		t.Fatalf("found %d sites in the fixture, want 2 — the fixture no longer exercises anything", len(sites))
	}
	// The real deriver must still be recognised, or this test would pass by
	// breaking the feature it is guarding rather than by resolving names.
	if !derivers["example.test/internal/projector.Start"] {
		t.Fatalf("projector.Start was not registered as a deriver; derivers=%v", derivers)
	}

	for _, s := range sites {
		if isQualifiedGroupID(s, derivers) {
			t.Errorf("%s:%d: %s was accepted as namespaced, but nothing in it calls "+
				"kafkaclient.Group — a lookalike name is laundering a bare group id past the scan",
				s.file, s.pos.Line, s.how)
		}
	}
}

// The migration lever has to reach group ids too. A deployment with live groups
// and committed offsets under the bare ids sets KAFKA_TOPIC_PREFIX="" to take
// this code without every consumer restarting from auto.offset.reset.
func TestEmptyPrefixLeavesEveryAPIGatewayGroupIDUntouched(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "")

	for _, logical := range []string{
		"api-gateway-consumer-group",
		"api-gateway-projector",
		"api-gateway-notifier",
		"api-gateway-domain-events",
		"websocket-bridge-pipeline.domain.events",
	} {
		if got := kafkaclient.Group(logical); got != logical {
			t.Errorf("with the prefix disabled, group %q became %q; a deployment taking this code would "+
				"lose its committed offsets", logical, got)
		}
	}
}
