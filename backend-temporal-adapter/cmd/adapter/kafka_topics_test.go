package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The adapter is produce-only on Kafka, and only to platform topics. A consumer here
// once subscribed to agent.control.results and signalled workflows that never listened
// for those signals; two activities produced to pipeline.failed.dlq and agent.failed.dlq
// and nothing ever scheduled them. Each of those names still got created on the broker
// (a subscribe or a produce auto-creates), so a default install carried topics no
// component used. This pins the set by reading the source, so a new topic, or a new
// consumer, has to be added here on purpose.

// adapterPlatformTopics is every topic this module may name, in its logical
// (unprefixed) spelling.
var adapterPlatformTopics = []string{
	"notifications",          // emitTerminalRunNotification
	"pipeline.domain.events", // emitDomainEventActivity
}

const kafkaclientImportPath = "github.com/rsync-ai/shared/kafkaclient"
const saramaImportPath = "github.com/IBM/sarama"

func TestAdapterProducesOnlyToPlatformTopics(t *testing.T) {
	files := readModuleGoSources(t, filepath.Join("..", ".."))
	// A scan that read nothing would pass every assertion below.
	if len(files) < 10 {
		t.Fatalf("read %d non-test Go files from the module; the walk is not finding the source", len(files))
	}

	topics, problems := kafkaTopicUsage(files)
	for _, p := range problems {
		t.Error(p)
	}
	if got, want := strings.Join(topics, ","), strings.Join(sortedCopy(adapterPlatformTopics), ","); got != want {
		t.Errorf("the adapter names Kafka topics %v, want exactly %v — a topic outside the "+
			"platform set is one a default install creates for nothing", topics, adapterPlatformTopics)
	}
}

// The scanner above is only worth something if it can see the shapes it exists to
// catch. Each case here is a shape that was, or would be, a regression.
func TestKafkaTopicUsageFlagsWhatItGuards(t *testing.T) {
	cases := []struct {
		name        string
		src         string
		wantTopics  []string
		wantProblem string
	}{
		{
			name: "the removed consume loop",
			src: `package x
import (
	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)
func f() {
	g, _ := sarama.NewConsumerGroup(nil, "grp", nil)
	_ = g
	_ = kafkaclient.Topics("agent.control.results")
}`,
			wantTopics:  []string{"agent.control.results"},
			wantProblem: "sarama.NewConsumerGroup",
		},
		{
			name: "a topic held in a same-package const",
			src: `package x
import "github.com/rsync-ai/shared/kafkaclient"
const dlq = "rsync.pipeline.failed.dlq"
func f() string { return kafkaclient.Topic(dlq) }`,
			wantTopics: []string{"pipeline.failed.dlq"},
		},
		{
			name: "a topic the scan cannot resolve",
			src: `package x
import "github.com/rsync-ai/shared/kafkaclient"
func f(name string) string { return kafkaclient.Topic(name) }`,
			wantProblem: "cannot resolve",
		},
		{
			name: "a ProducerMessage that bypasses kafkaclient.Topic",
			src: `package x
import "github.com/IBM/sarama"
func f() *sarama.ProducerMessage { return &sarama.ProducerMessage{Topic: "agent.failed.dlq"} }`,
			wantProblem: "not qualified by kafkaclient.Topic",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topics, problems := kafkaTopicUsage(map[string]string{"x/x.go": tc.src})
			if got, want := strings.Join(topics, ","), strings.Join(tc.wantTopics, ","); got != want {
				t.Errorf("topics = %v, want %v", topics, tc.wantTopics)
			}
			joined := strings.Join(problems, "\n")
			if tc.wantProblem == "" && joined != "" {
				t.Errorf("unexpected problems:\n%s", joined)
			}
			if tc.wantProblem != "" && !strings.Contains(joined, tc.wantProblem) {
				t.Errorf("problems %q do not mention %q", joined, tc.wantProblem)
			}
		})
	}
}

// readModuleGoSources returns every non-test .go file under root, keyed by path.
func readModuleGoSources(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (name == "vendor" || name == "testdata" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = string(b)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// kafkaTopicUsage reports the logical topic names passed to kafkaclient.Topic/Topics,
// sorted and de-duplicated with the default "rsync." prefix stripped, plus every
// shape the result cannot vouch for: an argument it cannot resolve to a string, a
// sarama.ProducerMessage whose Topic does not go through kafkaclient.Topic, and any
// sarama consumer constructor (subscribing auto-creates the topic, and this module
// has no consumer).
func kafkaTopicUsage(files map[string]string) (topics []string, problems []string) {
	fset := token.NewFileSet()
	type parsed struct {
		path string
		file *ast.File
	}
	byPkg := map[string][]parsed{}
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		f, err := parser.ParseFile(fset, p, files[p], 0)
		if err != nil {
			problems = append(problems, "parse "+p+": "+err.Error())
			continue
		}
		dir := filepath.Dir(p)
		byPkg[dir] = append(byPkg[dir], parsed{p, f})
	}

	seen := map[string]bool{}
	for _, pfs := range byPkg {
		consts := map[string]string{}
		for _, pf := range pfs {
			for _, decl := range pf.file.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				for _, spec := range gd.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, name := range vs.Names {
						if i < len(vs.Values) {
							if s, ok := stringLit(vs.Values[i]); ok {
								consts[name.Name] = s
							}
						}
					}
				}
			}
		}
		resolve := func(e ast.Expr) (string, bool) {
			if s, ok := stringLit(e); ok {
				return s, true
			}
			if id, ok := e.(*ast.Ident); ok {
				s, ok := consts[id.Name]
				return s, ok
			}
			return "", false
		}

		for _, pf := range pfs {
			kc := importName(pf.file, kafkaclientImportPath, "kafkaclient")
			sr := importName(pf.file, saramaImportPath, "sarama")
			isTopicCall := func(e ast.Expr) bool {
				call, ok := e.(*ast.CallExpr)
				return ok && selectorIs(call.Fun, kc, "Topic")
			}
			ast.Inspect(pf.file, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CallExpr:
					sel, ok := n.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					pkg, ok := sel.X.(*ast.Ident)
					if !ok {
						return true
					}
					switch {
					case kc != "" && pkg.Name == kc && (sel.Sel.Name == "Topic" || sel.Sel.Name == "Topics"):
						for _, arg := range n.Args {
							s, ok := resolve(arg)
							if !ok {
								problems = append(problems, fset.Position(arg.Pos()).String()+
									": cannot resolve the argument to "+kc+"."+sel.Sel.Name+" to a string constant")
								continue
							}
							seen[strings.TrimPrefix(s, "rsync.")] = true
						}
					case sr != "" && pkg.Name == sr && strings.HasPrefix(sel.Sel.Name, "NewConsumer"):
						problems = append(problems, fset.Position(n.Pos()).String()+": sarama."+sel.Sel.Name+
							" — this module has no Kafka consumer, and subscribing auto-creates the topic")
					}
				case *ast.CompositeLit:
					if !selectorIs(n.Type, sr, "ProducerMessage") {
						return true
					}
					for _, elt := range n.Elts {
						kv, ok := elt.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Topic" && !isTopicCall(kv.Value) {
							problems = append(problems, fset.Position(kv.Value.Pos()).String()+
								": sarama.ProducerMessage Topic is not qualified by kafkaclient.Topic")
						}
					}
				}
				return true
			})
		}
	}
	for s := range seen {
		topics = append(topics, s)
	}
	sort.Strings(topics)
	sort.Strings(problems)
	return topics, problems
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	return s, err == nil
}

// importName is the local name a file binds importPath to, or "" if it does not
// import it.
func importName(f *ast.File, importPath, defaultName string) string {
	for _, imp := range f.Imports {
		if p, _ := strconv.Unquote(imp.Path.Value); p == importPath {
			if imp.Name != nil {
				return imp.Name.Name
			}
			return defaultName
		}
	}
	return ""
}

func selectorIs(e ast.Expr, pkg, name string) bool {
	if pkg == "" {
		return false
	}
	sel, ok := e.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
