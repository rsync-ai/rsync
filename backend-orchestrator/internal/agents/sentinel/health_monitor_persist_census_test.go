package sentinel

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"testing"
)

// componentHealthWriteSites returns, for every function in health_monitor.go that assigns
// into the componentHealth map, whether that same function also reaches persistHealthToDB.
func componentHealthWriteSites(t *testing.T) map[string]bool {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "health_monitor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse health_monitor.go: %v", err)
	}

	sites := map[string]bool{}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		var writes, persists bool
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.AssignStmt:
				// Only an assignment INTO the map counts. `delete(h.componentHealth, id)`
				// is deliberately not a write site: an eviction has no verdict to publish,
				// and its own database obligation is covered by the eviction tests.
				for _, lhs := range node.Lhs {
					idx, ok := lhs.(*ast.IndexExpr)
					if !ok {
						continue
					}
					if sel, ok := idx.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "componentHealth" {
						writes = true
					}
				}
			case *ast.CallExpr:
				// Reached through GoStmt too: a persist run in a goroutine is still a
				// persist (TestPersistCensusDistinguishesPersistingFromSilent proves it).
				if sel, ok := node.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "persistHealthToDB" {
					persists = true
				}
			}
			return true
		})

		if writes {
			sites[fn.Name.Name] = persists
		}
	}

	return sites
}

// TestEveryComponentHealthWriteSitePersists is a census, not a point test.
//
// The same defect has now shipped in this file twice: a health verdict computed correctly,
// written into h.componentHealth, and never persisted. First for the three infrastructure
// checks (#731 T9, fixed by introducing recordInfraHealth), then — left behind by that same
// fix — for the three callers of RecordHealthChange (since deleted) and for
// checkMCPConnectorHealth. The
// map is read by no code outside health_monitor.go, so an unpersisted verdict reaches
// nobody: the row in sentinel_component_health is the only published form, and it is what
// GET /api/v1/monitoring/sentinel/health returns.
//
// Point tests would each guard one site and say nothing about the next one somebody adds.
// This walks the file instead and asserts the rule itself, so a new write site in a new
// function fails here until it either persists or is argued about deliberately.
func TestEveryComponentHealthWriteSitePersists(t *testing.T) {
	sites := componentHealthWriteSites(t)

	// Positive denominator. A matcher that silently matched nothing would make every
	// assertion below vacuously true — the census would report a clean bill of health for
	// a file it had failed to read. These two (recordInfraHealth, recordConnectorVerdict)
	// are the write sites that exist today; the assertion is >=, so adding a third is not
	// a failure, only an unpersisted one is.
	if len(sites) < 2 {
		t.Fatalf("census found only %d componentHealth write sites (%v); expected at least 2 — the matcher is broken, not the code", len(sites), sites)
	}

	for _, name := range sortedKeys(sites) {
		if !sites[name] {
			t.Errorf("%s writes a health verdict into componentHealth but never reaches persistHealthToDB — "+
				"no code outside this file reads that map, so the verdict reaches nobody", name)
		}
	}
}

// The census must be able to fail. If the detector could not tell a persisting function
// from a silent one, the test above would pass no matter what the file said — which is
// precisely the failure mode that let this bug ship twice.
func TestPersistCensusDistinguishesPersistingFromSilent(t *testing.T) {
	// The positive half, on the real file: recordInfraHealth is the write site every
	// infrastructure check and service probe goes through.
	sites := componentHealthWriteSites(t)
	persists, found := sites["recordInfraHealth"]
	if !found {
		t.Fatal("recordInfraHealth not found as a write site — the census is reading the wrong file")
	}
	if !persists {
		t.Error("detector failed to see recordInfraHealth's persist")
	}

	// The synthetic halves: a function that only writes must be reported silent, and one
	// that persists inside a `go` statement must not be — no write site in the real file
	// does that today, so the walk's descent into GoStmt is proved here.
	for _, tc := range []struct {
		name         string
		body         string
		wantPersists bool
	}{
		{"silent writer", `h.componentHealth[id] = health`, false},
		{"persist in a goroutine", "h.componentHealth[id] = health\n\tgo h.persistHealthToDB(health)", true},
	} {
		src := "package sentinel\nfunc (h *HealthMonitor) f(id string, health *ComponentHealth) {\n\t" + tc.body + "\n}"
		f2, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", src, 0)
		if err != nil {
			t.Fatalf("%s: parse synthetic: %v", tc.name, err)
		}
		fn := f2.Decls[0].(*ast.FuncDecl)
		var writes, persists bool
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.AssignStmt:
				for _, lhs := range node.Lhs {
					if idx, ok := lhs.(*ast.IndexExpr); ok {
						if sel, ok := idx.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "componentHealth" {
							writes = true
						}
					}
				}
			case *ast.CallExpr:
				if sel, ok := node.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "persistHealthToDB" {
					persists = true
				}
			}
			return true
		})
		if !writes {
			t.Errorf("%s: detector did not recognise a plain componentHealth write", tc.name)
		}
		if persists != tc.wantPersists {
			t.Errorf("%s: detector reported persists=%v, want %v", tc.name, persists, tc.wantPersists)
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
