package main

// The orchestrator must resolve the connector catalog through the one shared
// resolver, `connectorpaths.ToolsDir()`.
//
// It did not, and the consequence was total: on a clean self-host install the
// orchestrator logged
//
//	MCP Server Manager initialized (tools dir: /shared/mcp-connectors)
//
// and answered every connector lookup with
//
//	failed to locate connector "sample-data" in tools dir /shared/mcp-connectors
//
// while the catalog sat, correctly mounted, at /app/shared/mcp-connectors.
//
// The mechanism is two correct-looking pieces meeting: the viper default for
// TOOLS_DIR is `../../shared/mcp-connectors`, which is right for a developer
// running from backend-orchestrator/cmd/orchestrator, and `NewServerManager`
// takes `filepath.Abs` of whatever it is handed. Under the image's `WORKDIR
// /app` that pair cleans to `/shared/mcp-connectors` — two levels up from /app
// is /, so the relative path escapes the filesystem root's meaningful part and
// lands somewhere no image creates. Nothing errors: the directory walk finds
// nothing and reports an empty catalog, which is the "a count of zero is not an
// error" shape this repo has been bitten by before.
//
// `connectorpaths.ToolsDir()` exists precisely to end this class — its own doc
// comment says "One copy on purpose" — and the sentinel health monitor and the
// connector registry already call it. main.go was the last holdout.
//
// Two tests, deliberately different in kind:
//
//   - the structural one asserts main.go asks the resolver, and is what fails if
//     someone reintroduces a private answer;
//   - the behavioural one executes the mechanism against a real directory tree
//     and is what proves the resolver is doing work rather than agreeing with
//     the broken default by luck. It reads the default literal out of config.go
//     rather than hard-coding it, so it cannot drift from the source it is
//     making a claim about.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
)

// Where the images mount the catalog. Every compose file in the repo and the
// Helm chart agree on this path; `WORKDIR /app` in backend-orchestrator's
// Dockerfile is what makes the relative default miss it.
const catalogMountPath = "/app/shared/mcp-connectors"

// configGoPath is relative to this package's directory.
const configGoPath = "../../internal/config/config.go"

// toolsDirDefaultFromConfig returns the literal `config.go` passes to
// v.SetDefault("TOOLS_DIR", …). Read from source so the behavioural test below
// keeps describing the value that actually ships.
func toolsDirDefaultFromConfig(t *testing.T) string {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, configGoPath, nil, 0)
	if err != nil {
		t.Fatalf("could not parse %s: %v", configGoPath, err)
	}

	var found string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "SetDefault" {
			return true
		}
		key, ok := call.Args[0].(*ast.BasicLit)
		if !ok || key.Kind != token.STRING {
			return true
		}
		if k, err := strconv.Unquote(key.Value); err != nil || k != "TOOLS_DIR" {
			return true
		}
		val, ok := call.Args[1].(*ast.BasicLit)
		if !ok || val.Kind != token.STRING {
			return true
		}
		if v, err := strconv.Unquote(val.Value); err == nil {
			found = v
		}
		return false
	})

	if found == "" {
		t.Fatalf("no v.SetDefault(\"TOOLS_DIR\", <string literal>) found in %s — "+
			"if the default moved, move this test's subject with it", configGoPath)
	}
	return found
}

// TestToolsDirIsResolvedByTheSharedResolver pins main.go to the shared resolver.
//
// It asserts on the assignment, not on the presence of the import: an import can
// be there for an unrelated call, and the defect was specifically that this one
// assignment carried its own answer.
func TestToolsDirIsResolvedByTheSharedResolver(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("could not parse main.go: %v", err)
	}

	assignments := 0
	viaResolver := false

	ast.Inspect(file, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		ident, ok := assign.Lhs[0].(*ast.Ident)
		if !ok || ident.Name != "toolsDir" {
			return true
		}
		assignments++

		call, ok := assign.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if ok && pkg.Name == "connectorpaths" && sel.Sel.Name == "ToolsDir" {
			viaResolver = true
		}
		return true
	})

	// Denominator first. If main.go stops assigning `toolsDir` at all, the
	// assertion below would pass on an empty set and this guard would go quiet
	// without anyone noticing.
	if assignments == 0 {
		t.Fatal("main.go no longer assigns `toolsDir` — this guard has lost its subject " +
			"and must be re-aimed at whatever now feeds mcp.NewServerManager")
	}

	if !viaResolver {
		t.Errorf("main.go assigns `toolsDir` %d time(s) but never from connectorpaths.ToolsDir(); "+
			"a private answer here resolves to %q inside the image and finds no connectors",
			assignments, "/shared/mcp-connectors")
	}
}

// TestTheConfigDefaultMissesTheCatalogTheImagesMount executes the mechanism.
//
// The temp tree stands in for the container: a working directory called `app`
// with the catalog mounted inside it. The assertions are a matched pair — the
// default must miss, the resolver must hit — because a test that only asserted
// the resolver hits would also pass on a build where the default happened to
// work, which is exactly the environment (a developer checkout) that hid this
// bug for the whole life of the code.
func TestTheConfigDefaultMissesTheCatalogTheImagesMount(t *testing.T) {
	// Read the default before chdir — configGoPath is relative to this package.
	def := toolsDirDefaultFromConfig(t)

	// The resolver reads these first; empty means "unset" to it.
	t.Setenv("MCP_CONNECTORS_PATH", "")
	t.Setenv("TOOLS_DIR", "")

	root := t.TempDir()
	workdir := filepath.Join(root, "app")
	catalog := filepath.Join(workdir, "shared", "mcp-connectors")
	if err := os.MkdirAll(filepath.Join(catalog, "public"), 0o755); err != nil {
		t.Fatalf("could not build the fixture tree: %v", err)
	}
	t.Chdir(workdir)

	// Control: the fixture really does mirror the mount the images use, so a
	// miss below is the path arithmetic and not a missing directory.
	if _, err := os.Stat(catalog); err != nil {
		t.Fatalf("fixture is broken — %s does not exist: %v", catalog, err)
	}
	if got := filepath.Join(string(filepath.Separator), "app", "shared", "mcp-connectors"); got != catalogMountPath {
		t.Fatalf("fixture does not mirror the real mount %s", catalogMountPath)
	}

	// The broken half: what NewServerManager would have made of the default.
	abs, err := filepath.Abs(def)
	if err != nil {
		t.Fatalf("filepath.Abs(%q): %v", def, err)
	}
	if abs == catalog {
		t.Fatalf("the config default %q resolved to the catalog (%s) from a /app-shaped "+
			"working directory. If the default was deliberately changed to an absolute "+
			"path this test is stale — but check first that it is not simply agreeing "+
			"with the resolver by accident, which is what this case exists to rule out.",
			def, catalog)
	}
	if info, err := os.Stat(abs); err == nil && info.IsDir() {
		t.Errorf("the config default %q resolved to %q, which exists but is not the "+
			"mounted catalog %q — a connector walk there reports an empty catalog and no error",
			def, abs, catalog)
	}

	// The working half.
	got := connectorpaths.ToolsDir()
	if got == "" {
		t.Fatal("connectorpaths.ToolsDir() found nothing with the catalog mounted at " +
			"./shared/mcp-connectors — its candidate list no longer covers the layout the images ship")
	}
	gotAbs, err := filepath.Abs(got)
	if err != nil {
		t.Fatalf("filepath.Abs(%q): %v", got, err)
	}
	if gotAbs != catalog {
		t.Errorf("connectorpaths.ToolsDir() = %q (abs %q), want the mounted catalog %q",
			got, gotAbs, catalog)
	}
}
