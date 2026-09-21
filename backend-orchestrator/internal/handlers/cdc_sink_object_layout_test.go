package handlers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestRestartCDCSinkWorkerSendsObjectLayout: a sink restart must start the worker
// with the pipeline's recorded object-storage layout. Without the three fields the
// sink writes layout v1 keys, so a v2 pipeline would grow v1 folders after its
// first restart. The decision must also be able to stop the restart (a v2 pipeline
// that is no longer eligible), so it runs before the stop_sink call.
func TestRestartCDCSinkWorkerSendsObjectLayout(t *testing.T) {
	fset := token.NewFileSet()
	src, err := os.ReadFile("cdc_sink.go")
	if err != nil {
		t.Fatalf("read cdc_sink.go: %v", err)
	}
	f, err := parser.ParseFile(fset, "cdc_sink.go", src, 0)
	if err != nil {
		t.Fatalf("parse cdc_sink.go: %v", err)
	}
	var body string
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "restartCDCSinkWorker" {
			body = string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
		}
	}
	if body == "" {
		t.Fatal("restartCDCSinkWorker not found in cdc_sink.go; if it moved or was renamed, update this test")
	}
	resolve := strings.Index(body, "executor.ResolveObjectLayoutForRestart(")
	stop := strings.Index(body, `"stop_sink"`)
	start := strings.Index(body, `"start_sink"`)
	if resolve < 0 || stop < 0 || start < 0 {
		t.Fatalf("restartCDCSinkWorker must call executor.ResolveObjectLayoutForRestart and the stop_sink/start_sink tools")
	}
	if resolve > stop {
		t.Errorf("the layout must be resolved before stop_sink, so a refused restart leaves the running worker alone")
	}
	// gofmt re-aligns a map literal whenever a key is added, so compare with runs of
	// blanks collapsed to one space.
	startCfg := strings.Join(strings.Fields(body[start:]), " ")
	for _, k := range []string{`"storage_layout_version": layoutFields["storage_layout_version"]`,
		`"source_family": layoutFields["source_family"]`,
		`"source_database": layoutFields["source_database"]`,
		// A server-level source mirrors its databases; a restart must keep doing so.
		`"mirror_source_namespace": mirrorSource`} {
		if !strings.Contains(startCfg, k) {
			t.Errorf("start_sink config must carry %s", k)
		}
	}
	if !strings.Contains(body, "executor.MirrorSourceNamespaces(") {
		t.Errorf("restartCDCSinkWorker must decide mirror_source_namespace with executor.MirrorSourceNamespaces")
	}
	// The picker's stored flatten/preserve choice decides first, as on the first start.
	if !strings.Contains(body, "executor.SchemaModeOverride(ctx, db, pipelineID)") {
		t.Errorf("restartCDCSinkWorker must pass the pipeline's executor.SchemaModeOverride to MirrorSourceNamespaces")
	}
}
