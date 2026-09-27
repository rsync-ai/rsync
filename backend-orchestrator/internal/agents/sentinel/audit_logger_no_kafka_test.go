package sentinel

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

// The sentinel audit trail is the sentinel_audit_logs and sentinel_healing_results
// tables plus the structured log. AuditLogger used to copy every entry to the
// rsync.sentinel.audit topic as well, which nothing consumed: the topic existed only
// because this producer auto-created it. That copy is gone, and these tests keep it
// gone: AuditLogger holds no Kafka handle, and logger.go makes no Kafka produce call.

func TestAuditLoggerHoldsNoKafkaHandle(t *testing.T) {
	typ := reflect.TypeOf(AuditLogger{})
	if typ.NumField() == 0 {
		t.Fatal("AuditLogger has no fields; the check below would prove nothing")
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		ft := f.Type
		for ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if strings.HasSuffix(ft.PkgPath(), "/internal/kafka") {
			t.Errorf("AuditLogger.%s is a %s: the audit logger must not publish to Kafka; "+
				"the audit trail is the sentinel_audit_logs table", f.Name, f.Type)
		}
	}
}

func TestAuditLoggerSourceMakesNoProduceCall(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "logger.go", nil, 0)
	if err != nil {
		t.Fatalf("parse logger.go: %v", err)
	}
	calls := 0
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		calls++
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && strings.HasPrefix(sel.Sel.Name, "Produce") {
			t.Errorf("logger.go calls %s at %s; the sentinel audit trail has no Kafka copy",
				sel.Sel.Name, fset.Position(call.Pos()))
		}
		return true
	})
	if calls == 0 {
		t.Fatal("found no calls at all in logger.go; the parse is broken")
	}
}
