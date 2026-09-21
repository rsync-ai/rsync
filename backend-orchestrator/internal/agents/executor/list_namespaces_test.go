package executor

import (
	"encoding/json"
	"reflect"
	"testing"
)

// The connector's list_namespaces result reaches the orchestrator as decoded
// JSON, so the names arrive as []interface{}. Blank and non-string entries are
// dropped rather than shown as empty choices in the scope picker.
func TestNamespacesFromResult(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantNames   []string
		wantCurrent string
	}{
		{"schemas and current", `{"success":true,"namespaces":["public","shop"],"current":"shop"}`, []string{"public", "shop"}, "shop"},
		{"blank and non-string dropped", `{"namespaces":["a"," ",3,null,"b"],"current":" a "}`, []string{"a", "b"}, "a"},
		{"no list is an empty list", `{"success":true}`, []string{}, ""},
		{"current not a string", `{"namespaces":[],"current":7}`, []string{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var result map[string]interface{}
			if err := json.Unmarshal([]byte(tc.body), &result); err != nil {
				t.Fatal(err)
			}
			names, current := namespacesFromResult(result)
			if !reflect.DeepEqual(names, tc.wantNames) || current != tc.wantCurrent {
				t.Fatalf("got %q %q, want %q %q", names, current, tc.wantNames, tc.wantCurrent)
			}
		})
	}
}
