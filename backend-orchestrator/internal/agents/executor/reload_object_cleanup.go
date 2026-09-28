package executor

import (
	"fmt"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/internal/storage"
)

// reloadObjectPrefixKeys are the destination-config keys the sink reads for the
// object-storage base prefix (kafka-sink-worker main.go, the v1 reload cleanup's
// firstStr list). The orchestrator must read the same keys, or the two cleanups
// point at different folders.
var reloadObjectPrefixKeys = []string{"path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path"}

// reloadObjectTablePrefix is the v1 table folder a reload empties before the first
// batch. It is built exactly like the sink's objectLayoutV1TablePrefix/tablePrefix
// (kafka-sink-worker): an unset base prefix adds no segment (no leading "/"), empty
// segments are skipped, and the table is the BARE name — the sink writes
// "public.users" under ".../users/", so a schema-qualified name here matched nothing.
func reloadObjectTablePrefix(destConfig map[string]string, dataset, dbOrSchema, tableName string) string {
	base := ""
	for _, k := range reloadObjectPrefixKeys {
		if v := strings.TrimSpace(destConfig[k]); v != "" {
			base = v
			break
		}
	}
	table := strings.TrimSpace(tableName)
	if idx := strings.LastIndex(table, "."); idx >= 0 && idx+1 < len(table) {
		table = table[idx+1:]
	}
	parts := []string{}
	if p := strings.Trim(base, "/"); p != "" {
		parts = append(parts, p)
	}
	for _, seg := range []string{dataset, dbOrSchema, storage.SanitizePath(table)} {
		if seg != "" {
			parts = append(parts, seg)
		}
	}
	return strings.Join(parts, "/") + "/"
}

// reloadObjectDeleteParams names the bucket the way each connector expects it:
// azure-blob takes "container", the others "bucket".
func reloadObjectDeleteParams(destType string, destConfig map[string]string, prefix string) map[string]interface{} {
	bucket := strings.TrimSpace(destConfig["bucket"])
	if bucket == "" {
		bucket = strings.TrimSpace(destConfig["bucket_name"])
	}
	params := map[string]interface{}{"prefix": prefix}
	if strings.Contains(strings.ToLower(destType), "azure") {
		container := strings.TrimSpace(destConfig["container"])
		if container == "" {
			container = bucket
		}
		if container != "" {
			params["container"] = container
		}
	} else if bucket != "" {
		params["bucket"] = bucket
	}
	return params
}

// classifyReloadDeleteResult decides whether a reload's delete_prefix proved the
// table folder empty. The connectors return {success, deleted, failed, complete}
// (shared/delete_prefix_contract_golden.json); anything short of a complete,
// failure-free delete is fatal, because the reload would otherwise rewrite the table
// on top of the old files and readers would see both generations.
func classifyReloadDeleteResult(resp *mcp.ExecuteResponse, err error) (bool, string) {
	if err != nil {
		return true, err.Error()
	}
	if resp == nil {
		return true, "no response from destination"
	}
	if !resp.Success {
		if resp.Error != "" {
			return true, resp.Error
		}
		return true, "destination reported success=false"
	}
	r := resp.Result
	if v, ok := r["success"].(bool); ok && !v {
		return true, fmt.Sprintf("destination reported success=false (%v)", r["error"])
	}
	if v, ok := r["complete"].(bool); ok && !v {
		return true, fmt.Sprintf("delete stopped before the listing ended (deleted=%v failed=%v)", r["deleted"], r["failed"])
	}
	if n, ok := r["failed"].(float64); ok && n > 0 {
		return true, fmt.Sprintf("%v objects could not be deleted (deleted=%v)", n, r["deleted"])
	}
	return false, ""
}
