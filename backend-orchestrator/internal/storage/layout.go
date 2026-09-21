package storage

// Object-storage key layout contract (L6).
//
// Pure key builders, not yet called by any writer or reader. The LayoutV1* functions are
// a port of what the kafka-sink-worker writes today
// (shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go:
// slugify, sanitizePathPart, timePartitionSegment, cdcObjectPath, cdcObjectKey,
// cdcPipelineSegment, cdcPipelineRootPrefix, fileExt, tablePrefix, partKey, manifestKey,
// successKey, plus the batch writer's inline normalization). They deliberately do NOT
// reuse KeyBuilder / Slugify / SanitizePath in keybuilder.go, whose rules differ from
// the worker's. The LayoutV2* functions are the agreed next layout.
//
// Both are pinned by shared/object_layout_golden.json, which the worker's
// object_layout.go and rsync_protocol/object_layout.py (v2 only) also read.

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// LayoutV1Input carries every value a v1 key depends on. DestConfig is the destination
// connection config (prefix keys, file format, partition_time_granularity). Compression
// is the already-resolved codec. NowMs stands in for the clock when Dt or TsMs is unset.
type LayoutV1Input struct {
	DestConfig       map[string]interface{} `json:"dest_config"`
	PipelineID       string                 `json:"pipeline_id"`
	WorkerPipelineID string                 `json:"worker_pipeline_id"`
	Dataset          string                 `json:"dataset"`
	DBOrSchema       string                 `json:"db_or_schema"`
	Table            string                 `json:"table"`
	PartSegs         string                 `json:"part_segs"`
	Dt               string                 `json:"dt"`
	Offset           int64                  `json:"offset"`
	PartSuffix       string                 `json:"part_suffix"`
	Compression      string                 `json:"compression"`
	TsMs             int64                  `json:"ts_ms"`
	Partition        int                    `json:"partition"`
	FirstOffset      int64                  `json:"first_offset"`
	NowMs            int64                  `json:"now_ms"`
}

func layoutV1FirstStr(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func layoutV1Slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	return out
}

func layoutV1Sanitize(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	return strings.ReplaceAll(s, "..", "_")
}

func layoutV1Prefix(destCfg map[string]interface{}) string {
	return layoutV1FirstStr(destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path")
}

func layoutV1TablePrefix(prefix, dataset, db, table string) string {
	parts := []string{}
	for _, p := range []string{strings.Trim(prefix, "/"), dataset, db, table} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, "/") + "/"
}

func layoutV1FileExt(format, compression string) string {
	ext := strings.ToLower(strings.TrimSpace(format))
	if ext == "" {
		ext = "json"
	}
	switch ext {
	case "ndjson", "json_lines", "jsonlines":
		ext = "jsonl"
	}
	comp := strings.ToLower(strings.TrimSpace(compression))
	if comp == "" || comp == "none" || ext == "parquet" {
		return ext
	}
	switch comp {
	case "gzip", "gz":
		return ext + ".gz"
	case "zstd", "zst":
		return ext + ".zst"
	default:
		return ext + "." + comp
	}
}

func layoutV1BatchPath(in LayoutV1Input) (dataset, db, table, dt string) {
	dataset = layoutV1Slugify(in.Dataset)
	if dataset == "" {
		dataset = layoutV1Slugify(in.PipelineID)
	}
	db = layoutV1Sanitize(in.DBOrSchema)
	if db == "" {
		db = "default"
	}
	table = in.Table
	if idx := strings.LastIndex(table, "."); idx >= 0 && idx+1 < len(table) {
		table = table[idx+1:]
	}
	table = layoutV1Sanitize(table)
	dt = strings.TrimSpace(in.Dt)
	if dt == "" {
		dt = time.UnixMilli(in.NowMs).UTC().Format("2006-01-02")
	}
	return dataset, db, table, dt
}

// LayoutV1BatchPartKey is the worker's batch part-file key.
func LayoutV1BatchPartKey(in LayoutV1Input) string {
	format := layoutV1FirstStr(in.DestConfig, "file_format", "format")
	if format == "" {
		format = "csv"
	}
	dataset, db, table, dt := layoutV1BatchPath(in)
	name := fmt.Sprintf("part-%06d", in.Offset)
	if in.PartSuffix != "" {
		name += "-" + in.PartSuffix
	}
	return layoutV1TablePrefix(layoutV1Prefix(in.DestConfig), dataset, db, table) + in.PartSegs + "dt=" + dt + "/" + name + "." + layoutV1FileExt(format, in.Compression)
}

// LayoutV1BatchManifestKey is the worker's batch _MANIFEST.json key.
func LayoutV1BatchManifestKey(in LayoutV1Input) string {
	dataset, db, table, dt := layoutV1BatchPath(in)
	return layoutV1TablePrefix(layoutV1Prefix(in.DestConfig), dataset, db, table) + "dt=" + dt + "/_MANIFEST.json"
}

// LayoutV1BatchSuccessKey is the worker's batch _SUCCESS key.
func LayoutV1BatchSuccessKey(in LayoutV1Input) string {
	dataset, db, table, dt := layoutV1BatchPath(in)
	return layoutV1TablePrefix(layoutV1Prefix(in.DestConfig), dataset, db, table) + "dt=" + dt + "/_SUCCESS"
}

// LayoutV1TablePrefix is the scope of the worker's reload delete_prefix.
func LayoutV1TablePrefix(in LayoutV1Input) string {
	dataset, db, table, _ := layoutV1BatchPath(in)
	return layoutV1TablePrefix(layoutV1Prefix(in.DestConfig), dataset, db, table)
}

// LayoutV1CDCObjectKey is the worker's CDC object key.
func LayoutV1CDCObjectKey(in LayoutV1Input) string {
	format := layoutV1FirstStr(in.DestConfig, "file_format", "format")
	if format == "" {
		format = "jsonl"
	}
	tsMs := in.TsMs
	if tsMs <= 0 {
		tsMs = in.NowMs
	}
	ts := time.UnixMilli(tsMs).UTC()
	var dateSeg string
	switch strings.ToLower(strings.TrimSpace(layoutV1FirstStr(in.DestConfig, "partition_time_granularity"))) {
	case "hour":
		dateSeg = "dt=" + ts.Format("2006-01-02") + "/hour=" + ts.Format("15")
	case "month":
		dateSeg = "dt=" + ts.Format("2006-01")
	case "day":
		dateSeg = "dt=" + ts.Format("2006-01-02")
	default:
		dateSeg = ts.Format("2006-01-02")
	}

	table := in.Table
	schemaFromTable := ""
	if idx := strings.LastIndex(table, "."); idx >= 0 && idx+1 < len(table) {
		schemaFromTable = table[:idx]
		table = table[idx+1:]
	}
	db := layoutV1Sanitize(in.DBOrSchema)
	if db == "" {
		db = layoutV1Sanitize(schemaFromTable)
	}
	if db == "" {
		db = "default"
	}

	ext := format
	if strings.EqualFold(strings.TrimSpace(in.Compression), "gzip") && !strings.EqualFold(strings.TrimSpace(format), "parquet") {
		ext = format + ".gz"
	}
	name := ts.Format("20060102-150405") + fmt.Sprintf("%03d", ts.Nanosecond()/1_000_000)
	if in.Partition > 0 {
		name += fmt.Sprintf("-p%d", in.Partition)
	}
	name += fmt.Sprintf("-%d", in.FirstOffset)
	return layoutV1TablePrefix(layoutV1Prefix(in.DestConfig), layoutV1CDCPipelineSegment(in), db, layoutV1Sanitize(table)) + in.PartSegs + dateSeg + "/" + name + "." + ext
}

func layoutV1CDCPipelineSegment(in LayoutV1Input) string {
	if s := layoutV1Slugify(in.WorkerPipelineID); s != "" {
		return s
	}
	return layoutV1Slugify(in.PipelineID)
}

// LayoutV1CDCPipelineRootPrefix is the prefix the worker lists for get_cdc_offsets, or ""
// when the worker has no pipeline id.
func LayoutV1CDCPipelineRootPrefix(in LayoutV1Input) string {
	seg := layoutV1Slugify(in.WorkerPipelineID)
	if seg == "" {
		return ""
	}
	prefix := strings.Trim(layoutV1Prefix(in.DestConfig), "/")
	if prefix == "" {
		return seg + "/"
	}
	return prefix + "/" + seg + "/"
}

// ---- v2 ----------------------------------------------------------------------------

// LayoutV2Table names one table's place in the v2 layout. SourceFamily decides the
// namespace depth: postgresql/sqlserver/oracle use <db>/<schema>, mongodb/mysql use <db>,
// and "" (a non-database source) uses neither.
type LayoutV2Table struct {
	ConnPrefix     string `json:"conn_prefix"`
	PipelinePrefix string `json:"pipeline_prefix"`
	SourceFamily   string `json:"source_family"`
	Database       string `json:"database"`
	Schema         string `json:"schema"`
	Table          string `json:"table"`
}

// LayoutV2Error carries a stable code shared with the golden file and every port.
type LayoutV2Error struct {
	Code string
}

func (e *LayoutV2Error) Error() string { return "object layout v2: " + e.Code }

const layoutV2Space = " \t\n\r\v\f"

var (
	layoutV2PipelinePrefixRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	layoutV2DtRe             = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
)

// layoutV2EncodeName percent-encodes, byte by byte, %, /, \, =, control bytes, and a
// leading _ or . (hidden files to query engines). Everything else, case included, stays.
func layoutV2EncodeName(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' || c == '/' || c == '\\' || c == '=' || c < 0x20 || c == 0x7f || (i == 0 && (c == '_' || c == '.')) {
			fmt.Fprintf(&b, "%%%02X", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// LayoutV2PipelineRootPrefix is "<conn prefix>/<pipeline prefix>/".
func LayoutV2PipelineRootPrefix(connPrefix, pipelinePrefix string) (string, error) {
	if !layoutV2PipelinePrefixRe.MatchString(pipelinePrefix) {
		return "", &LayoutV2Error{Code: "pipeline_prefix_invalid"}
	}
	conn := strings.Trim(strings.Trim(connPrefix, layoutV2Space), "/")
	if conn == "" {
		return pipelinePrefix + "/", nil
	}
	return conn + "/" + pipelinePrefix + "/", nil
}

func layoutV2Namespace(t LayoutV2Table) (string, error) {
	db := strings.Trim(t.Database, layoutV2Space)
	schema := strings.Trim(t.Schema, layoutV2Space)
	table := strings.Trim(t.Table, layoutV2Space)
	var parts []string
	switch strings.ToLower(strings.Trim(t.SourceFamily, layoutV2Space)) {
	case "postgresql", "sqlserver", "oracle":
		if db == "" {
			return "", &LayoutV2Error{Code: "database_required"}
		}
		if schema == "" {
			return "", &LayoutV2Error{Code: "schema_required"}
		}
		parts = append(parts, db, schema)
	case "mongodb", "mysql":
		if db == "" {
			return "", &LayoutV2Error{Code: "database_required"}
		}
		parts = append(parts, db)
	case "":
	default:
		return "", &LayoutV2Error{Code: "source_family_unknown"}
	}
	if table == "" {
		return "", &LayoutV2Error{Code: "table_required"}
	}
	parts = append(parts, table)
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(layoutV2EncodeName(p))
		b.WriteByte('/')
	}
	return b.String(), nil
}

// LayoutV2TablePrefix is "<conn>/<pp>/<db>/[<schema>/]<table>/".
func LayoutV2TablePrefix(t LayoutV2Table) (string, error) {
	root, err := LayoutV2PipelineRootPrefix(t.ConnPrefix, t.PipelinePrefix)
	if err != nil {
		return "", err
	}
	ns, err := layoutV2Namespace(t)
	if err != nil {
		return "", err
	}
	return root + ns, nil
}

// LayoutV2SidecarTablePrefix is "<conn>/<pp>/_rsync/<db>/[<schema>/]<table>/".
func LayoutV2SidecarTablePrefix(t LayoutV2Table) (string, error) {
	root, err := LayoutV2PipelineRootPrefix(t.ConnPrefix, t.PipelinePrefix)
	if err != nil {
		return "", err
	}
	ns, err := layoutV2Namespace(t)
	if err != nil {
		return "", err
	}
	return root + "_rsync/" + ns, nil
}

func layoutV2CheckDt(dt string) error {
	if !layoutV2DtRe.MatchString(dt) {
		return &LayoutV2Error{Code: "dt_invalid"}
	}
	// Year 0000 parses in Go but not in Python; reject it so the ports agree.
	if _, err := time.Parse("2006-01-02", dt); err != nil || strings.HasPrefix(dt, "0000") {
		return &LayoutV2Error{Code: "dt_invalid"}
	}
	return nil
}

// LayoutV2LoadKey is a batch load file: "<table prefix>dt=<dt>/LOAD<8 digits>.parquet".
func LayoutV2LoadKey(t LayoutV2Table, dt string, loadSeq int64) (string, error) {
	prefix, err := LayoutV2TablePrefix(t)
	if err != nil {
		return "", err
	}
	if err := layoutV2CheckDt(dt); err != nil {
		return "", err
	}
	if loadSeq < 1 || loadSeq > 99999999 {
		return "", &LayoutV2Error{Code: "load_seq_out_of_range"}
	}
	return fmt.Sprintf("%sdt=%s/LOAD%08d.parquet", prefix, dt, loadSeq), nil
}

// LayoutV2CDCKey is a CDC file: "<table prefix>dt=<UTC day>/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<first offset>.parquet".
func LayoutV2CDCKey(t LayoutV2Table, tsMs int64, partition int, firstOffset int64) (string, error) {
	prefix, err := LayoutV2TablePrefix(t)
	if err != nil {
		return "", err
	}
	// Upper bound: 10000-01-01T00:00:00Z would render a 5-digit year.
	if tsMs <= 0 || tsMs >= 253402300800000 {
		return "", &LayoutV2Error{Code: "ts_invalid"}
	}
	if partition < 0 {
		return "", &LayoutV2Error{Code: "partition_invalid"}
	}
	if firstOffset < 0 {
		return "", &LayoutV2Error{Code: "offset_invalid"}
	}
	ts := time.UnixMilli(tsMs).UTC()
	name := ts.Format("20060102-150405") + fmt.Sprintf("%03d", ts.Nanosecond()/1_000_000)
	if partition > 0 {
		name += fmt.Sprintf("-p%d", partition)
	}
	return fmt.Sprintf("%sdt=%s/%s-%d.parquet", prefix, ts.Format("2006-01-02"), name, firstOffset), nil
}

func layoutV2SidecarKey(t LayoutV2Table, dt, leaf string) (string, error) {
	prefix, err := LayoutV2SidecarTablePrefix(t)
	if err != nil {
		return "", err
	}
	if err := layoutV2CheckDt(dt); err != nil {
		return "", err
	}
	return prefix + "dt=" + dt + "/" + leaf, nil
}

// LayoutV2ManifestKey is "<sidecar table prefix>dt=<dt>/_MANIFEST.json".
func LayoutV2ManifestKey(t LayoutV2Table, dt string) (string, error) {
	return layoutV2SidecarKey(t, dt, "_MANIFEST.json")
}

// LayoutV2SuccessKey is "<sidecar table prefix>dt=<dt>/_SUCCESS".
func LayoutV2SuccessKey(t LayoutV2Table, dt string) (string, error) {
	return layoutV2SidecarKey(t, dt, "_SUCCESS")
}
