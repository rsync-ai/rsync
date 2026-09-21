package main

// Object-storage key layout contract (L6).
//
// This file holds pure key builders only. The v1 functions compose the helpers the v1
// writers use (partKey, manifestKey, successKey, tablePrefix, fileExt, cdcObjectPath,
// cdcPipelineSegment, cdcObjectKey, timePartitionSegment, cdcPipelineRootPrefix) with
// the normalization those writers do inline, so the shared golden file pins what a v1
// pipeline writes. The v2 functions are called directly by the layout v2 writers
// (object_layout_v2_write.go) for a GCS pipeline with storage_layout_version 2.
//
// Both are pinned by shared/object_layout_golden.json, which the orchestrator
// (backend-orchestrator/internal/storage/layout.go) and rsync_protocol
// (shared/mcp-connectors/public/rsync_protocol/object_layout.py, v2 only) also read.
// Every identifier here starts with objectLayout so it cannot clash with the rest of
// package main.

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// objectLayoutV1Input carries every value a v1 key depends on. DestConfig is the
// destination connection config the writers read (prefix keys, file format,
// partition_time_granularity). Compression is the already-resolved value
// (objectStorageCompression). NowMs stands in for time.Now when Dt or TsMs is unset.
type objectLayoutV1Input struct {
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

func objectLayoutV1Prefix(destCfg map[string]interface{}) string {
	return firstStr(destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path")
}

// objectLayoutV1BatchPath is the batch writer's inline normalization (the object-storage
// branch of the batch write, and ensureWriteState for the manifest and reload paths).
func objectLayoutV1BatchPath(in objectLayoutV1Input) (dataset, dbOrSchema, table, dt string) {
	dataset = slugify(in.Dataset)
	if dataset == "" {
		dataset = slugify(in.PipelineID)
	}
	dbOrSchema = sanitizePathPart(in.DBOrSchema)
	if dbOrSchema == "" {
		dbOrSchema = "default"
	}
	table = in.Table
	if idx := strings.LastIndex(table, "."); idx >= 0 && idx+1 < len(table) {
		table = table[idx+1:]
	}
	table = sanitizePathPart(table)
	dt = strings.TrimSpace(in.Dt)
	if dt == "" {
		dt = time.UnixMilli(in.NowMs).UTC().Format("2006-01-02")
	}
	return dataset, dbOrSchema, table, dt
}

func objectLayoutV1BatchPartKey(in objectLayoutV1Input) string {
	format := firstStr(in.DestConfig, "file_format", "format")
	if format == "" {
		format = "csv"
	}
	dataset, db, table, dt := objectLayoutV1BatchPath(in)
	return partKey(objectLayoutV1Prefix(in.DestConfig), dataset, db, table, in.PartSegs, dt, in.Offset, in.PartSuffix, fileExt(format, in.Compression))
}

func objectLayoutV1BatchManifestKey(in objectLayoutV1Input) string {
	dataset, db, table, dt := objectLayoutV1BatchPath(in)
	return manifestKey(objectLayoutV1Prefix(in.DestConfig), dataset, db, table, dt)
}

func objectLayoutV1BatchSuccessKey(in objectLayoutV1Input) string {
	dataset, db, table, dt := objectLayoutV1BatchPath(in)
	return successKey(objectLayoutV1Prefix(in.DestConfig), dataset, db, table, dt)
}

// objectLayoutV1TablePrefix is the scope of the worker's reload delete_prefix.
func objectLayoutV1TablePrefix(in objectLayoutV1Input) string {
	dataset, db, table, _ := objectLayoutV1BatchPath(in)
	return tablePrefix(objectLayoutV1Prefix(in.DestConfig), dataset, db, table)
}

func objectLayoutV1CDCObjectKey(in objectLayoutV1Input) string {
	format := firstStr(in.DestConfig, "file_format", "format")
	if format == "" {
		format = "jsonl"
	}
	ts := in.TsMs
	if ts <= 0 {
		ts = in.NowMs
	}
	dateSeg := timePartitionSegment(ts, firstStr(in.DestConfig, "partition_time_granularity"))
	sm := &SinkMessage{PipelineID: in.PipelineID, DBOrSchema: in.DBOrSchema, Table: in.Table}
	db, table := cdcObjectPath(sm)
	return cdcObjectKey(objectLayoutV1Prefix(in.DestConfig), cdcPipelineSegment(&WorkerConfig{PipelineID: in.WorkerPipelineID}, sm), db, table, dateSeg, in.PartSegs, ts, in.Partition, in.FirstOffset, in.FirstOffset, format, in.Compression)
}

func objectLayoutV1CDCPipelineRootPrefix(in objectLayoutV1Input) string {
	return cdcPipelineRootPrefix(in.DestConfig, &WorkerConfig{PipelineID: in.WorkerPipelineID})
}

// ---- v2 ----------------------------------------------------------------------------

// objectLayoutV2Table names one table's place in the v2 layout. SourceFamily decides the
// namespace depth: postgresql/sqlserver/oracle use <db>/<schema>, mongodb/mysql use <db>,
// and "" (a non-database source) uses neither.
type objectLayoutV2Table struct {
	ConnPrefix     string `json:"conn_prefix"`
	PipelinePrefix string `json:"pipeline_prefix"`
	SourceFamily   string `json:"source_family"`
	Database       string `json:"database"`
	Schema         string `json:"schema"`
	Table          string `json:"table"`
}

// objectLayoutV2Error carries a stable code the golden file and every port share.
type objectLayoutV2Error struct {
	Code string
}

func (e *objectLayoutV2Error) Error() string { return "object layout v2: " + e.Code }

const objectLayoutV2Space = " \t\n\r\v\f"

var objectLayoutV2PipelinePrefixRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
var objectLayoutV2DtRe = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)

// objectLayoutV2EncodeName percent-encodes, byte by byte, the characters that would
// break a key or a Hive reader: %, /, \, =, control bytes, and a leading _ or . (which
// query engines treat as hidden files). Everything else, case included, is kept.
func objectLayoutV2EncodeName(s string) string {
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

func objectLayoutV2PipelineRootPrefix(connPrefix, pipelinePrefix string) (string, error) {
	if !objectLayoutV2PipelinePrefixRe.MatchString(pipelinePrefix) {
		return "", &objectLayoutV2Error{Code: "pipeline_prefix_invalid"}
	}
	conn := strings.Trim(strings.Trim(connPrefix, objectLayoutV2Space), "/")
	if conn == "" {
		return pipelinePrefix + "/", nil
	}
	return conn + "/" + pipelinePrefix + "/", nil
}

// objectLayoutV2Namespace returns "<db>/[<schema>/]<table>/" for the table.
func objectLayoutV2Namespace(t objectLayoutV2Table) (string, error) {
	db := strings.Trim(t.Database, objectLayoutV2Space)
	schema := strings.Trim(t.Schema, objectLayoutV2Space)
	table := strings.Trim(t.Table, objectLayoutV2Space)
	var parts []string
	switch strings.ToLower(strings.Trim(t.SourceFamily, objectLayoutV2Space)) {
	case "postgresql", "sqlserver", "oracle":
		if db == "" {
			return "", &objectLayoutV2Error{Code: "database_required"}
		}
		if schema == "" {
			return "", &objectLayoutV2Error{Code: "schema_required"}
		}
		parts = append(parts, db, schema)
	case "mongodb", "mysql":
		if db == "" {
			return "", &objectLayoutV2Error{Code: "database_required"}
		}
		parts = append(parts, db)
	case "":
	default:
		return "", &objectLayoutV2Error{Code: "source_family_unknown"}
	}
	if table == "" {
		return "", &objectLayoutV2Error{Code: "table_required"}
	}
	parts = append(parts, table)
	out := ""
	for _, p := range parts {
		out += objectLayoutV2EncodeName(p) + "/"
	}
	return out, nil
}

func objectLayoutV2TablePrefix(t objectLayoutV2Table) (string, error) {
	root, err := objectLayoutV2PipelineRootPrefix(t.ConnPrefix, t.PipelinePrefix)
	if err != nil {
		return "", err
	}
	ns, err := objectLayoutV2Namespace(t)
	if err != nil {
		return "", err
	}
	return root + ns, nil
}

// objectLayoutV2SidecarTablePrefix is where _MANIFEST.json and _SUCCESS live: under
// <pipeline root>/_rsync/, so a table folder holds data files only.
func objectLayoutV2SidecarTablePrefix(t objectLayoutV2Table) (string, error) {
	root, err := objectLayoutV2PipelineRootPrefix(t.ConnPrefix, t.PipelinePrefix)
	if err != nil {
		return "", err
	}
	ns, err := objectLayoutV2Namespace(t)
	if err != nil {
		return "", err
	}
	return root + "_rsync/" + ns, nil
}

func objectLayoutV2CheckDt(dt string) error {
	if !objectLayoutV2DtRe.MatchString(dt) {
		return &objectLayoutV2Error{Code: "dt_invalid"}
	}
	// Year 0000 parses in Go but not in Python; reject it so the ports agree.
	if _, err := time.Parse("2006-01-02", dt); err != nil || strings.HasPrefix(dt, "0000") {
		return &objectLayoutV2Error{Code: "dt_invalid"}
	}
	return nil
}

func objectLayoutV2LoadKey(t objectLayoutV2Table, dt string, loadSeq int64) (string, error) {
	prefix, err := objectLayoutV2TablePrefix(t)
	if err != nil {
		return "", err
	}
	if err := objectLayoutV2CheckDt(dt); err != nil {
		return "", err
	}
	if loadSeq < 1 || loadSeq > 99999999 {
		return "", &objectLayoutV2Error{Code: "load_seq_out_of_range"}
	}
	return fmt.Sprintf("%sdt=%s/LOAD%08d.parquet", prefix, dt, loadSeq), nil
}

func objectLayoutV2CDCKey(t objectLayoutV2Table, tsMs int64, partition int, firstOffset int64) (string, error) {
	prefix, err := objectLayoutV2TablePrefix(t)
	if err != nil {
		return "", err
	}
	// Upper bound: 10000-01-01T00:00:00Z would render a 5-digit year.
	if tsMs <= 0 || tsMs >= 253402300800000 {
		return "", &objectLayoutV2Error{Code: "ts_invalid"}
	}
	if partition < 0 {
		return "", &objectLayoutV2Error{Code: "partition_invalid"}
	}
	if firstOffset < 0 {
		return "", &objectLayoutV2Error{Code: "offset_invalid"}
	}
	ts := time.UnixMilli(tsMs).UTC()
	name := ts.Format("20060102-150405") + fmt.Sprintf("%03d", ts.Nanosecond()/1_000_000)
	if partition > 0 {
		name += fmt.Sprintf("-p%d", partition)
	}
	return fmt.Sprintf("%sdt=%s/%s-%d.parquet", prefix, ts.Format("2006-01-02"), name, firstOffset), nil
}

func objectLayoutV2SidecarKey(t objectLayoutV2Table, dt, leaf string) (string, error) {
	prefix, err := objectLayoutV2SidecarTablePrefix(t)
	if err != nil {
		return "", err
	}
	if err := objectLayoutV2CheckDt(dt); err != nil {
		return "", err
	}
	return prefix + "dt=" + dt + "/" + leaf, nil
}

func objectLayoutV2ManifestKey(t objectLayoutV2Table, dt string) (string, error) {
	return objectLayoutV2SidecarKey(t, dt, "_MANIFEST.json")
}

func objectLayoutV2SuccessKey(t objectLayoutV2Table, dt string) (string, error) {
	return objectLayoutV2SidecarKey(t, dt, "_SUCCESS")
}
