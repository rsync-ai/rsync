// Package storage provides utilities for cloud storage operations
package storage

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// KeyBuilder builds cloud storage object keys using the standard layout:
// {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/part-{offset:06d}.{format}[.{compression}]
type KeyBuilder struct {
	PathPrefix  string // From destination connection config
	Dataset     string // Pipeline-level namespace (slug of pipeline name or user override)
	DBOrSchema  string // Source database/schema name
	Table       string // Normalized table name
	Format      string // File format (json, jsonl, parquet, csv, etc.)
	Compression string // Compression type (none, gzip, zstd, snappy, lz4)
	RunDate     string // Ingestion/run date in UTC (YYYY-MM-DD format)
}

// Manifest represents the _MANIFEST.json content for a partition
type Manifest struct {
	PipelineID   string   `json:"pipeline_id"`
	ExecutionID  string   `json:"execution_id"`
	Dt           string   `json:"dt"`
	UploadedKeys []string `json:"uploaded_keys"`
	RowCounts    []int64  `json:"row_counts"` // Row count per key (parallel to UploadedKeys)
	TotalRows    int64    `json:"total_rows"`
	Watermark    any      `json:"watermark,omitempty"` // Optional watermark for incremental syncs
	CreatedAt    string   `json:"created_at"`
}

// NewKeyBuilder creates a new KeyBuilder with defaults
func NewKeyBuilder() *KeyBuilder {
	return &KeyBuilder{
		PathPrefix:  "staging",
		Format:      "json",
		Compression: "none",
		RunDate:     time.Now().UTC().Format("2006-01-02"),
	}
}

// WithPathPrefix sets the path prefix from destination config
func (kb *KeyBuilder) WithPathPrefix(prefix string) *KeyBuilder {
	kb.PathPrefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if kb.PathPrefix == "" {
		kb.PathPrefix = "staging"
	}
	return kb
}

// WithDataset sets the dataset namespace
func (kb *KeyBuilder) WithDataset(dataset string) *KeyBuilder {
	kb.Dataset = Slugify(dataset)
	return kb
}

// WithDBOrSchema sets the database/schema name
func (kb *KeyBuilder) WithDBOrSchema(dbOrSchema string) *KeyBuilder {
	kb.DBOrSchema = SanitizePath(dbOrSchema)
	return kb
}

// WithTable sets the table name
func (kb *KeyBuilder) WithTable(table string) *KeyBuilder {
	kb.Table = SanitizePath(table)
	return kb
}

// WithFormat sets the file format
func (kb *KeyBuilder) WithFormat(format string) *KeyBuilder {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "json"
	}
	// Normalize format names
	switch format {
	case "ndjson", "json_lines", "jsonlines":
		format = "jsonl"
	}
	kb.Format = format
	return kb
}

// WithCompression sets the compression type
func (kb *KeyBuilder) WithCompression(compression string) *KeyBuilder {
	compression = strings.ToLower(strings.TrimSpace(compression))
	if compression == "" {
		compression = "none"
	}
	kb.Compression = compression
	return kb
}

// WithRunDate sets the run date (YYYY-MM-DD format in UTC)
func (kb *KeyBuilder) WithRunDate(runDate string) *KeyBuilder {
	kb.RunDate = runDate
	return kb
}

// WithRunTime sets the run date from a time.Time value
func (kb *KeyBuilder) WithRunTime(t time.Time) *KeyBuilder {
	kb.RunDate = t.UTC().Format("2006-01-02")
	return kb
}

// PartitionPrefix returns the prefix path up to and including the dt partition
// Format: {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/
func (kb *KeyBuilder) PartitionPrefix() string {
	parts := []string{kb.PathPrefix}
	if kb.Dataset != "" {
		parts = append(parts, kb.Dataset)
	}
	if kb.DBOrSchema != "" {
		parts = append(parts, kb.DBOrSchema)
	}
	if kb.Table != "" {
		parts = append(parts, kb.Table)
	}
	parts = append(parts, fmt.Sprintf("dt=%s", kb.RunDate))
	return strings.Join(parts, "/") + "/"
}

// TablePrefix returns the prefix path up to and including the table (without dt partition)
// Format: {prefix}/{dataset}/{db_or_schema}/{table}/
// This is used for reload operations that clean the entire table's data
func (kb *KeyBuilder) TablePrefix() string {
	parts := []string{kb.PathPrefix}
	if kb.Dataset != "" {
		parts = append(parts, kb.Dataset)
	}
	if kb.DBOrSchema != "" {
		parts = append(parts, kb.DBOrSchema)
	}
	if kb.Table != "" {
		parts = append(parts, kb.Table)
	}
	return strings.Join(parts, "/") + "/"
}

// DatasetPrefix returns the prefix path for the entire dataset
// Format: {prefix}/{dataset}/
// This is used for full reload operations
func (kb *KeyBuilder) DatasetPrefix() string {
	parts := []string{kb.PathPrefix}
	if kb.Dataset != "" {
		parts = append(parts, kb.Dataset)
	}
	return strings.Join(parts, "/") + "/"
}

// fileExtension returns the file extension based on format and compression
func (kb *KeyBuilder) fileExtension() string {
	ext := kb.Format
	if kb.Compression != "" && kb.Compression != "none" {
		switch kb.Compression {
		case "gzip", "gz":
			ext = fmt.Sprintf("%s.gz", ext)
		case "zstd", "zst":
			ext = fmt.Sprintf("%s.zst", ext)
		case "snappy":
			ext = fmt.Sprintf("%s.snappy", ext)
		case "lz4":
			ext = fmt.Sprintf("%s.lz4", ext)
		default:
			ext = fmt.Sprintf("%s.%s", ext, kb.Compression)
		}
	}
	return ext
}

// PartKey returns the object key for a batch file
// Format: {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/part-{offset:06d}.{ext}
func (kb *KeyBuilder) PartKey(offset int) string {
	return fmt.Sprintf("%spart-%06d.%s", kb.PartitionPrefix(), offset, kb.fileExtension())
}

// CDCKey returns the object key for a CDC batch file
// Format: {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/cdc-{seq:012d}.{ext}
func (kb *KeyBuilder) CDCKey(seq int64) string {
	return fmt.Sprintf("%scdc-%012d.%s", kb.PartitionPrefix(), seq, kb.fileExtension())
}

// CDCRangeKey returns the object key for a CDC batch file with offset range
// Format: {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/cdc-{fromOffset}-{toOffset}.{ext}
func (kb *KeyBuilder) CDCRangeKey(fromOffset, toOffset int64) string {
	return fmt.Sprintf("%scdc-%d-%d.%s", kb.PartitionPrefix(), fromOffset, toOffset, kb.fileExtension())
}

// ManifestKey returns the object key for the _MANIFEST.json file
// Format: {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/_MANIFEST.json
func (kb *KeyBuilder) ManifestKey() string {
	return fmt.Sprintf("%s_MANIFEST.json", kb.PartitionPrefix())
}

// SuccessMarkerKey returns the object key for the _SUCCESS marker
// Format: {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/_SUCCESS
func (kb *KeyBuilder) SuccessMarkerKey() string {
	return fmt.Sprintf("%s_SUCCESS", kb.PartitionPrefix())
}

// BuildManifest creates a Manifest struct for the current partition
func (kb *KeyBuilder) BuildManifest(pipelineID, executionID string, keys []string, rowCounts []int64, watermark any) *Manifest {
	var totalRows int64
	for _, count := range rowCounts {
		totalRows += count
	}
	return &Manifest{
		PipelineID:   pipelineID,
		ExecutionID:  executionID,
		Dt:           kb.RunDate,
		UploadedKeys: keys,
		RowCounts:    rowCounts,
		TotalRows:    totalRows,
		Watermark:    watermark,
		CreatedAt:    time.Now().UTC().Format(time.RFC3339),
	}
}

// MarshalManifest returns the JSON representation of a Manifest
func MarshalManifest(m *Manifest) ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// UnmarshalManifest parses a Manifest from JSON
func UnmarshalManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Slugify converts a string to a URL/filesystem-safe slug
// Used for converting pipeline names to dataset namespaces
func Slugify(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Convert to lowercase
	s = strings.ToLower(s)

	// Replace common separators with hyphens
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")

	// Keep only alphanumeric and hyphens
	var result strings.Builder
	lastWasHyphen := false
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			result.WriteRune(r)
			lastWasHyphen = false
		} else if r == '-' && !lastWasHyphen && result.Len() > 0 {
			result.WriteRune('-')
			lastWasHyphen = true
		}
	}

	// Trim trailing hyphens
	return strings.TrimRight(result.String(), "-")
}

// SanitizePath removes or replaces characters that could cause path traversal or issues
func SanitizePath(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Replace path separators and dangerous characters
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "..", "_")
	s = strings.ReplaceAll(s, ":", "_")

	// Remove control characters
	var result strings.Builder
	for _, r := range s {
		if unicode.IsPrint(r) && r != '\n' && r != '\r' && r != '\t' {
			result.WriteRune(r)
		}
	}

	return result.String()
}

// ValidateDatasetUniqueness checks if a dataset name would be unique for the destination connection
// This should be called from the API layer when creating/updating pipelines
// Returns an error if the dataset is already used by another pipeline on the same destination
type DatasetConflictError struct {
	Dataset             string
	DestinationConnID   string
	ConflictingPipeline string
}

func (e *DatasetConflictError) Error() string {
	return fmt.Sprintf("dataset '%s' is already used by pipeline '%s' on destination connection '%s'",
		e.Dataset, e.ConflictingPipeline, e.DestinationConnID)
}

// LockKey represents a concurrency lock key for storage operations
type LockKey struct {
	DestConnID string
	Dataset    string
	DB         string
	Table      string
	Dt         string // Only for batch locks (empty for CDC)
}

// BatchLockKey creates a lock key for batch operations
// Lock scope: (dest_conn_id, dataset, db, table, dt)
func BatchLockKey(destConnID, dataset, db, table, dt string) LockKey {
	return LockKey{
		DestConnID: destConnID,
		Dataset:    dataset,
		DB:         db,
		Table:      table,
		Dt:         dt,
	}
}

// CDCLockKey creates a lock key for CDC operations
// Lock scope: (dest_conn_id, dataset, db, table)
func CDCLockKey(destConnID, dataset, db, table string) LockKey {
	return LockKey{
		DestConnID: destConnID,
		Dataset:    dataset,
		DB:         db,
		Table:      table,
	}
}

// String returns a string representation of the lock key
func (lk LockKey) String() string {
	if lk.Dt != "" {
		return fmt.Sprintf("batch:%s:%s:%s:%s:%s", lk.DestConnID, lk.Dataset, lk.DB, lk.Table, lk.Dt)
	}
	return fmt.Sprintf("cdc:%s:%s:%s:%s", lk.DestConnID, lk.Dataset, lk.DB, lk.Table)
}

// RunMode represents the sync execution mode
type RunMode string

const (
	// RunModeResume continues from checkpoints/watermarks without duplication (default)
	RunModeResume RunMode = "resume"
	// RunModeReload rebuilds destination from scratch (capability gated)
	RunModeReload RunMode = "reload"
)

// IsValid checks if the run mode is valid
func (rm RunMode) IsValid() bool {
	return rm == RunModeResume || rm == RunModeReload
}

// ParseRunMode parses a string to RunMode, defaulting to resume
func ParseRunMode(s string) RunMode {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == string(RunModeReload) {
		return RunModeReload
	}
	return RunModeResume
}

// WriteConflictError represents a pipeline write conflict (HTTP 409)
type WriteConflictError struct {
	LockKey            LockKey
	ConflictingPipeline string
	ConflictingExecID   string
}

func (e *WriteConflictError) Error() string {
	return fmt.Sprintf("pipeline_write_conflict: lock %s is held by pipeline '%s' (execution '%s')",
		e.LockKey.String(), e.ConflictingPipeline, e.ConflictingExecID)
}

// ExtractSchemaAndTable extracts schema/db and table from a qualified table name
// Handles formats like: "schema.table", "db.schema.table", "table"
func ExtractSchemaAndTable(qualifiedName string) (schema, table string) {
	parts := strings.Split(qualifiedName, ".")
	switch len(parts) {
	case 1:
		return "", parts[0]
	case 2:
		return parts[0], parts[1]
	case 3:
		// db.schema.table - use schema as the schema part
		return parts[1], parts[2]
	default:
		// Too many parts, just take last two
		n := len(parts)
		return parts[n-2], parts[n-1]
	}
}

// ValidDatasetPattern is a regex for valid dataset names
var ValidDatasetPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$|^[a-z0-9]$`)

// ValidateDataset checks if a dataset name is valid
func ValidateDataset(dataset string) error {
	if dataset == "" {
		return fmt.Errorf("dataset cannot be empty")
	}
	if len(dataset) > 63 {
		return fmt.Errorf("dataset name too long (max 63 characters)")
	}
	if !ValidDatasetPattern.MatchString(dataset) {
		return fmt.Errorf("dataset must contain only lowercase alphanumeric characters and hyphens, and must start/end with alphanumeric")
	}
	return nil
}
