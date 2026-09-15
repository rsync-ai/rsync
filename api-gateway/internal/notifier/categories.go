package notifier

import "strings"

// Alert categories — the unit a person opts in or out of.
//
// A category groups catalog codes by the question a reader asks of an alert
// ("is my data at risk?", "did my run fail?"), not by which agent emitted it.
// The admin mutes categories per instance for Slack; each user mutes them for
// their own email. Both are stored as MUTED lists (migration 102), so every
// category, including one added in a later release, is delivered unless
// someone switched it off.
//
// ── Adding a catalog code ─────────────────────────────────────────────────────
// Add it to codeCategory below in the same change as the catalog entry.
// TestEveryCatalogCodeHasACategory fails otherwise. The fallback for a code
// nobody categorized is CategoryOther, which is delivered by default, so a
// forgotten mapping still alerts. It just can't be muted on its own.

const (
	CategoryDataLoss    = "data_loss"
	CategoryRunStatus   = "run_status"
	CategorySchemaDrift = "schema_drift"
	CategoryHealth      = "health"
	CategoryCredentials = "credentials"
	CategorySourceSetup = "source_setup"
	CategoryOther       = "other"
)

// Category is one mutable group of alerts, as the settings UI lists it.
type Category struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
}

// categories is in display order. Data loss is first on purpose: it is the one
// a reader should think hardest about before muting.
var categories = []Category{
	{CategoryDataLoss, "Data loss & integrity", "Rows may be missing, or real-time sync lost its place and needs a fresh sync."},
	{CategoryRunStatus, "Run failed or stopped", "A pipeline run ended with an error or was stopped before it finished."},
	{CategorySchemaDrift, "Schema changes", "The source schema changed and needs approval, or an approved change was applied."},
	{CategoryHealth, "Health & capacity", "The destination is out of space, the source is rate limiting, or the network is retrying."},
	{CategoryCredentials, "Credentials & configuration", "A connection expired or lost a permission, or a pipeline setting needs fixing."},
	{CategorySourceSetup, "Source setup", "A source database setting or table blocks syncing (replication, CDC, primary keys)."},
	{CategoryOther, "Other alerts", "Anything not classified above, including problems we could not classify."},
}

// codeCategory maps every catalog code to its category. Keys must match
// catalog.go exactly.
var codeCategory = map[string]string{
	// Rows may be gone. MONGODB_RESUME_TOKEN_INVALID is here and not under
	// source setup because the change stream has moved past the last applied
	// event: the changes in between are not coming back without a fresh sync.
	"RSYNC_BUG_SILENT_DROP":           CategoryDataLoss,
	"RSYNC_BUG_OWNERSHIP_ROW_MISSING": CategoryDataLoss,
	"MONGODB_RESUME_TOKEN_INVALID":    CategoryDataLoss,

	"PIPELINE_RUN_FAILED":  CategoryRunStatus,
	"PIPELINE_RUN_STOPPED": CategoryRunStatus,

	"SCHEMA_DRIFT_DETECTED": CategorySchemaDrift,
	codeSchemaChangeApplied: CategorySchemaDrift,

	"DESTINATION_CAPACITY_EXCEEDED": CategoryHealth,
	"RATE_LIMIT_EXCEEDED":           CategoryHealth,
	"NETWORK_TRANSIENT_FAILURE":     CategoryHealth,

	"AUTH_TOKEN_EXPIRED":      CategoryCredentials,
	"AUTH_SCOPE_INSUFFICIENT": CategoryCredentials,
	"USER_CONFIG_INVALID":     CategoryCredentials,

	"CDC_TABLE_MISSING_PRIMARY_KEY":       CategorySourceSetup,
	"CDC_TABLE_NOT_FOUND_IN_SOURCE":       CategorySourceSetup,
	"MYSQL_BINLOG_FORMAT_NOT_ROW":         CategorySourceSetup,
	"MYSQL_BINLOG_ROW_IMAGE_NOT_FULL":     CategorySourceSetup,
	"POSTGRES_PUBLICATION_DOES_NOT_EXIST": CategorySourceSetup,
	"POSTGRES_WAL_LEVEL_NOT_LOGICAL":      CategorySourceSetup,
	"POSTGRES_REPLICATION_SLOT_CONFLICT":  CategorySourceSetup,
	"SQLSERVER_CDC_NOT_ENABLED":           CategorySourceSetup,
	"SQLSERVER_CDC_TIER_UNSUPPORTED":      CategorySourceSetup,
	"SQLSERVER_AGENT_NOT_RUNNING":         CategorySourceSetup,
	"SQLSERVER_CAPTURE_INSTANCE_ERROR":    CategorySourceSetup,
	"MONGODB_NOT_REPLICA_SET":             CategorySourceSetup,

	// The classifier gave up. These are still delivered by default: an
	// unclassified failure is exactly the alert nobody should miss by accident.
	"UNKNOWN_ERROR":       CategoryOther,
	"LEGACY_UNCLASSIFIED": CategoryOther,
}

// typeCategory covers producers that publish a bare event type with no code.
var typeCategory = map[string]string{
	// sentinel/cdc_wal_watchdog.go: retained WAL is growing behind a slot.
	// Left alone the source disk fills or the slot is invalidated, and an
	// invalidated slot loses every change it had not delivered.
	"cdc_wal_pressure": CategoryDataLoss,
}

// Categories returns every category in display order.
func Categories() []Category {
	out := make([]Category, len(categories))
	copy(out, categories)
	return out
}

// IsCategory reports whether id names a known category.
func IsCategory(id string) bool {
	for _, c := range categories {
		if c.ID == id {
			return true
		}
	}
	return false
}

// CategoryFor classifies one event. The code wins over the type; anything
// unmapped is CategoryOther.
func CategoryFor(code, eventType string) string {
	if c, ok := codeCategory[strings.TrimSpace(code)]; ok {
		return c
	}
	if c, ok := typeCategory[strings.ToLower(strings.TrimSpace(eventType))]; ok {
		return c
	}
	return CategoryOther
}

// mutedSet turns a stored muted-category array into a lookup, dropping ids
// that no longer name a category.
func mutedSet(ids []string) map[string]bool {
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		if IsCategory(id) {
			out[id] = true
		}
	}
	return out
}
