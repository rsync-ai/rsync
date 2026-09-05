package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rsync-ai/shared/naming"
	log "github.com/sirupsen/logrus"
)

// destination_mapping.go implements the destination-mapping HITL (PR-C) checks
// that fold into the pre-migration assessment. It validates the per-pipeline
// destination namespace the user chose (pipelines.config.destination_config):
// the name is identifier-safe, and — for relational destinations — whether the
// namespace already exists and whether the connection user may CREATE it.
//
// Hard constraints honoured here:
//   - The mapping is read from the PIPELINE config, never the connection config.
//   - Names are validated with the shared naming guard (PR #130 "the" disaster).
//   - Creating a new schema/database requires explicit confirmation: a missing
//     namespace with create enabled surfaces as a WARNING the user must ack;
//     a missing namespace with create disabled (or no privilege) is an ERROR.

// pipelineDestinationConfig is the structured mapping persisted under
// pipelines.config.destination_config. Mirrors the api-gateway DestinationConfig
// type but lives here so the assessment can decode it from raw config JSON.
type pipelineDestinationConfig struct {
	Namespace         string `json:"namespace"`
	NamespaceKind     string `json:"namespace_kind"`
	CreateIfNotExists bool   `json:"create_if_not_exists"`
}

// extractDestinationConfigFromConfigJSON pulls the destination_config object out
// of a pipeline's raw config JSONB, falling back to the legacy
// destination_namespace string for pipelines created before PR-C. Returns nil
// when neither is present (legacy pipelines run with source-derived defaults).
func extractDestinationConfigFromConfigJSON(configJSON []byte) *pipelineDestinationConfig {
	if len(configJSON) == 0 {
		return nil
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(configJSON, &cfg); err != nil {
		return nil
	}
	if raw, ok := cfg["destination_config"]; ok {
		dc := &pipelineDestinationConfig{}
		if err := json.Unmarshal(raw, dc); err == nil && strings.TrimSpace(dc.Namespace) != "" {
			return dc
		}
	}
	if raw, ok := cfg["destination_namespace"]; ok {
		var ns string
		if err := json.Unmarshal(raw, &ns); err == nil && strings.TrimSpace(ns) != "" {
			return &pipelineDestinationConfig{Namespace: ns}
		}
	}
	return nil
}

// assessDestinationNamespace builds the "(destination)" assessment table for the
// destination-mapping HITL. It always validates the name; for relational
// destinations it additionally probes existence + CREATE privilege live.
// Returns (table, true) when there is anything to report, (zero, false) when the
// pipeline has no destination mapping (legacy — nothing to assess).
func assessDestinationNamespace(ctx context.Context, database *sql.DB, workspaceID, destConnID, destType string, configJSON []byte) (AssessmentTable, bool) {
	dc := extractDestinationConfigFromConfigJSON(configJSON)
	if dc == nil {
		return AssessmentTable{}, false
	}
	namespace := strings.TrimSpace(dc.Namespace)
	kind := strings.TrimSpace(dc.NamespaceKind)
	if kind == "" {
		kind = namespaceKindForConnector(destType)
	}

	tbl := AssessmentTable{
		Name:   "(destination namespace)",
		Schema: namespace,
	}

	// 1. Name validity — applies to every destination type. A bad name is an
	//    always-blocking error (an article/stopword namespace silently
	//    misroutes data, the PR #130 failure mode).
	if reason := naming.ValidateNamespace(namespace); reason != "" {
		tbl.Findings = append(tbl.Findings, AssessmentFinding{
			Code:     FindingDestNamespaceInvalid,
			Severity: AssessmentError,
			Message:  fmt.Sprintf("Destination %s name is invalid: %s.", kind, reason),
			Details:  map[string]interface{}{"namespace": namespace, "namespace_kind": kind},
		})
		// No point probing an invalid name — the error already blocks the run.
		return tbl, true
	}

	// 2. Existence + privilege probe. Only relational destinations expose a
	//    cheap, reliable live probe; for everything else (object storage,
	//    warehouses we don't open directly) we trust the connector's lazy
	//    create and emit an informational note.
	if !isDBConnector(destType) {
		tbl.Findings = append(tbl.Findings, AssessmentFinding{
			Code:     FindingDestNamespaceExists,
			Severity: AssessmentInfo,
			Message:  fmt.Sprintf("Data will be written to %s %q on the destination.", kind, namespace),
			Details:  map[string]interface{}{"namespace": namespace, "namespace_kind": kind},
		})
		return tbl, true
	}

	exists, canCreate, probeErr := probeDestinationNamespace(ctx, database, workspaceID, destConnID, destType, namespace)
	tbl.Findings = append(tbl.Findings, destinationNamespaceFindings(namespace, kind, dc.CreateIfNotExists, exists, canCreate, probeErr)...)
	return tbl, true
}

// destinationNamespaceFindings is the pure decision matrix for a relational
// destination namespace given live-probe results. Kept separate from the DB
// probe so it is unit-testable without a database.
func destinationNamespaceFindings(namespace, kind string, createIfNotExists, exists, canCreate bool, probeErr error) []AssessmentFinding {
	base := map[string]interface{}{"namespace": namespace, "namespace_kind": kind}
	if probeErr != nil {
		// Probe infrastructure failure must NOT block a run — the destination
		// might simply be unreachable from the control plane right now, and the
		// run itself will surface a real connection error. Inform, don't block.
		return []AssessmentFinding{{
			Code:     FindingDestNamespaceUnverified,
			Severity: AssessmentInfo,
			Message:  fmt.Sprintf("Could not verify destination %s %q ahead of time (%s). It will be validated at write time.", kind, namespace, probeErr.Error()),
			Details:  base,
		}}
	}
	// A missing namespace is never a hard blocker: the destination connector
	// auto-creates a missing schema/database at write time (CREATE … IF NOT
	// EXISTS). We surface it as an acknowledgeable warning so the user knows a
	// new namespace will be provisioned, and tune the wording by whether we could
	// confirm CREATE privilege. (The privilege probe fails closed and under-reports
	// on managed databases whose grants aren't global, so we never block on it —
	// a genuine permission problem surfaces as a clear error at write time.)
	switch {
	case exists:
		return []AssessmentFinding{{
			Code:     FindingDestNamespaceExists,
			Severity: AssessmentInfo,
			// Honest label (P3-b): the chosen namespace already exists, so state both
			// what happens to tables already in it (they are written to in place) and
			// the one condition that still moves the pipeline elsewhere — another
			// rsync pipeline already writing one of the SAME tables there. Otherwise
			// the user looks for their data in a schema it was never written to.
			Message: fmt.Sprintf("Destination %s %q already exists — tables are created there, and a selected table already present is written to in place. Only if another rsync pipeline already writes one of the selected tables into %q is this pipeline moved to a collision-safe %s instead (%q, or %q if that is taken too); you are notified when that happens.",
				kind, namespace, namespace, kind, "rsync_"+namespace, "rsync_"+namespace+"_<id>"),
			Details: mergeDetails(base, map[string]interface{}{"exists": true, "collision_safe_prefix": "rsync_" + namespace}),
		}}
	case canCreate:
		return []AssessmentFinding{{
			Code:     FindingDestNamespaceWillCreate,
			Severity: AssessmentWarning,
			Message:  fmt.Sprintf("Destination %s %q does not exist yet and will be created automatically on this run.", kind, namespace),
			Details:  mergeDetails(base, map[string]interface{}{"exists": false, "will_create": true}),
		}}
	default: // missing; CREATE privilege could not be confirmed
		return []AssessmentFinding{{
			Code:     FindingDestNamespaceNoPrivilege,
			Severity: AssessmentWarning,
			Message:  fmt.Sprintf("Destination %s %q does not exist; it will be created automatically on this run. If the connection user lacks privilege to create it, the run will fail with a permission error — grant CREATE or create the %s manually.", kind, namespace, kind),
			Details:  mergeDetails(base, map[string]interface{}{"exists": false, "will_create": true, "can_create": false}),
		}}
	}
}

func mergeDetails(a, b map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// probeDestinationNamespace opens the relational destination connection and
// reports whether the namespace exists and whether the connection user may
// create it. Read-only: existence is an information_schema lookup and the
// privilege check is a catalog/grants query — it never mutates the destination.
func probeDestinationNamespace(ctx context.Context, database *sql.DB, workspaceID, destConnID, destType, namespace string) (exists bool, canCreate bool, err error) {
	var ct string
	cfg, err := decryptedConnectionConfig(database, workspaceID, destConnID, &ct)
	if err != nil {
		return false, false, fmt.Errorf("load destination connection: %w", err)
	}

	driverName, dsn, err := relationalDSN(destType, cfg)
	if err != nil {
		return false, false, err
	}

	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return false, false, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxLifetime(10 * time.Second)

	pingCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := conn.PingContext(pingCtx); err != nil {
		return false, false, fmt.Errorf("ping: %w", err)
	}

	switch driverName {
	case "postgres":
		// Existence: a schema row in the current database.
		if err := conn.QueryRowContext(pingCtx,
			`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
			namespace,
		).Scan(&exists); err != nil {
			return false, false, fmt.Errorf("schema existence query: %w", err)
		}
		// Privilege: CREATE on the current database lets the user CREATE SCHEMA.
		if err := conn.QueryRowContext(pingCtx,
			`SELECT has_database_privilege(current_database(), 'CREATE')`,
		).Scan(&canCreate); err != nil {
			// Non-fatal: treat an unreadable privilege as "unknown / cannot
			// confirm" → canCreate=false (fail-closed for the create path).
			canCreate = false
		}
		return exists, canCreate, nil

	case "mysql":
		// Existence: a database (schema) of this name.
		if err := conn.QueryRowContext(pingCtx,
			`SELECT EXISTS(SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?)`,
			namespace,
		).Scan(&exists); err != nil {
			return false, false, fmt.Errorf("database existence query: %w", err)
		}
		canCreate = mysqlUserCanCreateDatabase(pingCtx, conn)
		return exists, canCreate, nil

	default:
		return false, false, fmt.Errorf("namespace probe unsupported for %q", destType)
	}
}

// destTableBareName extracts the bare destination table name from a (possibly
// source-qualified) selected-table spec. The executor writes each source table
// to a destination table named after its last path segment (e.g. source
// "shopdb.products" → dest "<namespace>.products"), so collision detection must
// compare on that same bare name. Quotes/backticks are stripped.
func destTableBareName(sel string) string {
	s := strings.TrimSpace(sel)
	s = strings.Trim(s, "`\"")
	if i := strings.LastIndex(s, "."); i >= 0 {
		s = s[i+1:]
	}
	return strings.Trim(strings.TrimSpace(s), "`\"")
}

// destTableProbeSet builds the set of destination table names to look for in a
// candidate namespace.
//
// The executor names each destination table after the source table's last path
// segment, so the bare source names are the right probe set for a multi-table
// run. A SINGLE-table run is the exception: it can be redirected to a
// differently-named destination table, and the only redirect visible from here
// at lock time is the destination connection's `table` setting — the NL
// "…into table X" override is inferred later, in the executor. Include that name
// as well rather than pick between them: a wider probe set can only find more
// pre-existing tables, never miss one, and the ownership gate in
// namespaceProbe.isCollision keeps the extra name from causing a spurious
// relocation on its own.
func destTableProbeSet(selectedTables []string, destCfg map[string]interface{}) map[string]struct{} {
	want := make(map[string]struct{}, len(selectedTables)+1)
	for _, t := range selectedTables {
		if b := destTableBareName(t); b != "" {
			want[b] = struct{}{}
		}
	}
	// Multi-table runs route per source table; the executor deletes the
	// connection-level `table` hint outright, so honouring it here would invent a
	// table this pipeline never writes.
	if len(want) != 1 {
		return want
	}
	if v, ok := destCfg["table"].(string); ok {
		if b := destTableBareName(v); b != "" {
			want[b] = struct{}{}
		}
	}
	return want
}

// namespaceProbe is what one candidate namespace looks like to a pipeline about
// to lock it: which of this pipeline's destination tables already live there, and
// whether a DIFFERENT pipeline on the same destination connection writes any of
// those same tables into it.
type namespaceProbe struct {
	CollidingTables []string
	OwnerPipelineID string
}

// isCollision reports whether the namespace belongs to somebody else and has to
// be routed around. BOTH signals are required, and that conjunction is the whole
// of the KI-NSLOCK-SILENT-RELOCATION fix.
//
// "A table of this name already exists" is, on its own, the ORDINARY case — a
// user pointing a pipeline at a table they already have. Treating it as a
// collision moved every such pipeline into rsync_<namespace> and left the
// namespace the user configured empty; on prod one pipeline was relocated that
// way on 14 consecutive runs without a single user-visible signal. A namespace
// is somebody else's only when a *different* pipeline actually writes that same
// table into it.
func (p namespaceProbe) isCollision() bool {
	return len(p.CollidingTables) > 0 && strings.TrimSpace(p.OwnerPipelineID) != ""
}

// probeNamespaceCollision reports whether the candidate namespace already holds
// tables this pipeline would write AND those tables belong to another pipeline.
//
// Read-only: information_schema on the destination for what exists, the control
// plane for who owns it. Returns a non-nil error only on infrastructure failure
// (unreachable / bad creds) — the caller treats that as "cannot verify" and
// proceeds with the user's chosen namespace rather than blocking.
func probeNamespaceCollision(ctx context.Context, database *sql.DB, workspaceID, destConnID, destType, namespace, pipelineID string, selectedTables []string) (namespaceProbe, error) {
	var out namespaceProbe
	namespace = strings.TrimSpace(namespace)
	if namespace == "" || len(selectedTables) == 0 || !isDBConnector(destType) {
		return out, nil
	}

	var ct string
	cfg, err := decryptedConnectionConfig(database, workspaceID, destConnID, &ct)
	if err != nil {
		return out, fmt.Errorf("load destination connection: %w", err)
	}
	driverName, dsn, err := relationalDSN(destType, cfg)
	if err != nil {
		return out, err
	}
	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return out, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxLifetime(10 * time.Second)

	qCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := conn.PingContext(qCtx); err != nil {
		return out, fmt.Errorf("ping: %w", err)
	}

	want := destTableProbeSet(selectedTables, cfg)
	if len(want) == 0 {
		return out, nil
	}

	found := make([]string, 0, len(want))
	switch driverName {
	case "postgres":
		// information_schema.tables.table_name is case-sensitive; the connector
		// creates tables with their bare names verbatim, so an exact match is
		// correct here.
		rows, qErr := conn.QueryContext(qCtx,
			`SELECT table_name FROM information_schema.tables WHERE table_schema = $1`,
			namespace,
		)
		if qErr != nil {
			return out, fmt.Errorf("table existence query: %w", qErr)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return out, err
			}
			if _, ok := want[name]; ok {
				found = append(found, name)
			}
		}
	case "mysql":
		rows, qErr := conn.QueryContext(qCtx,
			`SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?`,
			namespace,
		)
		if qErr != nil {
			return out, fmt.Errorf("table existence query: %w", qErr)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return out, err
			}
			if _, ok := want[name]; ok {
				found = append(found, name)
			}
		}
	default:
		return out, fmt.Errorf("table collision probe unsupported for %q", destType)
	}
	out.CollidingTables = found
	if len(found) == 0 {
		// Nothing to route around, so ownership cannot change the outcome — skip
		// the extra round-trip.
		return out, nil
	}

	owner, err := namespaceTableOwner(qCtx, database, workspaceID, destConnID, pipelineID, namespace, cfg, want)
	if err != nil {
		return out, fmt.Errorf("ownership lookup: %w", err)
	}
	out.OwnerPipelineID = owner
	return out, nil
}

// namespaceTableOwner returns the id of a DIFFERENT pipeline that writes one of
// `want` into `namespace` on this same destination connection, or "" when nobody
// does — in which case the tables found there are the user's own and this
// pipeline syncs into them in place.
//
// Ownership is answered from the CONTROL PLANE, not from the destination's
// `_rsync_pipelines` ledger, and the reason is granularity. That ledger is keyed
// (pipeline_id, namespace), so the first pipeline to write anything into `public`
// claims the whole schema — which would relocate every later pipeline out of the
// default schema no matter which tables it writes. Measured against prod: 22
// pipelines share one destination connection, 11 of them target `public`, and the
// pipeline this KI was raised for writes `demo_customers`, a table NO other
// pipeline on that connection writes. Namespace-granular ownership keeps it
// relocated; table-granular ownership lets it lock `public`, while still
// reproducing every relocation that is genuinely warranted (`0f023bf3` writes
// `customers`, which `a9d7f773` already writes into `public`, and still moves to
// `rsync_public`). The ledger also under-populates (BACKLOG OWN-EmptyAfterRun),
// so a gate built on it is largely inert; `pipelines.config` is authoritative and
// complete.
//
// The other pipeline's destination tables are derived exactly as this pipeline's
// are — destTableProbeSet over its own selected_tables, with the SAME connection
// config, which is sound because the query already restricts to pipelines sharing
// this destination connection. A pipeline whose selected_tables were never
// recorded claims nothing: with no way to know what it writes, "the table is the
// user's own" is the fail-open this KI is about.
func namespaceTableOwner(ctx context.Context, database *sql.DB, workspaceID, destConnID, pipelineID, namespace string, destCfg map[string]interface{}, want map[string]struct{}) (string, error) {
	if database == nil || len(want) == 0 {
		return "", nil
	}
	pipelineID = strings.TrimSpace(pipelineID)

	// ::text on both sides rather than a ::uuid cast on the parameter: a malformed
	// id must read as "matches nothing", not raise a runtime SQL error that fails
	// the probe and (per the caller's fail-soft) silently skips collision checks.
	rows, err := database.QueryContext(ctx, `
		SELECT p.id::text, COALESCE(p.config->'selected_tables', '[]'::jsonb)::text
		FROM pipelines p
		WHERE p.id::text <> $1
		  AND p.workspace_id::text = $2
		  AND p.destination_connection_id::text = $3
		  AND COALESCE(
		        NULLIF(p.config->'destination_config'->>'namespace', ''),
		        p.config->>'destination_namespace',
		        '') = $4
		ORDER BY p.id
	`, pipelineID, workspaceID, destConnID, namespace)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var otherID, rawTables string
		if err := rows.Scan(&otherID, &rawTables); err != nil {
			return "", err
		}
		var selected []string
		if err := json.Unmarshal([]byte(rawTables), &selected); err != nil {
			// A selected_tables value that is not an array of strings tells us
			// nothing about what that pipeline writes. Skip it rather than fail the
			// whole probe.
			continue
		}
		for name := range destTableProbeSet(selected, destCfg) {
			if _, ok := want[name]; ok {
				return otherID, nil
			}
		}
	}
	return "", rows.Err()
}

// namespaceRelocation records that a pipeline was moved off the namespace it was
// configured with, so the caller can tell the user. Before this existed the move
// was recorded at Info level and nowhere else, which is how 14 consecutive prod
// runs of one pipeline wrote to a schema nobody was looking at.
type namespaceRelocation struct {
	Chosen          string
	Resolved        string
	CollidingTables []string
	OwnerPipelineID string
}

// resolveFirstRunNamespace picks the destination namespace a pipeline will OWN,
// routing around namespaces that belong to a DIFFERENT pipeline:
//
//   - chosen namespace is not another pipeline's → use it as-is, even when the
//     target tables already exist there (that is the ordinary "sync into my
//     existing table" case, and it is what the user asked for).
//   - chosen namespace already holds one of this pipeline's tables AND another
//     pipeline on the same destination connection writes that same table there
//     → use "rsync_<namespace>".
//   - "rsync_<namespace>" is ALSO another pipeline's by the same test → use
//     "rsync_<namespace>_<pipelineID8>" (guaranteed unique per pipeline).
//
// This runs ONCE, on first run, and the result is locked into
// pipelines.config.destination_namespace so reloads / incremental / scheduled
// runs reuse the same namespace + tables without ever re-prompting or
// re-prefixing. Mirrors Airbyte's stream-prefix isolation. Engine-agnostic:
// works identically for a PG schema and a MySQL database.
//
// Fail-soft: a probe infrastructure error means "cannot verify collision" — we
// return the user's chosen namespace unchanged rather than block the run (a real
// collision then surfaces, loudly, at write time, via the connector's ownership
// gate). A non-nil second return means the pipeline was relocated and the user
// needs to be told where its data actually went.
func resolveFirstRunNamespace(ctx context.Context, database *sql.DB, workspaceID, destConnID, destType, pipelineID, chosen string, selectedTables []string) (string, *namespaceRelocation) {
	chosen = strings.TrimSpace(chosen)
	if chosen == "" || !isDBConnector(destType) {
		return chosen, nil
	}

	probe, err := probeNamespaceCollision(ctx, database, workspaceID, destConnID, destType, chosen, pipelineID, selectedTables)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithFields(map[string]interface{}{
			"pipeline_id": pipelineID,
			"namespace":   chosen,
		}).Warn("namespace resolution: collision probe failed; using chosen namespace unverified")
		return chosen, nil
	}
	if !probe.isCollision() {
		log.WithContext(ctx).WithFields(map[string]interface{}{
			"pipeline_id":     pipelineID,
			"namespace":       chosen,
			"existing_tables": probe.CollidingTables,
		}).Info("namespace resolution: chosen namespace is not owned by another pipeline; locking as-is")
		return chosen, nil
	}

	// Another pipeline owns the chosen namespace and its tables → try the
	// rsync_ prefix.
	prefixed := "rsync_" + chosen
	if reason := naming.ValidateNamespace(prefixed); reason != "" {
		// Prefixed name is invalid (e.g. length) — fall straight to the
		// id-suffixed form which we also validate below.
		prefixed = ""
	}
	if prefixed != "" {
		probe2, err2 := probeNamespaceCollision(ctx, database, workspaceID, destConnID, destType, prefixed, pipelineID, selectedTables)
		if err2 != nil {
			log.WithContext(ctx).WithError(err2).WithFields(map[string]interface{}{
				"pipeline_id": pipelineID,
				"namespace":   prefixed,
			}).Warn("namespace resolution: rsync_ prefix probe failed; using prefix unverified")
			return prefixed, &namespaceRelocation{
				Chosen: chosen, Resolved: prefixed,
				CollidingTables: probe.CollidingTables, OwnerPipelineID: probe.OwnerPipelineID,
			}
		}
		if !probe2.isCollision() {
			rel := &namespaceRelocation{
				Chosen: chosen, Resolved: prefixed,
				CollidingTables: probe.CollidingTables, OwnerPipelineID: probe.OwnerPipelineID,
			}
			log.WithContext(ctx).WithFields(map[string]interface{}{
				"pipeline_id":       pipelineID,
				"chosen":            chosen,
				"resolved":          prefixed,
				"colliding_tables":  probe.CollidingTables,
				"owner_pipeline_id": probe.OwnerPipelineID,
			}).Warn("namespace resolution: chosen namespace belongs to another pipeline; relocated to rsync_ prefix")
			return prefixed, rel
		}
	}

	// Both chosen and rsync_<chosen> are another pipeline's → suffix with the
	// pipeline id8, which is unique to this pipeline so it cannot collide.
	id8 := strings.ReplaceAll(strings.TrimSpace(pipelineID), "-", "")
	if len(id8) > 8 {
		id8 = id8[:8]
	}
	resolved := "rsync_" + chosen + "_" + id8
	log.WithContext(ctx).WithFields(map[string]interface{}{
		"pipeline_id":       pipelineID,
		"chosen":            chosen,
		"resolved":          resolved,
		"colliding_tables":  probe.CollidingTables,
		"owner_pipeline_id": probe.OwnerPipelineID,
	}).Warn("namespace resolution: chosen namespace and rsync_ prefix both belong to other pipelines; relocated to id-suffixed namespace")
	return resolved, &namespaceRelocation{
		Chosen: chosen, Resolved: resolved,
		CollidingTables: probe.CollidingTables, OwnerPipelineID: probe.OwnerPipelineID,
	}
}

// notifyNamespaceRelocation records a user-visible notification that a pipeline
// was moved off the namespace it was configured with.
//
// A relocation is silent data displacement from the user's point of view: they
// look in the schema they picked and find it empty. An Info log is not a
// user-visible signal, so the move also lands in the notification inbox the
// healer/sentinel already use.
//
// Best-effort throughout: this runs on the HITL resume path, and failing to
// record a notification must never strand a pipeline in its park.
func notifyNamespaceRelocation(ctx context.Context, database *sql.DB, pipelineID string, rel *namespaceRelocation) {
	if database == nil || rel == nil {
		return
	}
	var userID sql.NullString
	if err := database.QueryRowContext(ctx,
		`SELECT created_by::text FROM pipelines WHERE id = $1::uuid`, pipelineID,
	).Scan(&userID); err != nil || strings.TrimSpace(userID.String) == "" {
		log.WithContext(ctx).WithField("pipeline_id", pipelineID).
			Warn("namespace relocation: no owning user; notification skipped")
		return
	}

	// `action_label` MUST stay non-empty. ListNotifications runs every row through
	// repairPreCatalogCopy (notifications.go:200), which treats an empty
	// action_label as "pre-catalog row" and OVERWRITES title/impact/severity with
	// notifier.RenderStored copy keyed by type — and this type is not in the
	// catalog, so the message written here would be replaced by fallback copy.
	// A non-empty action_label makes the repair return early and the copy survive.
	meta, _ := json.Marshal(map[string]interface{}{
		"chosen_namespace":   rel.Chosen,
		"resolved_namespace": rel.Resolved,
		"colliding_tables":   rel.CollidingTables,
		"owner_pipeline_id":  rel.OwnerPipelineID,
		"impact":             fmt.Sprintf("Rows land in %q, not %q.", rel.Resolved, rel.Chosen),
		"action_label":       "View pipeline",
	})
	message := fmt.Sprintf(
		"This pipeline was set up to write to %q, but %s already written there by pipeline %s. To avoid overwriting its data, this pipeline writes to %q instead — look for your data there.",
		rel.Chosen, describeCollidingTables(rel.CollidingTables), rel.OwnerPipelineID, rel.Resolved,
	)
	if _, err := database.ExecContext(ctx, `
		INSERT INTO pipeline_notifications
			(pipeline_id, user_id, type, severity, title, message, action_url, metadata, delivery_status, dedup_key, created_at)
		VALUES ($1::uuid, $2::uuid, 'destination_namespace_relocated', 'warning', $3, $4, $5, $6, 'pending', $7, NOW())
	`,
		pipelineID, strings.TrimSpace(userID.String),
		"Destination changed to avoid another pipeline's data",
		message,
		"/pipelines/"+pipelineID,
		meta,
		"ns-relocation:"+pipelineID+":"+rel.Resolved,
	); err != nil {
		log.WithContext(ctx).WithError(err).WithField("pipeline_id", pipelineID).
			Warn("namespace relocation: failed to record notification (ignored)")
	}
}

// clearRelocatedCheckpoints drops the pipeline's resume checkpoints when the lock
// has just moved it to a different namespace, and reports how many it removed.
//
// pipeline_checkpoints is keyed by (pipeline_id, source_table) and records NOTHING
// about the destination namespace, so a checkpoint written while the pipeline wrote
// to `public` reads as valid after the lock relocates it to `rsync_public_<id8>` —
// a namespace that does not exist yet and holds no rows. The next run resumes from
// `cursor 2000 / table_complete true`, transfers 0 rows, and reports success, while
// the relocation notification points the user at an empty (in fact uncreated) schema.
// Prod pipeline 12c3579c did exactly that: relocated, locked, notified, moved 0 rows
// (KI-NSLOCK-RELOCATION-STRANDS-CHECKPOINT).
//
// Relocation means the destination is empty by construction, so every resume
// position for this pipeline is stale — the same state run_mode=reload clears, and
// for the same reason ("instant success" no-op runs).
//
// Deliberately here rather than in the executor: `relocated` is observable EXACTLY
// ONCE, because the lock is one-way and every later call short-circuits on it. A
// clear that lives a network hop away from the write that made it observable can be
// lost for good; next to it, both callers (ResumeTables and the run boundary) get it
// from one code path.
//
// Fail-soft like the rest of this path: a failed delete is logged, never fatal. The
// worst case is the pre-fix behaviour.
func clearRelocatedCheckpoints(ctx context.Context, database *sql.DB, pipelineID string, rel *namespaceRelocation) int64 {
	if database == nil || rel == nil {
		return 0
	}
	res, err := database.ExecContext(ctx,
		`DELETE FROM pipeline_checkpoints WHERE pipeline_id = $1::uuid`, pipelineID)
	if err != nil {
		log.WithContext(ctx).WithError(err).WithFields(map[string]interface{}{
			"pipeline_id": pipelineID,
			"resolved":    rel.Resolved,
		}).Warn("namespace relocation: failed to clear stale checkpoints; the new namespace may stay empty (ignored)")
		return 0
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.WithContext(ctx).WithFields(map[string]interface{}{
			"pipeline_id":         pipelineID,
			"from":                rel.Chosen,
			"to":                  rel.Resolved,
			"checkpoints_cleared": n,
		}).Info("namespace relocation: cleared resume checkpoints written against the old namespace")
	}
	return n
}

// describeCollidingTables renders the colliding table list for user-facing copy,
// with agreement that reads correctly for one table and for many.
func describeCollidingTables(tables []string) string {
	switch len(tables) {
	case 0:
		return "its tables are"
	case 1:
		return fmt.Sprintf("table %q is", tables[0])
	default:
		quoted := make([]string, 0, len(tables))
		for _, t := range tables {
			quoted = append(quoted, fmt.Sprintf("%q", t))
		}
		return "tables " + strings.Join(quoted, ", ") + " are"
	}
}

// mysqlUserCanCreateDatabase inspects SHOW GRANTS for the current user and
// reports whether it includes a global CREATE (or ALL PRIVILEGES) grant, which
// is what CREATE DATABASE requires. Fails closed (false) on any uncertainty.
func mysqlUserCanCreateDatabase(ctx context.Context, conn *sql.DB) bool {
	rows, err := conn.QueryContext(ctx, "SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return false
		}
		up := strings.ToUpper(grant)
		// A global grant applies to all databases: "... ON *.* TO ...".
		if !strings.Contains(up, "ON *.*") {
			continue
		}
		if strings.Contains(up, "ALL PRIVILEGES") || containsPrivilege(up, "CREATE") {
			return true
		}
	}
	return false
}

// containsPrivilege reports whether a GRANT statement's privilege list contains
// priv as a distinct token (so "CREATE" matches but "CREATE TEMPORARY TABLES"
// or "CREATE VIEW" — which do NOT permit CREATE DATABASE — do not).
func containsPrivilege(grantUpper, priv string) bool {
	idx := strings.Index(grantUpper, " ON ")
	if idx < 0 {
		return false
	}
	privList := grantUpper[:idx] // "GRANT CREATE, SELECT" portion
	for _, part := range strings.Split(privList, ",") {
		tok := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(part), "GRANT "))
		if tok == priv {
			return true
		}
	}
	return false
}

// relationalDSN builds a database/sql driver name + DSN for a relational
// destination connection config, mirroring sampleDataFromConnection's logic so
// TLS handling matches the rest of api-gateway (managed MySQL/PG over TLS).
func relationalDSN(connectorType string, config map[string]interface{}) (driverName, dsn string, err error) {
	host, _ := config["host"].(string)
	user, _ := config["user"].(string)
	password, _ := config["password"].(string)
	dbName, _ := config["database"].(string)

	switch strings.ToLower(strings.TrimSpace(connectorType)) {
	case "mysql", "mariadb":
		portInt := configPortInt(config["port"], 3306)
		tlsMode := resolveMySQLTLSMode(config, host)
		return "mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&timeout=8s&tls=%s",
			user, password, host, portInt, dbName, tlsMode), nil
	case "postgresql", "postgres", "pg":
		portInt := configPortInt(config["port"], 5432)
		sslMode := resolvePostgresSSLMode(config, host)
		return "postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=8",
			host, portInt, user, password, dbName, sslMode), nil
	default:
		return "", "", fmt.Errorf("no relational DSN for connector %q", connectorType)
	}
}

// configPortInt coerces a JSON-decoded port (float64/int/string) to an int,
// falling back to def when absent or unparseable.
func configPortInt(v interface{}, def int) int {
	switch p := v.(type) {
	case float64:
		return int(p)
	case int:
		return p
	case string:
		n := def
		fmt.Sscanf(p, "%d", &n)
		return n
	}
	return def
}
