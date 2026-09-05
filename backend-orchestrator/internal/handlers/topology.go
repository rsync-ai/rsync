package handlers

import (
	"database/sql"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// TopologyHandler handles Kafka topology management requests
// This provides API endpoints for plan-time topic provisioning.
//
// db is the tenancy oracle: the namespace guard below answers "is this OUR
// topic", and only the database can answer "is this THIS CALLER's topic".
type TopologyHandler struct {
	manager *kafka.TopologyManager
	db      *sql.DB
}

// NewTopologyHandler creates a new topology handler
func NewTopologyHandler(manager *kafka.TopologyManager, db *sql.DB) *TopologyHandler {
	return &TopologyHandler{manager: manager, db: db}
}

// CreateTopicRequest is the request body for creating a topic
type CreateTopicRequest struct {
	TopicName         string            `json:"topic_name" binding:"required"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replication_factor"`
	Config            map[string]string `json:"config,omitempty"`
}

// CreateTopicForPipelineRequest is the request for pipeline-based topic creation
type CreateTopicForPipelineRequest struct {
	PipelineID      string  `json:"pipeline_id" binding:"required"`
	SyncMode        string  `json:"sync_mode" binding:"required"`
	TableCount      int     `json:"table_count"`
	EstimatedSizeGB float64 `json:"estimated_size_gb"`
}

// TopicResponse is the response for topic operations
type TopicResponse struct {
	Success           bool              `json:"success"`
	TopicName         string            `json:"topic_name,omitempty"`
	Partitions        int               `json:"partitions,omitempty"`
	ReplicationFactor int               `json:"replication_factor,omitempty"`
	Config            map[string]string `json:"config,omitempty"`
	Error             string            `json:"error,omitempty"`
	CreatedAt         *time.Time        `json:"created_at,omitempty"`
}

// Topic-namespace confinement.
//
// These handlers act on whichever broker the deployment points at. Against the
// bundled single-tenant Kafka an arbitrary name is merely untidy; against a
// customer's shared cluster (BYO-Kafka — the managed-Kubernetes deployment
// shape) an unconfined DELETE /topics/:name reaches that customer's *other*
// applications' topics. Confinement keeps a buggy or hostile caller inside
// rsync-ai's own namespace even when it holds a valid principal.
//
// Since f1ee815e every topic this platform mints is routed through
// kafkaclient.Topic(), so it carries the deployment's KAFKA_TOPIC_PREFIX
// ("rsync." by default) on the wire:
//
//	rsync.agent.…                control-plane topics    kafka/topology.go CreateStandardTopics
//	rsync.pipeline.<id8>.data    batch data + .dlq       kafka/topology.go generateTopicName
//	rsync.cdc.<id8>              provisioned CDC topic   kafka/topology.go generateTopicName
//	rsync.cdc-<id8>[.db.table]   Debezium topic.prefix   executor.go
//	rsync.schemahistory.cdc-…    Debezium schema history debezium connector.py
//	rsync.signals.<id8>          incremental signals     executor.go:2916
//	rsync.pii.…                  PII scanner req/resp    llm-service pii_scanner
//	rsync.task.…                 task lifecycle events   api-gateway
//
// The configured prefix is therefore the whole allowlist a current deployment
// needs, and it is the only entry that is actually safe on a shared cluster.

// envOwnedTopicPrefixes lets a deployment that names topics differently extend
// the set (comma-separated). It adds to the built-ins and never replaces them,
// so widening the allowlist can't accidentally strand the platform's own topics
// outside it.
const envOwnedTopicPrefixes = "KAFKA_OWNED_TOPIC_PREFIXES"

// envAllowLegacyUnprefixedTopics re-admits the pre-namespace bare names below.
// See legacyBareTopicPrefixes for why it is off by default and what it widens.
const envAllowLegacyUnprefixedTopics = "KAFKA_ALLOW_LEGACY_UNPREFIXED_TOPICS"

// brandedTopicPrefixes are owned unconditionally: they spell the product's own
// name, so no other team's topic can land on them whatever prefix is
// configured. "_rsync-connect-offsets" (executor/hybrid_cdc.go:128) is the one
// that exists today.
var brandedTopicPrefixes = []string{
	"_rsync-",
}

// legacyBareTopicPrefixes are the PRE-NAMESPACE names, and they are NOT in the
// allowlist by default any more.
//
// They are dangerous on exactly the deployment shape this branch targets:
// kafkaclient's own doc comment (shared/go/kafkaclient/topics.go:21) says of
// these very names that they are generic enough "to collide outright with
// another team's topics". Left in unconditionally, a BYO customer's own `task.`
// or `pipeline.` topic sits inside the delete blast radius of any authenticated
// rsync-ai caller — for a verb that Kafka cannot undo.
//
// They are honored in exactly two cases, both deliberate and both narrow:
//
//  1. The deployment runs with an EMPTY KAFKA_TOPIC_PREFIX (the documented
//     opt-out of namespacing). Then these bare names *are* the platform's own
//     names and dropping them would strand the platform outside its own
//     allowlist — it could no longer repartition or clean up its own data.
//  2. The operator sets KAFKA_ALLOW_LEGACY_UNPREFIXED_TOPICS=true — the
//     migration window for a deployment that has adopted the prefix but still
//     has pre-namespace topics to reclaim. It logs on first use, naming the
//     variable and the risk, so the widening is never silent.
//
// Nothing else needs them. In particular the platform's own reclamation path
// (POST /cdc/kafka-teardown → cleanupPipelineKafkaResources, cdc_kafka_teardown.go)
// talks to the TopologyManager directly and never passes through this
// allowlist, so narrowing it here cannot strand a teardown.
var legacyBareTopicPrefixes = []string{
	"agent.",
	"pipeline.",
	"cdc.",
	"cdc-",
	"schemahistory.",
	"pii.",
	"task.",
}

// legacyPrefixWarnOnce keeps the opt-out warning to one line per process rather
// than one per request.
var legacyPrefixWarnOnce sync.Once

// legacyBarePrefixesAllowed reports whether legacyBareTopicPrefixes are part of
// this deployment's allowlist. See that variable for the two qualifying cases.
func legacyBarePrefixesAllowed() bool {
	if kafkaclient.TopicPrefix() == "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envAllowLegacyUnprefixedTopics))) {
	case "1", "true", "yes", "on":
		legacyPrefixWarnOnce.Do(func() {
			log.Warnf("⚠️  %s is set: the topology API allowlist is widened to the "+
				"un-namespaced prefixes %s. On a shared/BYO Kafka cluster another team's "+
				"topics under those names become deletable through this API. Unset it once "+
				"the pre-namespace topics have been reclaimed.",
				envAllowLegacyUnprefixedTopics, strings.Join(legacyBareTopicPrefixes, " "))
		})
		return true
	}
	return false
}

// allowedTopicPrefixes is the effective allowlist for this deployment. It is
// the single source for both the match and the 403 message, so the error can
// never advertise a prefix the guard does not actually accept.
func allowedTopicPrefixes() []string {
	out := make([]string, 0, len(brandedTopicPrefixes)+len(legacyBareTopicPrefixes)+2)
	if p := kafkaclient.TopicPrefix(); p != "" {
		out = append(out, p)
	}
	out = append(out, brandedTopicPrefixes...)
	if legacyBarePrefixesAllowed() {
		out = append(out, legacyBareTopicPrefixes...)
	}
	for _, p := range strings.Split(os.Getenv(envOwnedTopicPrefixes), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// isPlatformOwnedTopic reports whether name sits inside a namespace rsync-ai owns.
func isPlatformOwnedTopic(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, p := range allowedTopicPrefixes() {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// rejectForeignTopic writes 403 and aborts when name is outside the platform
// namespace. Returns true when the request was rejected, in which case the
// calling handler must return immediately.
func rejectForeignTopic(c *gin.Context, name string) bool {
	if isPlatformOwnedTopic(name) {
		return false
	}
	log.Warnf("🚫 topology: refusing %s on topic outside the rsync-ai namespace: %q",
		c.Request.Method, name)
	c.JSON(http.StatusForbidden, gin.H{
		"success": false,
		"error": "topic is outside the rsync-ai namespace (allowed prefixes: " +
			strings.Join(allowedTopicPrefixes(), ", ") + ")",
	})
	c.Abort()
	return true
}

// ── Tenant scoping ────────────────────────────────────────────────────────
//
// The namespace guard above answers "is this OUR topic". It does not answer
// "is this THIS CALLER's topic", and requirePrincipal — the middleware that
// gates this group in cmd/orchestrator/main.go — only authenticates. Between
// them, any user holding a valid session in ANY workspace could DELETE or
// repartition another tenant's rsync.pipeline.<id8>.data / rsync.cdc.<id8>.
// That is not recoverable: Kafka topic deletion is irreversible, and
// rsync.cdc.<id8> is created cleanup.policy=compact retention.ms=-1 — the
// durable store of that pipeline's change stream.
//
// The join key is the 8-char pipeline id every producer embeds in the name (see
// kafka/topology.go generateTopicName and executor.go), so the gates below
// resolve it back to pipelines.id and reuse the workspace-role policy the CDC
// routes already run — decideResourceAccess in cdc_authz.go, same ladder, same
// legacy-row fallback, same internal-caller passthrough.

// pipelineID8Pattern is exactly what utils.SafeID8 emits: the first 8 chars of
// a UUID as Postgres renders it.
var pipelineID8Pattern = regexp.MustCompile(`^[0-9a-fA-F]{8}$`)

// topicPipelineID8 returns the pipeline id8 a topic name is scoped to.
//
// ok=false means the name is not pipeline-scoped: a shared control topic
// (agent./pii./task.), a Kafka Connect internal (_rsync-connect-offsets), or
// the planner's per-CONNECTION CDC topic ("cdc.<connection-name>",
// llm-service/src/agents/planner/strategies.py:650), which carries no pipeline
// id at all. Those are platform infrastructure rather than one tenant's data,
// so the gates below refuse them for user principals instead of guessing an
// owner.
func topicPipelineID8(name string) (string, bool) {
	s := strings.TrimSpace(name)
	if p := kafkaclient.TopicPrefix(); p != "" {
		s = strings.TrimPrefix(s, p)
	}
	// Debezium's schema-history topic wraps the connector name, which is itself
	// "cdc-<id8>", so unwrap it before the switch rather than duplicating a case.
	s = strings.TrimPrefix(s, "schemahistory.")

	var rest string
	switch {
	case strings.HasPrefix(s, "pipeline."):
		rest = s[len("pipeline."):]
	case strings.HasPrefix(s, "signals."):
		rest = s[len("signals."):]
	case strings.HasPrefix(s, "cdc."):
		rest = s[len("cdc."):]
	case strings.HasPrefix(s, "cdc-"):
		rest = s[len("cdc-"):]
	default:
		return "", false
	}
	// The id8 runs to the next separator: pipeline.<id8>.data,
	// cdc-<id8>.<db>.<table>, or cdc.<id8> with no separator at all.
	if i := strings.IndexByte(rest, '.'); i >= 0 {
		rest = rest[:i]
	}
	if !pipelineID8Pattern.MatchString(rest) {
		return "", false
	}
	// Lower-cased because every producer derives the id from pipelines.id::text,
	// which Postgres renders lower-case (same normalization as
	// cdc_kafka_teardown.go:164).
	return strings.ToLower(rest), true
}

// topicPipelineAccessQuery resolves the id8 embedded in a topic name back to
// every pipeline that could have produced it, plus the caller's role in each
// one's workspace. Keyed on LEFT(id::text, 8) because that is exactly what
// SafeID8 / generateTopicName put into the name.
const topicPipelineAccessQuery = `
	SELECT p.workspace_id::text, p.created_by::text, wm.role
	  FROM pipelines p
	  LEFT JOIN workspace_members wm
	    ON wm.workspace_id = p.workspace_id AND wm.user_id::text = $2
	 WHERE LEFT(p.id::text, 8) = $1`

// readableTopicID8Query is the caller's whole readable set in one round trip,
// for the list route. The floor is any workspace membership — viewer included,
// since listing is not mutating — plus the legacy pre-workspaces creator
// fallback that decideResourceAccess applies to a NULL workspace_id.
const readableTopicID8Query = `
	SELECT DISTINCT LEFT(p.id::text, 8)
	  FROM pipelines p
	  LEFT JOIN workspace_members wm
	    ON wm.workspace_id = p.workspace_id AND wm.user_id::text = $1
	 WHERE wm.user_id IS NOT NULL
	    OR (p.workspace_id IS NULL AND p.created_by::text = $1)`

// loadTopicPipelineAccess returns one resourceAccess per pipeline sharing the
// id8. Normally that is 0 or 1 row; two pipelines whose UUIDs share the first 8
// chars collide on the topic name itself (cdc_kafka_teardown.go id8IsUnique
// documents the same hazard from the teardown side), so the gate demands access
// to all of them.
func loadTopicPipelineAccess(db *sql.DB, id8, authUser string) ([]resourceAccess, error) {
	rows, err := db.Query(topicPipelineAccessQuery, id8, authUser)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []resourceAccess
	for rows.Next() {
		var workspace, owner, role sql.NullString
		if err := rows.Scan(&workspace, &owner, &role); err != nil {
			return nil, err
		}
		out = append(out, resourceAccess{
			found:       true,
			workspaceID: workspace.String,
			owner:       owner.String,
			memberRole:  role.String,
		})
	}
	return out, rows.Err()
}

// readableTopicID8s is the set of pipeline id8s authUser may see.
func readableTopicID8s(db *sql.DB, authUser string) (map[string]bool, error) {
	rows, err := db.Query(readableTopicID8Query, authUser)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]bool{}
	for rows.Next() {
		var id8 sql.NullString
		if err := rows.Scan(&id8); err != nil {
			return nil, err
		}
		if id8.Valid {
			out[strings.ToLower(id8.String)] = true
		}
	}
	return out, rows.Err()
}

// requireTopicPipelineScope gates a MUTATING topology action (create, delete,
// repartition) on the caller's workspace role in the pipeline whose data the
// topic carries. Trusted internal callers pass through exactly as in
// cdc_authz.go — the planner's POST /topics (strategies.py:766) and api-gateway
// have already applied their own gates.
//
// Writes the response and aborts on every failure, so the handler must return.
func (h *TopologyHandler) requireTopicPipelineScope(c *gin.Context, topic string) bool {
	authUser, internal := cdcPrincipal(c)
	if internal {
		return true
	}
	if authUser == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"success": false, "error": "authentication required"})
		c.Abort()
		return false
	}
	id8, ok := topicPipelineID8(topic)
	if !ok {
		// Shared platform topic: it is not any one tenant's to mutate, and
		// deleting it would take every tenant's control plane with it.
		log.Warnf("🚫 topology: refusing %s on shared platform topic %q from a user principal",
			c.Request.Method, topic)
		c.JSON(http.StatusForbidden, gin.H{
			"success": false,
			"error":   "topic is not scoped to a pipeline; internal callers only",
		})
		c.Abort()
		return false
	}
	if h.db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "database unavailable"})
		c.Abort()
		return false
	}
	rows, err := loadTopicPipelineAccess(h.db, id8, authUser)
	if err != nil {
		log.WithError(err).Warn("topology: topic ownership lookup failed")
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "authorization check failed"})
		c.Abort()
		return false
	}
	if len(rows) == 0 {
		// No live pipeline owns this topic, so there is nobody to authorize
		// against. Reclaiming an orphan is the internal teardown route's job
		// (POST /cdc/kafka-teardown), not a tenant's — fail closed.
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "no pipeline owns this topic"})
		c.Abort()
		return false
	}
	// Fail closed on an id8 collision: one topic carries both pipelines' data,
	// so the caller must be authorized for every pipeline the name can mean.
	for _, a := range rows {
		allow, status, message := decideResourceAccess(a, authUser, "topic")
		if !allow {
			c.JSON(status, gin.H{"success": false, "error": message})
			c.Abort()
			return false
		}
	}
	return true
}

// requireTopicReadScope is the read-side twin: it gates GET /topics/:name.
// Reading is not mutating, so the floor is any workspace membership rather than
// requireTopicPipelineScope's `member`. Shared platform topics stay readable —
// they are rsync-ai's own infrastructure, not another tenant's data — but a
// pipeline topic the caller cannot reach answers 404 rather than 403, so the
// route does not confirm another tenant's pipeline exists.
func (h *TopologyHandler) requireTopicReadScope(c *gin.Context, topic string) bool {
	authUser, internal := cdcPrincipal(c)
	if internal {
		return true
	}
	if authUser == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"success": false, "error": "authentication required"})
		c.Abort()
		return false
	}
	id8, ok := topicPipelineID8(topic)
	if !ok {
		return true
	}
	if h.db == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "database unavailable"})
		c.Abort()
		return false
	}
	rows, err := loadTopicPipelineAccess(h.db, id8, authUser)
	if err != nil {
		log.WithError(err).Warn("topology: topic read lookup failed")
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "authorization check failed"})
		c.Abort()
		return false
	}
	if !topicReadAllowed(rows, authUser) {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "topic not found"})
		c.Abort()
		return false
	}
	return true
}

// topicReadAllowed applies the read floor to every pipeline sharing the id8:
// any membership role passes, a pre-workspaces row falls back to its creator,
// and an empty set fails closed. Split out so the policy is unit-testable
// without a database, matching decideResourceAccess's shape.
func topicReadAllowed(rows []resourceAccess, authUser string) bool {
	if len(rows) == 0 {
		return false
	}
	for _, a := range rows {
		if a.workspaceID == "" {
			if a.owner == "" || a.owner != authUser {
				return false
			}
			continue
		}
		if a.memberRole == "" {
			return false
		}
	}
	return true
}

// RegisterRoutes registers the topology API routes
func (h *TopologyHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/topics", h.CreateTopic)
	rg.POST("/topics/pipeline", h.CreateTopicForPipeline)
	rg.GET("/topics", h.ListTopics)
	rg.GET("/topics/:name", h.GetTopic)
	rg.DELETE("/topics/:name", h.DeleteTopic)
	rg.PUT("/topics/:name/partitions", h.UpdatePartitions)
	rg.GET("/calculate-partitions", h.CalculatePartitions)
}

// CreateTopic creates a new Kafka topic
// POST /api/v1/topology/topics
func (h *TopologyHandler) CreateTopic(c *gin.Context) {
	var req CreateTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, TopicResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	if rejectForeignTopic(c, req.TopicName) {
		return
	}
	if !h.requireTopicPipelineScope(c, req.TopicName) {
		return
	}

	log.Infof("📦 Creating topic: %s (partitions=%d)", req.TopicName, req.Partitions)

	config := kafka.TopicConfig{
		Name:              req.TopicName,
		Partitions:        req.Partitions,
		ReplicationFactor: req.ReplicationFactor,
		Config:            req.Config,
	}

	if err := h.manager.CreateTopic(c.Request.Context(), config); err != nil {
		log.Errorf("Failed to create topic: %v", err)
		c.JSON(http.StatusInternalServerError, TopicResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	now := time.Now()
	c.JSON(http.StatusCreated, TopicResponse{
		Success:           true,
		TopicName:         req.TopicName,
		Partitions:        int(req.Partitions),
		ReplicationFactor: int(req.ReplicationFactor),
		Config:            req.Config,
		CreatedAt:         &now,
	})
}

// CreateTopicForPipeline creates an optimally configured topic for a pipeline
// POST /api/v1/topology/topics/pipeline
// This is the main entry point for plan-time topic provisioning
func (h *TopologyHandler) CreateTopicForPipeline(c *gin.Context) {
	var req CreateTopicForPipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, TopicResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	// The topic name is constructed from req.PipelineID rather than supplied, so
	// the namespace guard has nothing to catch here — the whole boundary is
	// whether the caller may act on THAT pipeline. Same workspace-role gate the
	// six CDC control routes run.
	if !assertPipelineOwnerForHandlers(c, h.db, req.PipelineID) {
		return
	}

	log.Infof("📦 Creating pipeline topic: pipeline=%s, mode=%s, tables=%d, size=%.1fGB",
		req.PipelineID, req.SyncMode, req.TableCount, req.EstimatedSizeGB)

	topicInfo, err := h.manager.CreateTopicForPipeline(
		c.Request.Context(),
		req.PipelineID,
		req.SyncMode,
		req.TableCount,
		req.EstimatedSizeGB,
	)
	if err != nil {
		log.Errorf("Failed to create pipeline topic: %v", err)
		c.JSON(http.StatusInternalServerError, TopicResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	now := time.Now()
	c.JSON(http.StatusCreated, TopicResponse{
		Success:           true,
		TopicName:         topicInfo.Name,
		Partitions:        topicInfo.Partitions,
		ReplicationFactor: topicInfo.ReplicationFactor,
		Config:            topicInfo.Config,
		CreatedAt:         &now,
	})
}

// ListTopics lists the Kafka topics the caller may see
// GET /api/v1/topology/topics
//
// This is a broker-wide enumeration, so it needs both confinements. Without the
// namespace filter it hands a BYO-Kafka customer's caller a directory of that
// customer's unrelated applications; without the tenant filter it enumerates
// every other tenant's pipeline id8s.
func (h *TopologyHandler) ListTopics(c *gin.Context) {
	authUser, internal := cdcPrincipal(c)
	if !internal && authUser == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"success": false, "error": "authentication required"})
		return
	}

	// Resolved before the broker call so a failed lookup cannot fall through to
	// an unfiltered listing.
	var reachable map[string]bool
	if !internal {
		if h.db == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "database unavailable"})
			return
		}
		var err error
		if reachable, err = readableTopicID8s(h.db, authUser); err != nil {
			log.WithError(err).Warn("topology: readable-pipeline lookup failed")
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "authorization check failed"})
			return
		}
	}

	topics, err := h.manager.ListTopics(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Filter out internal topics unless requested
	includeInternal := c.Query("include_internal") == "true"
	result := visibleTopics(topics, includeInternal, internal, reachable)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"topics":  result,
		"count":   len(result),
	})
}

// visibleTopics applies both confinements to a raw broker listing. Split out so
// the filter is unit-testable without a live TopologyManager. `internal` is the
// trusted-service principal; `reachable` is meaningless when it is true.
func visibleTopics(topics map[string]*kafka.TopicInfo, includeInternal, internal bool, reachable map[string]bool) []*kafka.TopicInfo {
	result := make([]*kafka.TopicInfo, 0, len(topics))
	for _, topic := range topics {
		if topic == nil {
			continue
		}
		if topic.IsInternal && !includeInternal {
			continue
		}
		// Applied after include_internal on purpose: that flag widens the view
		// within the platform namespace, it must not escape it.
		if !isPlatformOwnedTopic(topic.Name) {
			continue
		}
		if !internal {
			// Shared platform topics (no id8) stay visible — they are the
			// product's own infrastructure, not another tenant's data.
			if id8, ok := topicPipelineID8(topic.Name); ok && !reachable[id8] {
				continue
			}
		}
		result = append(result, topic)
	}
	return result
}

// GetTopic gets information about a specific topic
// GET /api/v1/topology/topics/:name
func (h *TopologyHandler) GetTopic(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "topic name is required",
		})
		return
	}

	if rejectForeignTopic(c, name) {
		return
	}
	if !h.requireTopicReadScope(c, name) {
		return
	}

	topic, err := h.manager.GetTopic(c.Request.Context(), name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"topic":   topic,
	})
}

// DeleteTopic deletes a Kafka topic
// DELETE /api/v1/topology/topics/:name
func (h *TopologyHandler) DeleteTopic(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "topic name is required",
		})
		return
	}

	if rejectForeignTopic(c, name) {
		return
	}
	if !h.requireTopicPipelineScope(c, name) {
		return
	}

	log.Warnf("🗑️ Deleting topic: %s", name)

	if err := h.manager.DeleteTopic(c.Request.Context(), name); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Topic deleted",
	})
}

// UpdatePartitionsRequest is the request for updating partitions
type UpdatePartitionsRequest struct {
	Partitions int32 `json:"partitions" binding:"required"`
}

// UpdatePartitions updates the partition count for a topic
// PUT /api/v1/topology/topics/:name/partitions
func (h *TopologyHandler) UpdatePartitions(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   "topic name is required",
		})
		return
	}

	if rejectForeignTopic(c, name) {
		return
	}
	if !h.requireTopicPipelineScope(c, name) {
		return
	}

	var req UpdatePartitionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	log.Infof("📈 Updating partitions for topic %s to %d", name, req.Partitions)

	if err := h.manager.UpdatePartitions(c.Request.Context(), name, req.Partitions); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":    true,
		"message":    "Partitions updated",
		"partitions": req.Partitions,
	})
}

// CalculatePartitions calculates optimal partitions for given parameters
// GET /api/v1/topology/calculate-partitions?sync_mode=cdc&table_count=10&size_gb=50
func (h *TopologyHandler) CalculatePartitions(c *gin.Context) {
	syncMode := c.DefaultQuery("sync_mode", "batch")
	tableCount := 1
	if tc := c.Query("table_count"); tc != "" {
		if _, err := fmt.Sscanf(tc, "%d", &tableCount); err != nil {
			tableCount = 1
		}
	}
	sizeGB := 0.0
	if sg := c.Query("size_gb"); sg != "" {
		fmt.Sscanf(sg, "%f", &sizeGB)
	}

	// Use the manager's calculation logic
	partitions := h.calculateOptimalPartitions(syncMode, tableCount, sizeGB)

	c.JSON(http.StatusOK, gin.H{
		"success":                true,
		"sync_mode":              syncMode,
		"table_count":            tableCount,
		"size_gb":                sizeGB,
		"recommended_partitions": partitions,
		"explanation":            h.getPartitionExplanation(syncMode, tableCount, sizeGB, partitions),
	})
}

// calculateOptimalPartitions calculates optimal partition count for dry-run endpoint.
// Note: Duplicates logic from TopologyManager.calculateOptimalPartitions intentionally
// to allow partition calculation without creating topics (preview/dry-run mode).
func (h *TopologyHandler) calculateOptimalPartitions(syncMode string, tableCount int, sizeGB float64) int {
	const (
		MinPartitions  = 3
		MaxPartitions  = 50
		GBPerPartition = 2.0
	)

	var partitions int
	if syncMode == "cdc" || syncMode == "streaming" {
		partitions = max(MinPartitions, tableCount)
	} else {
		partitions = max(MinPartitions, int(sizeGB/GBPerPartition))
	}
	return min(max(partitions, MinPartitions), MaxPartitions)
}

func (h *TopologyHandler) getPartitionExplanation(syncMode string, tableCount int, sizeGB float64, partitions int) string {
	if syncMode == "cdc" || syncMode == "streaming" {
		return fmt.Sprintf("CDC mode: %d tables → %d partitions (1 partition per table for ordering guarantee)", tableCount, partitions)
	}
	return fmt.Sprintf("Batch mode: %.1f GB → %d partitions (2GB per partition for parallelism)", sizeGB, partitions)
}

// min/max helpers - package-scoped (Go idiom: duplicate simple helpers rather than create shared package)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
