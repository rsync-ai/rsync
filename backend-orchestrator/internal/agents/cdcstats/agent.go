package cdcstats

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
)

// Agent consumes Debezium CDC topics and emits TABLE_STATS domain events.
// This enables DMS-like per-table statistics for CDC pipelines.
//
// Enable with: ENABLE_CDC_TABLE_STATS=true
type Agent struct {
	db         *sql.DB
	kafka      *kafka.Manager
	connectURL string
	httpClient *http.Client
	// security carries the SPLIT broker list plus any SASL/TLS the deployment
	// configured. It comes from the kafka.Manager rather than being re-derived
	// here, which is what removed this file's own hardcoded "kafka:29092".
	security   kafkaclient.Config
	flushEvery time.Duration
	syncEvery  time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	workers map[string]*pipelineWorker // pipeline_id -> worker
	started bool
}

type pipelineWorker struct {
	pipelineID    string
	executionID   string
	connectorName string
	topicPrefix   string
	// destNamespace is pipelines.config->>'destination_namespace' — the schema this
	// pipeline actually writes into. Used only to qualify the DROP statements this
	// worker reports, which the user runs by hand against the DESTINATION; see
	// healer.DestinationQualifiedTable. Refreshed on every sync tick alongside
	// executionID, because the namespace is locked on the pipeline's first run and a
	// worker started before that lock would otherwise hold "" forever.
	destNamespace string

	// selectedTables is pipelines.config->'selected_tables' — the tables this
	// pipeline was actually told to capture. The DDL consumer needs it to decide
	// whether a source DROP is worth recording: a drop of a table nobody selected
	// is somebody else's DDL on a shared database, and degrading a stream over it
	// would be a false alarm operators page on.
	//
	// Guarded by selMu rather than written bare like destNamespace above, because a
	// slice header is three words: the sync tick that refreshes it and the DDL
	// consumer goroutine that reads it can otherwise tear one.
	selMu          sync.RWMutex
	selectedTables []string

	consumer sarama.ConsumerGroup
	// ddlConsumer reads the connector's bare topic.prefix topic — Debezium's
	// schema-change stream, which no consumer read until CDC-D1. Separate group from
	// `consumer` on purpose: the two want opposite starting offsets (see
	// newSchemaChangeConsumerConfig) and must not share a lag or a rebalance.
	ddlConsumer sarama.ConsumerGroup
	stats       *Accumulator

	ctx    context.Context
	cancel context.CancelFunc
}

// kafkaServiceName names the PROCESS this agent runs inside, not the agent, so
// the two consumer groups below identify themselves to a customer-managed broker
// as "rsync-orchestrator" in its request logs, quota buckets and authorization
// denials instead of the client library's anonymous default. KAFKA_CLIENT_ID,
// which the manager records when it reads the environment, still wins.
const kafkaServiceName = "orchestrator"

func New(db *sql.DB, kafkaManager *kafka.Manager) *Agent {
	var security kafkaclient.Config
	if kafkaManager != nil {
		security = kafkaManager.SecurityConfig().WithServiceName(kafkaServiceName)
	}

	connectURL := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
	if connectURL == "" {
		connectURL = "http://kafka-connect:8083"
	}

	return &Agent{
		db:         db,
		kafka:      kafkaManager,
		connectURL: connectURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		security:   security,
		flushEvery: 10 * time.Second,
		syncEvery:  30 * time.Second,
		workers:    make(map[string]*pipelineWorker),
	}
}

func (a *Agent) Start() error {
	if os.Getenv("ENABLE_CDC_TABLE_STATS") != "true" {
		return nil
	}
	if a.db == nil || a.kafka == nil {
		log.Warn("cdc table stats enabled but db/kafka is nil; skipping")
		return nil
	}

	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return nil
	}
	a.started = true
	a.ctx, a.cancel = context.WithCancel(context.Background())
	a.mu.Unlock()

	go a.syncLoop()
	log.Info("✅ CDC table stats agent started (Debezium topic consumer)")
	return nil
}

func (a *Agent) Stop() {
	a.mu.Lock()
	if !a.started {
		a.mu.Unlock()
		return
	}
	if a.cancel != nil {
		a.cancel()
	}
	workers := a.workers
	a.workers = make(map[string]*pipelineWorker)
	a.started = false
	a.mu.Unlock()

	for _, w := range workers {
		w.stop()
	}
}

// StopPipeline tears down the worker for a single pipeline, leaving the rest of
// the agent running. Reports whether a worker was actually stopped.
//
// syncOnce already reaps workers whose pipeline is no longer running, but it
// does so on a 30s tick. Pipeline deletion needs the consumer gone *now*:
// Kafka refuses to delete a consumer group while a member is still joined, so
// waiting for the tick would leak cdc-table-stats-<id> and
// cdc-schema-changes-<id> permanently.
func (a *Agent) StopPipeline(pipelineID string) bool {
	pipelineID = strings.TrimSpace(pipelineID)
	if pipelineID == "" {
		return false
	}

	a.mu.Lock()
	w, ok := a.workers[pipelineID]
	if ok {
		delete(a.workers, pipelineID)
	}
	a.mu.Unlock()

	if !ok {
		return false
	}
	w.stop()
	log.WithField("pipeline_id", pipelineID).Info("cdc table stats: stopped worker for deleted pipeline")
	return true
}

// setSelectedTables adopts a freshly-read selection. An EMPTY read is deliberately
// not adopted over a non-empty one, for the same reason destNamespace only ever
// adopts a real value: config->'selected_tables' is absent until the table-selection
// HITL resolves, and a transient empty read must not blank a list the DDL consumer
// is currently matching against.
func (w *pipelineWorker) setSelectedTables(tables []string) {
	if len(tables) == 0 {
		return
	}
	cp := append([]string(nil), tables...)
	w.selMu.Lock()
	w.selectedTables = cp
	w.selMu.Unlock()
}

// selection returns a snapshot the caller may hold across a DB round-trip.
func (w *pipelineWorker) selection() []string {
	w.selMu.RLock()
	defer w.selMu.RUnlock()
	return append([]string(nil), w.selectedTables...)
}

func (w *pipelineWorker) stop() {
	if w.cancel != nil {
		w.cancel()
	}
	if w.consumer != nil {
		_ = w.consumer.Close()
	}
	if w.ddlConsumer != nil {
		_ = w.ddlConsumer.Close()
	}
}

func (a *Agent) syncLoop() {
	// Run once immediately
	a.syncOnce()

	t := time.NewTicker(a.syncEvery)
	defer t.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-t.C:
			a.syncOnce()
		}
	}
}

func (a *Agent) syncOnce() {
	ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
	defer cancel()

	// Discover running CDC pipelines.
	rows, err := a.db.QueryContext(ctx, `
		SELECT
			p.id::text,
			COALESCE(pp.execution_id::text, '') AS execution_id,
			COALESCE(TRIM(COALESCE(p.config->>'destination_namespace', '')), '') AS destination_namespace,
			COALESCE(p.config->'selected_tables', '[]'::jsonb)::text AS selected_tables
		FROM pipelines p
		LEFT JOIN pipeline_progress pp
			ON pp.pipeline_id = p.id
		WHERE p.status = 'running'
		  AND (p.sync_mode = 'cdc' OR p.cdc_mode IS NOT NULL)
	`)
	if err != nil {
		log.WithError(err).Warn("cdc table stats: failed to query running CDC pipelines")
		return
	}
	defer rows.Close()

	type activePipeline struct {
		executionID    string
		destNamespace  string
		selectedTables []string
	}
	active := make(map[string]activePipeline) // pipeline_id -> per-sync facts
	for rows.Next() {
		var id, execID, destNS, selRaw string
		if err := rows.Scan(&id, &execID, &destNS, &selRaw); err == nil && strings.TrimSpace(id) != "" {
			active[strings.TrimSpace(id)] = activePipeline{
				executionID:    strings.TrimSpace(execID),
				destNamespace:  strings.TrimSpace(destNS),
				selectedTables: parseSelectedTables(selRaw),
			}
		}
	}

	// Stop workers for pipelines that are no longer active.
	a.mu.Lock()
	for pid, w := range a.workers {
		if _, ok := active[pid]; !ok {
			go w.stop()
			delete(a.workers, pid)
		}
	}
	a.mu.Unlock()

	// Ensure workers for active pipelines.
	for pid, ap := range active {
		a.ensureWorker(pid, ap.executionID, ap.destNamespace, ap.selectedTables)
	}
}

// parseSelectedTables reads config->'selected_tables' as the JSON array of strings
// every persistence site writes. Anything else is a shape we don't understand, so we
// don't judge it: an empty selection means the DDL consumer records no drops, which
// is the fail-soft direction (a missed degrade, never a false one).
func parseSelectedTables(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		log.WithError(err).Debug("cdc table stats: selected_tables is not a string array (ignored)")
		return nil
	}
	return out
}

func (a *Agent) ensureWorker(pipelineID string, executionID string, destNamespace string, selectedTables []string) {
	a.mu.Lock()
	if w, ok := a.workers[pipelineID]; ok {
		// Keep execution_id fresh (pipeline_progress can change on new run).
		if strings.TrimSpace(executionID) != "" && w.executionID != executionID {
			w.executionID = executionID
		}
		// Same for the namespace: it is locked on the pipeline's first run, which can
		// happen after this worker started. Only ever adopt a real value — a transient
		// empty read must not blank a namespace we already know.
		if ns := strings.TrimSpace(destNamespace); ns != "" && w.destNamespace != ns {
			w.destNamespace = ns
		}
		a.mu.Unlock()
		// Re-selection during a run must reach the DDL consumer, or a table added to
		// the selection after the worker started could be dropped at the source
		// without the stream ever noticing.
		w.setSelectedTables(selectedTables)
		return
	}
	a.mu.Unlock()

	connectorName, cfg, err := a.findConnectorConfig(pipelineID)
	if err != nil {
		// Connector may not exist yet; retry next sync.
		return
	}

	topicPrefix, _ := cfg["topic.prefix"].(string)
	topicPrefix = strings.TrimSpace(topicPrefix)
	if topicPrefix == "" {
		return
	}

	ctx, cancel := context.WithCancel(a.ctx)
	w := &pipelineWorker{
		pipelineID:    pipelineID,
		executionID:   executionID,
		connectorName: connectorName,
		topicPrefix:   topicPrefix,
		destNamespace: strings.TrimSpace(destNamespace),
		stats:         NewAccumulator(pipelineID),
		ctx:           ctx,
		cancel:        cancel,
	}
	w.setSelectedTables(selectedTables)

	groupID := tableStatsGroupID(pipelineID)
	statsCfg, err := a.secured(newConsumerConfigOldest())
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("cdc table stats: invalid Kafka security configuration")
		return
	}
	cg, err := a.consumerGroup(groupID, statsCfg)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("cdc table stats: failed to create consumer group")
		return
	}
	w.consumer = cg

	// Source DDL reporting is best-effort and strictly additive: if the group cannot be
	// created we still want table stats, so this failure is logged and dropped rather
	// than aborting ensureWorker.
	ddlGroupID := schemaChangeGroupID(pipelineID)
	ddlCfg, derr := a.secured(newSchemaChangeConsumerConfig())
	if derr != nil {
		log.WithError(derr).WithField("pipeline_id", pipelineID).
			Warn("cdc schema changes: invalid Kafka security configuration; source DDL will not be reported")
	} else if dcg, derr := a.consumerGroup(ddlGroupID, ddlCfg); derr == nil {
		w.ddlConsumer = dcg
	} else {
		log.WithError(derr).WithField("pipeline_id", pipelineID).
			Warn("cdc schema changes: failed to create consumer group; source DDL will not be reported")
	}

	a.mu.Lock()
	a.workers[pipelineID] = w
	a.mu.Unlock()

	go a.runWorker(w)
	go a.flushLoop(w)
	if w.ddlConsumer != nil {
		go a.runSchemaChangeWorker(w)
	}
}

func (a *Agent) runWorker(w *pipelineWorker) {
	handler := &debeziumHandler{pipelineID: w.pipelineID, stats: w.stats}
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		topics := a.topicsForPrefix(w.topicPrefix)
		if len(topics) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		// Sarama consumer groups can't dynamically add topics mid-Consume.
		// When users add a new table to Debezium, a new topic may appear after the worker starts.
		// We watch for topic set changes and cancel the Consume call to re-join with the new topic list.
		key := topicsKey(topics)
		consumeCtx, cancel := context.WithCancel(w.ctx)
		watchDone := make(chan struct{})
		go func(expectedKey string) {
			defer close(watchDone)
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-consumeCtx.Done():
					return
				case <-t.C:
					next := a.topicsForPrefix(w.topicPrefix)
					if len(next) == 0 {
						continue
					}
					if topicsKey(next) != expectedKey {
						log.WithFields(log.Fields{
							"pipeline_id":  w.pipelineID,
							"topic_prefix": w.topicPrefix,
						}).Info("cdc table stats: topics changed, restarting consumer to include new topics")
						cancel()
						return
					}
				}
			}
		}(key)

		err := w.consumer.Consume(consumeCtx, topics, handler)
		cancel()
		<-watchDone

		// If we cancelled due to topic changes, just loop and re-consume with the updated list.
		if consumeCtx.Err() != nil && w.ctx.Err() == nil {
			continue
		}

		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"pipeline_id":  w.pipelineID,
				"topic_prefix": w.topicPrefix,
			}).Warn("cdc table stats: consume loop error")
			time.Sleep(2 * time.Second)
		}
	}
}

func (a *Agent) topicsForPrefix(prefix string) []string {
	all, err := a.kafka.ListTopics()
	if err != nil {
		return nil
	}
	want := prefix + "."
	out := make([]string, 0, 64)
	for _, t := range all {
		if strings.HasPrefix(t, want) {
			out = append(out, t)
		}
	}
	return out
}

func topicsKey(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	cp := make([]string, 0, len(topics))
	for _, t := range topics {
		v := strings.TrimSpace(t)
		if v != "" {
			cp = append(cp, v)
		}
	}
	sort.Strings(cp)
	return strings.Join(cp, "|")
}

func (a *Agent) flushLoop(w *pipelineWorker) {
	t := time.NewTicker(a.flushEvery)
	defer t.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-t.C:
			updates := w.stats.FlushDirty()
			for _, u := range updates {
				b, err := json.Marshal(BuildCDCTableStatsEvent(w.pipelineID, w.executionID, u))
				if err != nil {
					continue
				}
				_ = a.kafka.ProduceWithHeaders("pipeline.domain.events", []byte(w.pipelineID), b, map[string]string{
					"trace_id": w.pipelineID,
				})
			}
		}
	}
}

// secured stamps the resolved SASL/TLS onto a freshly-built consumer config.
// The two factories below are deliberately pure; routing both through here
// means a cdcstats consumer group cannot be created unsecured by accident.
func (a *Agent) secured(cfg *sarama.Config) (*sarama.Config, error) {
	if err := saramaauth.Apply(cfg, a.security); err != nil {
		return nil, err
	}
	return cfg, nil
}

func newConsumerConfigOldest() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Consumer.Return.Errors = true
	// IMPORTANT:
	// - Debezium snapshot events are produced immediately when the connector starts.
	// - The cdcstats agent may start AFTER the connector (or restart mid-run).
	// Starting from Oldest ensures a newly-created group can backfill snapshot events so
	// "captured" counts match the initial "applied" snapshot rows in the UI.
	// Once offsets are committed, restarts continue from the last committed position.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	return cfg
}

func (a *Agent) findConnectorConfig(pipelineID string) (string, map[string]interface{}, error) {
	// List connectors and pick the one that contains the pipeline_id (or short id).
	names, err := a.listConnectors()
	if err != nil {
		return "", nil, err
	}
	short := pipelineID
	if len(short) > 8 {
		short = short[:8]
	}
	pidLower := strings.ToLower(pipelineID)
	shortLower := strings.ToLower(short)
	cand := ""
	for _, n := range names {
		ln := strings.ToLower(n)
		if strings.Contains(ln, pidLower) || strings.Contains(ln, shortLower) {
			cand = n
			break
		}
	}
	if cand == "" {
		return "", nil, fmt.Errorf("connector not found")
	}

	cfg, err := a.getConnectorConfig(cand)
	if err != nil {
		return "", nil, err
	}
	return cand, cfg, nil
}

func (a *Agent) listConnectors() ([]string, error) {
	req, _ := http.NewRequestWithContext(a.ctx, http.MethodGet, a.connectURL+"/connectors", nil)
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kafka connect status %d", resp.StatusCode)
	}
	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return nil, err
	}
	return names, nil
}

func (a *Agent) getConnectorConfig(connectorName string) (map[string]interface{}, error) {
	u := fmt.Sprintf("%s/connectors/%s/config", a.connectURL, connectorName)
	req, _ := http.NewRequestWithContext(a.ctx, http.MethodGet, u, nil)
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kafka connect status %d", resp.StatusCode)
	}
	var cfg map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

type debeziumHandler struct {
	pipelineID string
	stats      *Accumulator
}

func (h *debeziumHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *debeziumHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *debeziumHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var payload map[string]interface{}
		if err := kafka.SmartDeserialize(msg.Value, &payload); err == nil {
			if upd, ok := ParseDebeziumChange(payload, msg.Topic); ok {
				h.stats.Observe(upd)
			}
		}
		session.MarkMessage(msg, "")
	}
	return nil
}
