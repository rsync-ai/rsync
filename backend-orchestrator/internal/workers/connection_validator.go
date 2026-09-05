package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/shared/correlation"
	"github.com/rsync-ai/shared/crypto"
)

// connectorAliases maps connector types to their known aliases
var connectorAliases = map[string][]string{
	"postgresql": {"postgres", "pg"},
	"postgres":   {"postgresql", "pg"},
	"mysql":      {"mariadb"},
	"mariadb":    {"mysql"},
	"s3":         {"aws-s3", "aws_s3", "minio"},
	"aws-s3":     {"s3", "aws_s3", "minio"},
	"aws_s3":     {"s3", "aws-s3", "minio"},
	"minio":      {"s3", "aws-s3", "aws_s3"},
	"gcs":        {"google-cloud-storage"},
	"mongodb":    {"mongo"},
	"mongo":      {"mongodb"},
}

// ==============================================================================
// CONNECTION VALIDATOR WORKER
// ==============================================================================
// This worker checks if connections exist and are valid.
// This is a clean HITL boundary: if connection is missing, pause and ask user.
//
// Responsibilities:
// 1. Check if source connection exists in database
// 2. Check if destination connection exists in database
// 3. Validate connection credentials by calling MCP test_connection
// 4. Request connection configuration if missing (HITL pause point)
// ==============================================================================

// ConnectionValidatorWorker validates connections
type ConnectionValidatorWorker struct {
	kafkaManager      *kafka.Manager
	db                *sql.DB
	executorAgent     *executor.Agent // For testing connections via MCP
	progressEmitter   *ProgressEmitter
	ctx               context.Context
	cancel            context.CancelFunc
	tracer            trace.Tracer
	correlationClient *correlation.Client
	workerID          string
}

// NewConnectionValidatorWorker creates a new connection validator worker
func NewConnectionValidatorWorker(kafkaManager *kafka.Manager, db *sql.DB, toolsDir string) *ConnectionValidatorWorker {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize executor agent for MCP access (connection testing)
	executorAgent := executor.NewAgent(kafkaManager, db, toolsDir)

	// Initialize correlation client for V2 workflows
	redisAddr := os.Getenv("REDIS_ADDRESS")
	if redisAddr == "" {
		redisAddr = os.Getenv("REDIS_ADDR")
	}
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	correlationClient, err := correlation.NewClient(redisAddr, redisPassword)
	if err != nil {
		log.WithError(err).Warn("Failed to initialize correlation client for Connection Validator worker")
	}

	workerID := fmt.Sprintf("connection-validator-worker-%d", time.Now().UnixNano()%10000)

	return &ConnectionValidatorWorker{
		kafkaManager:      kafkaManager,
		db:                db,
		executorAgent:     executorAgent,
		progressEmitter:   NewProgressEmitter(kafkaManager, db),
		ctx:               ctx,
		cancel:            cancel,
		tracer:            otel.Tracer("connection-validator-worker"),
		correlationClient: correlationClient,
		workerID:          workerID,
	}
}

// AgentName returns the worker name
func (w *ConnectionValidatorWorker) AgentName() string {
	return "connection_validator"
}

// Start begins consuming tasks
func (w *ConnectionValidatorWorker) Start() error {
	log.Info("🚀 Starting Connection Validator Worker")

	// Start Redis poller for V2 workflows (correlation pattern)
	if w.correlationClient != nil {
		go w.startRedisPoller()
		log.Info("✅ ConnectionValidatorWorker: Redis poller started for V2 correlation requests")
	}

	// Consume from dedicated topic (no consumer group = no rebalancing)
	err := w.kafkaManager.ConsumeWithContext("agent.control.commands.connection_validator", w.handleTask)
	if err != nil {
		return fmt.Errorf("failed to consume agent.control.commands: %w", err)
	}

	log.Info("✅ Connection Validator Worker started")
	return nil
}

// Stop stops the worker
func (w *ConnectionValidatorWorker) Stop() {
	log.Info("🛑 Stopping Connection Validator Worker")
	w.cancel()

	// Stop the executor agent
	if w.executorAgent != nil {
		w.executorAgent.Stop()
	}
}

// handleTask processes incoming task assignments
func (w *ConnectionValidatorWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var assignment Task
	if err := json.Unmarshal(msg.Value, &assignment); err != nil {
		log.WithError(err).Error("Failed to unmarshal task assignment")
		return err
	}

	// V2 tasks are handled by the Redis correlation poller; the Kafka path is V1-only.
	// Skipping here prevents double-execution (see KI-HYBRID-1).
	if assignment.CorrelationID != "" {
		return nil
	}

	// Filter: only process connection validation tasks
	if assignment.TaskType != "validate_connection" {
		return nil // Not for us
	}

	log.WithFields(log.Fields{
		"pipeline_id": assignment.PipelineID,
		"task_id":     assignment.TaskID,
	}).Info("🔐 Processing connection validation task")

	result, err := w.ProcessTask(ctx, assignment)
	if err != nil {
		log.WithError(err).Error("Connection validation failed")
		result.Status = "failed"
		result.Error = err.Error()
	}

	// Route result to correlation store (V2) or Kafka (V1)
	return RouteResult(ctx, assignment, result, w.kafkaManager)
}

// ProcessTask implements the connection validation logic
func (w *ConnectionValidatorWorker) ProcessTask(ctx context.Context, assignment Task) (TaskResult, error) {
	// Add timeout protection to avoid hanging forever.
	// NOTE: This stage performs network calls (MCP test_connection), so 5s is too aggressive and can
	// cause false failures. We keep per-connection test timeouts below; this is a hard upper bound.
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	ctx, span := w.tracer.Start(ctx, "connection_validator.process_task",
		trace.WithAttributes(
			attribute.String("task_id", assignment.TaskID),
			attribute.String("pipeline_id", assignment.PipelineID),
		),
	)
	defer span.End()

	_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  assignment.PipelineID,
		ExecutionID: getExecutionID(assignment),
		Stage:       "connection_validator",
		Status:      "processing",
		Message:     "Validating connections",
		Summary:     "Validating connections",
		Progress: ProgressInfo{
			Percent:     28,
			CurrentStep: 3,
			TotalSteps:  7,
			Stage:       "connection_validator",
		},
	})

	// Extract connector information
	sourceConnector := assignment.Context["source_connector"]
	destinationConnector := assignment.Context["destination_connector"]
	userID := assignment.UserID

	// PHASE 1: Check if connection IDs were provided (from HITL resume or previous stage)
	sourceConnID := w.getConnectionID(assignment, "source")
	destConnID := w.getConnectionID(assignment, "destination")

	// Allow validation to proceed if connection IDs are provided, even if connector metadata is missing
	// This is essential for V2 correlation-based validation after HITL resume
	if (sourceConnector == nil || destinationConnector == nil) && (sourceConnID == "" || destConnID == "") {
		return TaskResult{
			TaskID:      assignment.TaskID,
			WorkflowID:  assignment.WorkflowID,
			StepID:      assignment.StepID,
			Status:      "failed",
			Error:       "Missing connector information in context and no connection IDs provided",
			CompletedAt: time.Now(),
			TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		}, fmt.Errorf("missing connector information")
	}

	sourceType := ""
	destType := ""
	if sourceConnector != nil {
		sourceType, _ = sourceConnector.(string)
	}
	if destinationConnector != nil {
		destType, _ = destinationConnector.(string)
	}

	log.WithFields(log.Fields{
		"pipeline_id":      assignment.PipelineID,
		"user_id":          userID,
		"source_type":      sourceType,
		"dest_type":        destType,
		"source_conn_id":   sourceConnID,
		"dest_conn_id":     destConnID,
	}).Info("🔐 Checking connection availability")

	var sourceConnection map[string]interface{}
	var destConnection map[string]interface{}
	var err error

	// PHASE 2: Load connections by ID if provided, otherwise search by type
	if sourceConnID != "" {
		log.WithFields(log.Fields{
			"connection_id": sourceConnID,
			"pipeline_id":   assignment.PipelineID,
		}).Info("🔍 Loading source connection by ID (from resume/payload)")
		sourceConnection, err = w.loadConnectionByID(ctx, userID, sourceConnID, "source")
		if err != nil {
			log.WithError(err).Warn("Failed to load source connection by ID")
			// Connection was deleted or belongs to different user
			return w.emitConnectionError(ctx, assignment, "source", sourceConnID, err)
		}
	} else {
		// Search by connector type
		sourceConnection, err = w.findConnection(ctx, userID, sourceType, "source")
		if err != nil {
			log.WithError(err).Warn("Error querying source connection")
		}
	}

	if destConnID != "" {
		log.WithFields(log.Fields{
			"connection_id": destConnID,
			"pipeline_id":   assignment.PipelineID,
		}).Info("🔍 Loading destination connection by ID (from resume/payload)")
		destConnection, err = w.loadConnectionByID(ctx, userID, destConnID, "destination")
		if err != nil {
			log.WithError(err).Warn("Failed to load destination connection by ID")
			return w.emitConnectionError(ctx, assignment, "destination", destConnID, err)
		}
	} else {
		destConnection, err = w.findConnection(ctx, userID, destType, "destination")
		if err != nil {
			log.WithError(err).Warn("Error querying destination connection")
		}
	}

	// If both connections exist, validate them
	if sourceConnection != nil && destConnection != nil {
		log.WithField("pipeline_id", assignment.PipelineID).Info("✅ Both connections found, validating...")

		// Use the connection's REAL connector_type for validation (not the requested type)
		actualSourceType := sourceConnection["connector_type"].(string)
		actualDestType := destConnection["connector_type"].(string)

		// Validate connections by calling MCP test_connection (parallel for speed)
		type vres struct {
			ok   bool
			err  string
		}

		sourceCh := make(chan vres, 1)
		destCh := make(chan vres, 1)

		go func() {
			ok, errMsg := w.validateConnection(ctx, actualSourceType, getConnectorVersionFromTask(assignment, "source"), sourceConnection)
			sourceCh <- vres{ok: ok, err: errMsg}
		}()
		go func() {
			ok, errMsg := w.validateConnection(ctx, actualDestType, getConnectorVersionFromTask(assignment, "destination"), destConnection)
			destCh <- vres{ok: ok, err: errMsg}
		}()

		sourceRes := <-sourceCh
		destRes := <-destCh

		sourceValid, sourceError := sourceRes.ok, sourceRes.err
		destValid, destError := destRes.ok, destRes.err

		if sourceValid && destValid {
			log.WithFields(log.Fields{
				"pipeline_id":   assignment.PipelineID,
				"source_id":     sourceConnection["id"],
				"source_name":   sourceConnection["name"],
				"dest_id":       destConnection["id"],
				"dest_name":     destConnection["name"],
			}).Info("✅ All connections valid")

			// Probe destination schema access as a soft pre-flight check.
			// If schema discovery fails despite auth success, the user likely lacks write permissions.
			destSchemaWarning := w.probeDestinationSchemaAccess(ctx,
				actualDestType,
				getConnectorVersionFromTask(assignment, "destination"),
				destConnection,
			)

			// Persist connection IDs to pipelines table so later workers can access them
			if assignment.PipelineID != "" {
				w.persistConnectionIDs(ctx, assignment.PipelineID,
					sourceConnection["id"].(string),
					destConnection["id"].(string))
			}

			progressMeta := map[string]interface{}{
				"source_connection_id":      sourceConnection["id"],
				"source_connection_name":    sourceConnection["name"],
				"destination_connection_id": destConnection["id"],
				"destination_connection_name": destConnection["name"],
				"action_taken":              "user_selected",
			}
			if destSchemaWarning != "" {
				progressMeta["destination_schema_warning"] = destSchemaWarning
			}

			_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
				EventType:   "STAGE_COMPLETED",
				PipelineID:  assignment.PipelineID,
				ExecutionID: getExecutionID(assignment),
				Stage:       "connection_validator",
				Status:      "processing",
				Message:     "Connections validated successfully",
				Summary:     "Connections valid",
				Progress: ProgressInfo{
					Percent:     30,
					CurrentStep: 3,
					TotalSteps:  7,
					Stage:       "connection_validator",
				},
				Metadata: progressMeta,
			})

			output := map[string]interface{}{
				"source_connection_id":        sourceConnection["id"],
				"source_connection_name":      sourceConnection["name"],
				"destination_connection_id":   destConnection["id"],
				"destination_connection_name": destConnection["name"],
				"source_connection":           sourceConnection,
				"destination_connection":      destConnection,
				"source_valid":                true,
				"destination_valid":           true,
				"action_taken":                "user_selected",
			}
			if destSchemaWarning != "" {
				output["destination_schema_warning"] = destSchemaWarning
			}

			return TaskResult{
				TaskID:     assignment.TaskID,
				WorkflowID: assignment.WorkflowID,
				StepID:     assignment.StepID,
				Status:     "success",
				Output:     output,
				NextAction:  "discover_schema",
				CompletedAt: time.Now(),
				TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
			}, nil
		}

		// Connections exist but validation failed
		validationErrors := make(map[string]string)
		if !sourceValid {
			validationErrors["source"] = sourceError
		}
		if !destValid {
			validationErrors["destination"] = destError
		}

		log.WithFields(log.Fields{
			"pipeline_id":       assignment.PipelineID,
			"validation_errors": validationErrors,
		}).Warn("⚠️  Connection validation failed")

		_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "PIPELINE_WAITING",
			PipelineID:  assignment.PipelineID,
			ExecutionID: getExecutionID(assignment),
			Stage:       "connection_validator",
			Status:      "waiting_for_user",
			Message:     "Connection credentials need attention",
			Summary:     "Waiting for connection fix",
			Progress: ProgressInfo{
				Percent:     30,
				CurrentStep: 3,
				TotalSteps:  7,
				Stage:       "connection_validator",
			},
			BlockingReason: &BlockingReason{
				Type:        "user_input_required",
				Description: "Please fix connection credentials to continue.",
				Details: map[string]interface{}{
					"request_type":          "connection_fix",
					"validation_errors":     validationErrors,
					"source_connector":      sourceType,
					"destination_connector": destType,
				},
			},
			Metadata: map[string]interface{}{
				"validation_errors":     validationErrors,
				"source_connector":      sourceType,
				"destination_connector": destType,
			},
		})

		return TaskResult{
			TaskID:     assignment.TaskID,
			WorkflowID: assignment.WorkflowID,
			StepID:     assignment.StepID,
			Status:     "needs_input", // Trigger HITL to fix credentials
			Output: map[string]interface{}{
				"source_connector":       sourceType,
				"destination_connector":  destType,
				"source_connection":      sourceConnection,
				"destination_connection": destConnection,
				"validation_errors":      validationErrors,
				"requires_fix":           true,
			},
			NextAction:  "await_connection_fix",
			CompletedAt: time.Now(),
			TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		}, nil
	}

	// If connections missing, try auto-select or request configuration (HITL)
	// PHASE 3: Auto-select if exactly one viable option for each side
	if sourceConnection == nil || destConnection == nil {
		log.WithFields(log.Fields{
			"pipeline_id":  assignment.PipelineID,
			"source_found": sourceConnection != nil,
			"dest_found":   destConnection != nil,
		}).Info("⚠️  Connections missing or incomplete, checking for auto-select")

		// Try to auto-select if no explicit IDs were provided
		if sourceConnID == "" && destConnID == "" {
			autoSelected, result := w.tryAutoSelect(ctx, assignment, userID, sourceType, destType, sourceConnection, destConnection)
			if autoSelected {
				return result, nil
			}
		}

		// Auto-select failed or not attempted - emit HITL with V2 shape
		return w.emitConnectionConfigHITL(ctx, assignment, sourceType, destType, sourceConnection, destConnection)
	}

	// This shouldn't be reached, but just in case
	return TaskResult{
		TaskID:      assignment.TaskID,
		WorkflowID:  assignment.WorkflowID,
		StepID:      assignment.StepID,
		Status:      "failed",
		Error:       "Unexpected state in connection validator",
		CompletedAt: time.Now(),
		TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
	}, fmt.Errorf("unexpected state")
}

// findConnection looks up an existing connection from the database
func (w *ConnectionValidatorWorker) findConnection(ctx context.Context, userID, connectorType, connectionType string) (map[string]interface{}, error) {
	// Query the connections table
	var id, name, encryptedConfig string
	var lastTestedAt sql.NullTime
	var lastTestStatus sql.NullString

	query := `
		SELECT id, name, config, last_tested_at, last_test_status
		FROM connections
		WHERE user_id = $1 
		AND connector_type = $2 
		AND type = $3
		AND status = 'active'
		ORDER BY updated_at DESC
		LIMIT 1
	`

	err := w.db.QueryRowContext(ctx, query, userID, connectorType, connectionType).Scan(
		&id, &name, &encryptedConfig, &lastTestedAt, &lastTestStatus,
	)

	if err == sql.ErrNoRows {
		log.WithFields(log.Fields{
			"user_id":        userID,
			"connector_type": connectorType,
			"type":           connectionType,
		}).Info("🔍 No connection found")
		return nil, nil // No connection found (not an error)
	}

	if err != nil {
		log.WithError(err).Error("Database query failed")
		return nil, err
	}

	// Decrypt config
	decryptedJSON, err := crypto.DecryptString(encryptedConfig)
	if err != nil {
		log.WithError(err).Error("Failed to decrypt connection config")
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}

	connection := map[string]interface{}{
		"id":             id,
		"name":           name,
		"config":         decryptedJSON, // Stored as JSON string, will be parsed later
		"connector_type": connectorType,
		"type":           connectionType,
	}

	if lastTestedAt.Valid {
		connection["last_tested_at"] = lastTestedAt.Time
	}
	if lastTestStatus.Valid {
		connection["last_test_status"] = lastTestStatus.String
	}

	log.WithFields(log.Fields{
		"connection_id": id,
		"name":          name,
		"type":          connectionType,
	}).Info("✅ Connection found and decrypted")

	return connection, nil
}

// validateConnection validates connection credentials by calling MCP test_connection
func (w *ConnectionValidatorWorker) validateConnection(ctx context.Context, connectorType string, connectorVersion string, connection map[string]interface{}) (bool, string) {
	// Tight timeout for connection tests so the UI doesn't look "stuck".
	// If a connector is slow/hung, we'll fail fast and ask user to fix credentials/host.
	testCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	// Extract config (already decrypted in findConnection)
	configStr, ok := connection["config"].(string)
	if !ok || configStr == "" {
		return false, "Missing connection configuration"
	}

	// Parse config as generic map[string]interface{} to handle all field types (strings, numbers, booleans)
	var configInterface map[string]interface{}
	if err := json.Unmarshal([]byte(configStr), &configInterface); err != nil {
		log.WithError(err).Warn("Failed to parse connection config")
		return false, "Invalid connection configuration format"
	}

	// Convert to map[string]string for MCP calls (MCP expects string values)
	// This is generic and works for all connectors
	configMap := make(map[string]string)
	for k, v := range configInterface {
		configMap[k] = fmt.Sprintf("%v", v)
	}

	// Update connection object with parsed config so it's available in output
	// This is important because downstream workers (Discovery, Executor) expect a map, not a string
	// However, we must be careful not to expose secrets in logs/output if possible
	// But downstream workers NEED the credentials to function.
	connection["config"] = configInterface

	log.WithFields(log.Fields{
		"connector_type": connectorType,
		"connection_id":  connection["id"],
	}).Info("🔍 Testing connection via MCP")

	// Call executor agent's TestConnection method (which calls MCP)
	valid, errorMsg := w.executorAgent.TestConnection(testCtx, connectorType, connectorVersion, configMap)

	if !valid {
		log.WithFields(log.Fields{
			"connector_type": connectorType,
			"error":          errorMsg,
		}).Warn("❌ Connection test failed")
		return false, errorMsg
	}

	log.WithField("connector_type", connectorType).Info("✅ Connection test passed")
	return true, ""
}

// probeDestinationSchemaAccess attempts a schema discovery on the destination connector (soft check).
// Returns a non-empty warning string if discovery fails despite auth succeeding — this indicates the
// destination user may lack CREATE/INSERT privileges even though credentials are valid.
func (w *ConnectionValidatorWorker) probeDestinationSchemaAccess(ctx context.Context, connectorType string, _ string, connection map[string]interface{}) string {
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	configRaw, ok := connection["config"]
	if !ok || configRaw == nil {
		return ""
	}
	var configMap map[string]interface{}
	switch v := configRaw.(type) {
	case map[string]interface{}:
		configMap = v
	case string:
		if err := json.Unmarshal([]byte(v), &configMap); err != nil {
			return ""
		}
	default:
		return ""
	}

	tables, err := w.executorAgent.DiscoverSchema(probeCtx, connectorType, configMap)
	if err != nil {
		log.WithFields(log.Fields{
			"connector_type": connectorType,
			"error":          err.Error(),
		}).Warn("⚠️ Destination schema probe failed — user may lack schema access")
		return fmt.Sprintf("Destination connected but schema discovery failed (%s). Check the database user has CREATE TABLE and INSERT privileges on the target schema.", err.Error())
	}

	log.WithFields(log.Fields{
		"connector_type": connectorType,
		"table_count":    len(tables),
	}).Info("✅ Destination schema probe passed")
	return ""
}

// getConnectorVersionFromTask attempts to extract a connector version from the task payload/context.
// This keeps the worker generic across workflow versions and avoids hardcoding "latest" when a
// connection/pipeline is pinned to a specific connector version (e.g. api-key vs oauth versions).
func getConnectorVersionFromTask(task Task, direction string) string {
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction == "" {
		return ""
	}

	// 1) payload[direction].version  (e.g. {"type":"pipedrive","version":"v1.0.1"})
	if task.Payload != nil {
		if raw, ok := task.Payload[direction]; ok {
			if m, ok := raw.(map[string]interface{}); ok && m != nil {
				if v, ok := m["version"].(string); ok && strings.TrimSpace(v) != "" {
					return v
				}
			}
		}
		// 2) payload["<direction>_connector_snapshot"].version
		snapKey := fmt.Sprintf("%s_connector_snapshot", direction)
		if raw, ok := task.Payload[snapKey]; ok {
			if m, ok := raw.(map[string]interface{}); ok && m != nil {
				if v, ok := m["version"].(string); ok && strings.TrimSpace(v) != "" {
					return v
				}
			}
		}
	}

	// 3) context["<direction>_version"]
	if task.Context != nil {
		ctxKey := fmt.Sprintf("%s_version", direction)
		if v, ok := task.Context[ctxKey].(string); ok && strings.TrimSpace(v) != "" {
			return v
		}
	}

	return ""
}

// getConnectionID extracts connection ID from assignment payload/context/pipelines table
func (w *ConnectionValidatorWorker) getConnectionID(assignment Task, direction string) string {
	field := "source_connection_id"
	if direction == "destination" {
		field = "destination_connection_id"
	}

	// 1. Check Payload (preferred - from Temporal signal)
	if val, ok := assignment.Payload[field]; ok {
		if id, ok := val.(string); ok && id != "" {
			return id
		}
	}

	// 2. Check Context
	if val, ok := assignment.Context[field]; ok {
		if id, ok := val.(string); ok && id != "" {
			return id
		}
	}

	// 3. Fallback to pipelines table
	if assignment.PipelineID != "" {
		var connID sql.NullString
		query := fmt.Sprintf("SELECT %s FROM pipelines WHERE id = $1", field)
		err := w.db.QueryRow(query, assignment.PipelineID).Scan(&connID)
		if err == nil && connID.Valid && connID.String != "" {
			log.WithFields(log.Fields{
				"pipeline_id":   assignment.PipelineID,
				"direction":     direction,
				"connection_id": connID.String,
			}).Info("🔍 Found connection ID in pipelines table")
			return connID.String
		}
	}

	return ""
}

// loadConnectionByID loads a connection by ID with validation
func (w *ConnectionValidatorWorker) loadConnectionByID(ctx context.Context, userID, connectionID, expectedType string) (map[string]interface{}, error) {
	var id, name, connectorType, connType, encryptedConfig, status string
	var lastTestedAt sql.NullTime
	var lastTestStatus sql.NullString

	query := `
		SELECT id, name, connector_type, type, config, status, last_tested_at, last_test_status
		FROM connections
		WHERE id = $1
	`

	err := w.db.QueryRowContext(ctx, query, connectionID).Scan(
		&id, &name, &connectorType, &connType, &encryptedConfig, &status, &lastTestedAt, &lastTestStatus,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("connection %s not found (may have been deleted)", connectionID)
	}

	if err != nil {
		log.WithError(err).Error("Database query failed")
		return nil, err
	}

	// Validate ownership
	var ownerID string
	err = w.db.QueryRowContext(ctx, "SELECT user_id FROM connections WHERE id = $1", connectionID).Scan(&ownerID)
	if err == nil && ownerID != userID {
		log.WithFields(log.Fields{
			"connection_id": connectionID,
			"owner_id":      ownerID,
			"requested_by":  userID,
		}).Warn("⚠️  Security: Connection access denied (wrong user)")
		return nil, fmt.Errorf("connection not found")
	}

	// Validate type matches
	if connType != expectedType {
		return nil, fmt.Errorf("connection %s is type '%s' but expected '%s'", connectionID, connType, expectedType)
	}

	// Check if connection is active
	if status != "active" {
		return nil, fmt.Errorf("connection %s is not active (status: %s)", connectionID, status)
	}

	// Decrypt config
	decryptedJSON, err := crypto.DecryptString(encryptedConfig)
	if err != nil {
		log.WithError(err).Error("Failed to decrypt connection config")
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}

	connection := map[string]interface{}{
		"id":             id,
		"name":           name,
		"config":         decryptedJSON,
		"connector_type": connectorType,
		"type":           connType,
		"status":         status,
	}

	if lastTestedAt.Valid {
		connection["last_tested_at"] = lastTestedAt.Time
	}
	if lastTestStatus.Valid {
		connection["last_test_status"] = lastTestStatus.String
	}

	log.WithFields(log.Fields{
		"connection_id":  id,
		"name":           name,
		"connector_type": connectorType,
		"type":           connType,
	}).Info("✅ Connection loaded by ID and validated")

	return connection, nil
}

// persistConnectionIDs persists connection IDs to pipelines table
func (w *ConnectionValidatorWorker) persistConnectionIDs(ctx context.Context, pipelineID, sourceID, destID string) {
	query := `
		UPDATE pipelines 
		SET source_connection_id = $1, destination_connection_id = $2, updated_at = NOW()
		WHERE id = $3
	`
	_, err := w.db.ExecContext(ctx, query, sourceID, destID, pipelineID)
	if err != nil {
		log.WithError(err).Warn("Failed to persist connection IDs to pipelines table")
	} else {
		log.WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"source_id":   sourceID,
			"dest_id":     destID,
		}).Info("💾 Persisted connection IDs to pipelines table")
	}
}

// emitConnectionError emits a HITL event when a selected connection is invalid
func (w *ConnectionValidatorWorker) emitConnectionError(ctx context.Context, assignment Task, direction, connectionID string, err error) (TaskResult, error) {
	errorMsg := fmt.Sprintf("Selected %s connection is no longer valid: %v", direction, err)

	_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "PIPELINE_WAITING",
		PipelineID:  assignment.PipelineID,
		ExecutionID: getExecutionID(assignment),
		Stage:       "connection_validator",
		Status:      "waiting_for_user",
		Message:     errorMsg,
		Summary:     "Connection error - reselection required",
		Progress: ProgressInfo{
			Percent:     30,
			CurrentStep: 3,
			TotalSteps:  7,
			Stage:       "connection_validator",
		},
		BlockingReason: &BlockingReason{
			Type:        "user_input_required",
			Description: errorMsg,
			Details: map[string]interface{}{
				"request_type":          "connection_reselect",
				"error":                 err.Error(),
				"invalid_connection_id": connectionID,
				"direction":             direction,
				"require_reselection":   true,
			},
		},
	})

	return TaskResult{
		TaskID:      assignment.TaskID,
		WorkflowID:  assignment.WorkflowID,
		StepID:      assignment.StepID,
		Status:      "needs_input",
		Error:       errorMsg,
		CompletedAt: time.Now(),
		TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
	}, nil
}

// getConnectorCandidates returns all possible connector type names for a given type
func getConnectorCandidates(connectorType string) []string {
	normalized := strings.ToLower(strings.TrimSpace(connectorType))
	normalized = strings.ReplaceAll(normalized, " ", "-")

	candidates := []string{normalized}
	seen := map[string]bool{normalized: true}

	// Add hyphen/underscore variants
	if !seen[strings.ReplaceAll(normalized, "-", "_")] {
		variant := strings.ReplaceAll(normalized, "-", "_")
		candidates = append(candidates, variant)
		seen[variant] = true
	}
	if !seen[strings.ReplaceAll(normalized, "_", "-")] {
		variant := strings.ReplaceAll(normalized, "_", "-")
		candidates = append(candidates, variant)
		seen[variant] = true
	}

	// Add known aliases
	if aliases, ok := connectorAliases[normalized]; ok {
		for _, alias := range aliases {
			if !seen[alias] {
				candidates = append(candidates, alias)
				seen[alias] = true
			}
		}
	}

	return candidates
}

// listActiveConnections returns all active connections for a user matching any of the connector candidates
func (w *ConnectionValidatorWorker) listActiveConnections(ctx context.Context, userID string, connectorCandidates []string, connectionType string) ([]map[string]interface{}, error) {
	if len(connectorCandidates) == 0 {
		return nil, nil
	}

	// Build IN clause for SQL
	placeholders := make([]string, len(connectorCandidates))
	args := []interface{}{userID}
	for i, candidate := range connectorCandidates {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, candidate)
	}
	args = append(args, connectionType)

	query := fmt.Sprintf(`
		SELECT id, name, connector_type, type, config, status
		FROM connections
		WHERE user_id = $1 
		AND LOWER(connector_type) IN (%s)
		AND type = $%d
		AND status = 'active'
		ORDER BY updated_at DESC
	`, strings.Join(placeholders, ","), len(args))

	rows, err := w.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var connections []map[string]interface{}
	for rows.Next() {
		var id, name, connectorType, connType, encryptedConfig, status string
		err := rows.Scan(&id, &name, &connectorType, &connType, &encryptedConfig, &status)
		if err != nil {
			log.WithError(err).Warn("Failed to scan connection row")
			continue
		}

		// Decrypt config
		decryptedJSON, err := crypto.DecryptString(encryptedConfig)
		if err != nil {
			log.WithError(err).Warn("Failed to decrypt connection config")
			continue
		}

		connections = append(connections, map[string]interface{}{
			"id":             id,
			"name":           name,
			"connector_type": connectorType,
			"type":           connType,
			"config":         decryptedJSON,
			"status":         status,
		})
	}

	return connections, nil
}

// tryAutoSelect attempts to auto-select connections if exactly one viable option exists for each side
func (w *ConnectionValidatorWorker) tryAutoSelect(ctx context.Context, assignment Task, userID, sourceType, destType string, sourceConn, destConn map[string]interface{}) (bool, TaskResult) {
	// Get candidates for each connector type
	sourceCandidates := getConnectorCandidates(sourceType)
	destCandidates := getConnectorCandidates(destType)

	log.WithFields(log.Fields{
		"pipeline_id":       assignment.PipelineID,
		"source_type":       sourceType,
		"dest_type":         destType,
		"source_candidates": sourceCandidates,
		"dest_candidates":   destCandidates,
	}).Info("🔍 Checking for auto-select candidates")

	// List active connections for each side
	var sourceConnections []map[string]interface{}
	var destConnections []map[string]interface{}
	var err error

	if sourceConn == nil {
		sourceConnections, err = w.listActiveConnections(ctx, userID, sourceCandidates, "source")
		if err != nil {
			log.WithError(err).Warn("Failed to list source connections")
			return false, TaskResult{}
		}
	} else {
		sourceConnections = []map[string]interface{}{sourceConn}
	}

	if destConn == nil {
		destConnections, err = w.listActiveConnections(ctx, userID, destCandidates, "destination")
		if err != nil {
			log.WithError(err).Warn("Failed to list destination connections")
			return false, TaskResult{}
		}
	} else {
		destConnections = []map[string]interface{}{destConn}
	}

	log.WithFields(log.Fields{
		"pipeline_id":     assignment.PipelineID,
		"source_count":    len(sourceConnections),
		"dest_count":      len(destConnections),
	}).Info("📊 Auto-select check results")

	// Only auto-select if EXACTLY one option for each side
	if len(sourceConnections) == 1 && len(destConnections) == 1 {
		selectedSource := sourceConnections[0]
		selectedDest := destConnections[0]

		log.WithFields(log.Fields{
			"pipeline_id": assignment.PipelineID,
			"source_id":   selectedSource["id"],
			"source_name": selectedSource["name"],
			"dest_id":     selectedDest["id"],
			"dest_name":   selectedDest["name"],
		}).Info("🎯 Auto-selected connections (only one viable option each)")

		// Validate the auto-selected connections
		actualSourceType := selectedSource["connector_type"].(string)
		actualDestType := selectedDest["connector_type"].(string)

		sourceValid, sourceError := w.validateConnection(ctx, actualSourceType, getConnectorVersionFromTask(assignment, "source"), selectedSource)
		destValid, destError := w.validateConnection(ctx, actualDestType, getConnectorVersionFromTask(assignment, "destination"), selectedDest)

		if !sourceValid || !destValid {
			log.WithFields(log.Fields{
				"pipeline_id":   assignment.PipelineID,
				"source_valid":  sourceValid,
				"source_error":  sourceError,
				"dest_valid":    destValid,
				"dest_error":    destError,
			}).Warn("⚠️  Auto-selected connections failed validation")
			return false, TaskResult{}
		}

		// Persist connection IDs
		sourceID := selectedSource["id"].(string)
		destID := selectedDest["id"].(string)
		w.persistConnectionIDs(ctx, assignment.PipelineID, sourceID, destID)

		_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_COMPLETED",
			PipelineID:  assignment.PipelineID,
			ExecutionID: getExecutionID(assignment),
			Stage:       "connection_validator",
			Status:      "processing",
			Message:     fmt.Sprintf("Auto-selected %s and %s", selectedSource["name"], selectedDest["name"]),
			Summary:     "Connections auto-selected",
			Progress: ProgressInfo{
				Percent:     30,
				CurrentStep: 3,
				TotalSteps:  7,
				Stage:       "connection_validator",
			},
			Metadata: map[string]interface{}{
				"source_connection_id":      sourceID,
				"destination_connection_id": destID,
				"action_taken":              "auto_selected",
			},
		})

		return true, TaskResult{
			TaskID:     assignment.TaskID,
			WorkflowID: assignment.WorkflowID,
			StepID:     assignment.StepID,
			Status:     "success",
			Output: map[string]interface{}{
				"source_connection_id":      sourceID,
				"destination_connection_id": destID,
				"source_connection":         selectedSource,
				"destination_connection":    selectedDest,
				"source_valid":              true,
				"destination_valid":         true,
				"action_taken":              "auto_selected",
			},
			NextAction:  "discover_schema",
			CompletedAt: time.Now(),
			TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		}
	}

	log.WithFields(log.Fields{
		"pipeline_id":  assignment.PipelineID,
		"source_count": len(sourceConnections),
		"dest_count":   len(destConnections),
	}).Info("❌ Auto-select not possible (not exactly 1 option for each side)")

	return false, TaskResult{}
}

// emitConnectionConfigHITL emits a V2-friendly HITL event for connection configuration
func (w *ConnectionValidatorWorker) emitConnectionConfigHITL(ctx context.Context, assignment Task, sourceType, destType string, sourceConn, destConn map[string]interface{}) (TaskResult, error) {
	// Build V2 HITL shape
	missingConnections := []map[string]interface{}{}
	requiredConnections := map[string]interface{}{
		"source": map[string]interface{}{
			"connector_type": sourceType,
		},
		"destination": map[string]interface{}{
			"connector_type": destType,
		},
	}

	missingDirection := "both"
	if sourceConn == nil {
		missingConnections = append(missingConnections, map[string]interface{}{
			"direction": "source",
			"type":      sourceType,
		})
		if destConn != nil {
			missingDirection = "source"
		}
	}
	if destConn == nil {
		missingConnections = append(missingConnections, map[string]interface{}{
			"direction": "destination",
			"type":      destType,
		})
		if sourceConn != nil {
			missingDirection = "destination"
		}
	}

	log.WithFields(log.Fields{
		"pipeline_id":         assignment.PipelineID,
		"missing_connections": missingConnections,
		"missing_direction":   missingDirection,
	}).Info("⏸️  Emitting HITL for connection configuration")

	_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "PIPELINE_WAITING",
		PipelineID:  assignment.PipelineID,
		ExecutionID: getExecutionID(assignment),
		Stage:       "connection_validator",
		Status:      "waiting_for_user",
		Message:     "Please configure connections to continue",
		Summary:     "Waiting for connection configuration",
		Progress: ProgressInfo{
			Percent:     30,
			CurrentStep: 3,
			TotalSteps:  7,
			Stage:       "connection_validator",
		},
		BlockingReason: &BlockingReason{
			Type:        "user_input_required",
			Description: "Please configure the missing connection(s) to continue.",
			Details: map[string]interface{}{
				"request_type":          "connection_config",
				"required_connections":  requiredConnections,
				"missing_connections":   missingConnections,
				// Legacy fields for backward compatibility
				"missing_direction":     missingDirection,
				"source_connector":      sourceType,
				"destination_connector": destType,
			},
		},
		Metadata: map[string]interface{}{
			"required_connections": requiredConnections,
			"missing_connections":  missingConnections,
			"missing_direction":    missingDirection,
			"source_connector":     sourceType,
			"destination_connector": destType,
		},
	})

	return TaskResult{
		TaskID:     assignment.TaskID,
		WorkflowID: assignment.WorkflowID,
		StepID:     assignment.StepID,
		Status:     "needs_input",
		Output: map[string]interface{}{
			"source_connector":       sourceType,
			"destination_connector":  destType,
			"source_connection":      sourceConn,
			"destination_connection": destConn,
			"missing_connections":    missingConnections,
			"requires_configuration": true,
		},
		NextAction:  "await_connection_config",
		CompletedAt: time.Now(),
		TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
	}, nil
}
