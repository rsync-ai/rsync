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
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/intent"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/registry"
	"github.com/rsync-ai/shared/correlation"
)

// ==============================================================================
// CAPABILITY RESOLVER WORKER
// ==============================================================================
// This worker checks if connectors exist or can be generated.
// This is a clean HITL boundary: if connector is missing, pause and ask user.
//
// Responsibilities:
// 1. Check if source connector exists in connector registry
// 2. Check if destination connector exists in connector registry
// 3. Request connector generation if missing (HITL pause point)
// ==============================================================================

// CapabilityResolverWorker resolves connector capabilities
type CapabilityResolverWorker struct {
	kafkaManager      *kafka.Manager
	db                *sql.DB
	connectorRegistry *registry.ConnectorRegistry
	workspaceResolver *intent.WorkspaceContextResolver
	progressEmitter   *ProgressEmitter
	ctx               context.Context
	cancel            context.CancelFunc
	tracer            trace.Tracer
	correlationClient *correlation.Client
	workerID          string
}

// NewCapabilityResolverWorker creates a new capability resolver worker
func NewCapabilityResolverWorker(kafkaManager *kafka.Manager, db *sql.DB) *CapabilityResolverWorker {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize connector registry for querying available connectors
	connectorRegistry := registry.NewConnectorRegistry(db)

	// Initialize workspace context resolver for resolving workspace references
	workspaceResolver := intent.NewWorkspaceContextResolver(db, connectorRegistry)

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
		log.WithError(err).Warn("Failed to initialize correlation client for Capability Resolver worker")
	}

	return &CapabilityResolverWorker{
		kafkaManager:      kafkaManager,
		db:                db,
		connectorRegistry: connectorRegistry,
		workspaceResolver: workspaceResolver,
		progressEmitter:   NewProgressEmitter(kafkaManager, db),
		ctx:               ctx,
		cancel:            cancel,
		tracer:            otel.Tracer("capability-resolver-worker"),
		correlationClient: correlationClient,
		workerID:          fmt.Sprintf("capability-resolver-worker-%s", uuid.New().String()[:8]),
	}
}

// AgentName returns the worker name
func (w *CapabilityResolverWorker) AgentName() string {
	return "capability_resolver"
}

// Start begins consuming tasks
func (w *CapabilityResolverWorker) Start() error {
	log.Info("🚀 Starting Capability Resolver Worker")

	// Start Redis poller for V2 workflows (correlation pattern)
	if w.correlationClient != nil {
		go w.startRedisPollerCapability_resolver()
		log.Info("✅ CapabilityResolverWorker: Redis poller started for V2 correlation requests")
	} else {
		log.Warn("⚠️  CapabilityResolverWorker: Correlation client not initialized - V2 workflows will not work")
	}

	// Consume from dedicated topic (no consumer group = no rebalancing)
	err := w.kafkaManager.ConsumeWithContext("agent.control.commands.capability_resolver", w.handleTask)
	if err != nil {
		return fmt.Errorf("failed to consume agent.control.commands: %w", err)
	}

	log.Info("✅ Capability Resolver Worker started")
	return nil
}

// Stop stops the worker
func (w *CapabilityResolverWorker) Stop() {
	log.Info("🛑 Stopping Capability Resolver Worker")
	w.cancel()
	// Kafka consumer will be stopped via context cancellation
}

// handleTask processes incoming task assignments
func (w *CapabilityResolverWorker) handleTask(ctx context.Context, msg *sarama.ConsumerMessage) error {
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

	// Filter: only process capability resolution tasks
	if assignment.TaskType != "resolve_capability" {
		return nil // Not for us
	}

	log.WithFields(log.Fields{
		"pipeline_id": assignment.PipelineID,
		"task_id":     assignment.TaskID,
	}).Info("🔌 Processing capability resolution task")

	result, err := w.ProcessTask(ctx, assignment)
	if err != nil {
		log.WithError(err).Error("Capability resolution failed")
		result.Status = "failed"
		result.Error = err.Error()
	}

	// Route result to correlation store (V2) or Kafka (V1)
	return RouteResult(ctx, assignment, result, w.kafkaManager)
}

// ProcessTask implements the capability resolution logic
func (w *CapabilityResolverWorker) ProcessTask(ctx context.Context, assignment Task) (TaskResult, error) {
	ctx, span := w.tracer.Start(ctx, "capability_resolver.process_task",
		trace.WithAttributes(
			attribute.String("task_id", assignment.TaskID),
			attribute.String("pipeline_id", assignment.PipelineID),
		),
	)
	defer span.End()

	// Emit stage started (UI needs this to move past intent)
	_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  assignment.PipelineID,
		ExecutionID: getExecutionID(assignment),
		Stage:       "capability_resolver",
		Status:      "processing",
		Message:     "Checking connector availability",
		Summary:     "Checking connectors",
		Progress: ProgressInfo{
			Percent:     20,
			CurrentStep: 2,
			TotalSteps:  7,
			Stage:       "capability_resolver",
		},
	})

	// PHASE 2.4 FIX: First check if pipeline already has connection IDs
	// If so, fetch and return them immediately - no need to resolve
	var sourceConnID, destConnID string

	// Prefer connection IDs coming from the V2 Temporal correlation payload (avoids DB NULL scan issues).
	sourceConnID = getStringFromMap(assignment.Payload, "source_connection_id")
	destConnID = getStringFromMap(assignment.Payload, "destination_connection_id")
	if sourceConnID == "" {
		sourceConnID = getStringFromMap(assignment.Context, "source_connection_id")
	}
	if destConnID == "" {
		destConnID = getStringFromMap(assignment.Context, "destination_connection_id")
	}

	if sourceConnID != "" && destConnID != "" {
		log.WithFields(log.Fields{
			"pipeline_id":               assignment.PipelineID,
			"source_connection_id":      sourceConnID,
			"destination_connection_id": destConnID,
		}).Info("Using connection IDs from task payload/context")
	} else if assignment.PipelineID != "" {
		log.WithField("pipeline_id", assignment.PipelineID).Debug("Querying pipelines table for connection IDs")
		// Query pipeline table for connection IDs (nullable-safe)
		var sourceNS, destNS sql.NullString
		err := w.db.QueryRowContext(ctx, `
			SELECT source_connection_id, destination_connection_id 
			FROM pipelines 
			WHERE id = $1
		`, assignment.PipelineID).Scan(&sourceNS, &destNS)

		if err == nil {
			if sourceNS.Valid {
				sourceConnID = sourceNS.String
			}
			if destNS.Valid {
				destConnID = destNS.String
			}
		}
	}

	if sourceConnID != "" && destConnID != "" {
		log.WithFields(log.Fields{
			"pipeline_id":               assignment.PipelineID,
			"source_connection_id":      sourceConnID,
			"destination_connection_id": destConnID,
		}).Info("Pipeline already has connection IDs - using them directly")

		// Fetch connection details
		var sourceType, destType string
		_ = w.db.QueryRowContext(ctx, `SELECT connector_type FROM connections WHERE id = $1`, sourceConnID).Scan(&sourceType)
		_ = w.db.QueryRowContext(ctx, `SELECT connector_type FROM connections WHERE id = $1`, destConnID).Scan(&destType)

		// Return resolved connections
		availableConnectors := []string{}
		if sourceType != "" {
			availableConnectors = append(availableConnectors, sourceType)
		}
		if destType != "" && destType != sourceType {
			availableConnectors = append(availableConnectors, destType)
		}

		output := map[string]interface{}{
			"version": 2,
			"resolved_connectors": map[string]interface{}{
				"source":      sourceType,
				"destination": destType,
			},
			"source_connection_id":      sourceConnID,
			"destination_connection_id": destConnID,
			"missing_connectors":        []string{},
			"available_connectors":      availableConnectors,
			"action_taken":              "found_existing",
			"requires_user_action":      false,
		}

		_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_COMPLETED",
			PipelineID:  assignment.PipelineID,
			ExecutionID: getExecutionID(assignment),
			Stage:       "capability_resolver",
			Status:      "processing",
			Message:     "Connections resolved from pipeline",
			Summary:     "Connections ready",
			Progress: ProgressInfo{
				Percent:     25,
				CurrentStep: 2,
				TotalSteps:  7,
				Stage:       "capability_resolver",
			},
			Metadata: map[string]interface{}{
				"available_connectors": availableConnectors,
				"action_taken":         "found_existing",
			},
		})

		return TaskResult{
			TaskID:      assignment.TaskID,
			WorkflowID:  assignment.WorkflowID,
			StepID:      assignment.StepID,
			Status:      "success",
			Output:      output,
			NextAction:  "validate_connection",
			CompletedAt: time.Now(),
			TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		}, nil
	}

	// Extract parsed intent
	parsedIntent := assignment.Context["parsed_intent"]
	if parsedIntent == nil {
		return TaskResult{
			TaskID:      assignment.TaskID,
			WorkflowID:  assignment.WorkflowID,
			StepID:      assignment.StepID,
			Status:      "failed",
			Error:       "Missing parsed_intent in context",
			CompletedAt: time.Now(),
			TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		}, fmt.Errorf("missing parsed_intent")
	}

	parsedIntentMap, ok := parsedIntent.(map[string]interface{})
	if !ok {
		return TaskResult{
			TaskID:      assignment.TaskID,
			WorkflowID:  assignment.WorkflowID,
			StepID:      assignment.StepID,
			Status:      "failed",
			Error:       "Invalid parsed_intent format",
			CompletedAt: time.Now(),
			TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		}, fmt.Errorf("invalid parsed_intent format")
	}

	// Extract source/destination from intent (could be category or specific type)
	sourceCategory, _ := parsedIntentMap["source_category"].(string)
	destCategory, _ := parsedIntentMap["destination_category"].(string)

	// Try to get specific types if available
	sourceType, _ := parsedIntentMap["source_type"].(string)
	destinationType, _ := parsedIntentMap["destination_type"].(string)

	// --------------------------------------------------------------------------
	// Deterministic override: prefer explicit connector mentions in the user request.
	//
	// This avoids cases where the LLM intent parser picks a "default" connector (e.g. mysql)
	// even though the user explicitly typed "pipedrive to s3".
	// --------------------------------------------------------------------------
	userReq := getStringFromMap(assignment.Payload, "user_request")
	if userReq == "" {
		userReq = getStringFromMap(assignment.Payload, "request")
	}
	if userReq == "" {
		userReq = getStringFromMap(assignment.Context, "user_request")
	}
	if userReq == "" {
		userReq = getStringFromMap(assignment.Context, "request")
	}
	lowerReq := strings.ToLower(strings.TrimSpace(userReq))
	if lowerReq != "" {
		// Prefer explicit connector mentions (handles phrases with tables between connectors):
		// e.g. "Stream CDC from mysql table db.t into postgres table x".
		if src, dst, ok := extractExplicitSourceDest(lowerReq); ok {
			sourceType, sourceCategory = src, src
			destinationType, destCategory = dst, dst
		} else {
			// Source hints (only when explicitly used as a source)
			if strings.Contains(lowerReq, "pipedrive") {
				sourceType = "pipedrive"
				sourceCategory = "pipedrive"
			}
			if strings.Contains(lowerReq, "from mysql") || strings.Contains(lowerReq, "source mysql") {
				sourceType = "mysql"
				sourceCategory = "mysql"
			}
			if strings.Contains(lowerReq, "from postgres") || strings.Contains(lowerReq, "from postgresql") ||
				strings.Contains(lowerReq, "source postgres") || strings.Contains(lowerReq, "source postgresql") {
				sourceType = "postgresql"
				sourceCategory = "postgresql"
			}

			// Destination hints (only when explicitly used as a destination)
			// Treat "s3" as AWS S3 unless the user explicitly says "minio".
			if strings.Contains(lowerReq, "minio") {
				destinationType = "minio"
				destCategory = "minio"
			} else if strings.Contains(lowerReq, "aws s3") || strings.Contains(lowerReq, "s3") {
				destinationType = "aws-s3"
				destCategory = "aws-s3"
			}
			if strings.Contains(lowerReq, "to postgres") || strings.Contains(lowerReq, "into postgres") ||
				strings.Contains(lowerReq, "destination postgres") || strings.Contains(lowerReq, "destination postgresql") {
				destinationType = "postgresql"
				destCategory = "postgresql"
			}
		}
	}

	// WORKSPACE RESOLUTION: Resolve workspace references (production, staging, etc)
	// This runs BEFORE connector validation to prevent wrong-table selection
	workspaceID := getStringFromMap(assignment.Payload, "workspace_id")
	if workspaceID == "" {
		workspaceID = getStringFromMap(assignment.Context, "workspace_id")
	}
	if workspaceID == "" {
		// Try to get from user_id (for backward compatibility)
		workspaceID = assignment.UserID
	}

	if workspaceID != "" && w.workspaceResolver != nil {
		// Prefer sourceType over sourceCategory for resolution
		sourceRef := sourceType
		if sourceRef == "" {
			sourceRef = sourceCategory
		}
		destRef := destinationType
		if destRef == "" {
			destRef = destCategory
		}

		// Attempt workspace resolution
		rawIntent := intent.RawIntent{
			Source:      sourceRef,
			Destination: destRef,
			// Tables and filters can be added later
		}

		resolved, err := w.workspaceResolver.Resolve(ctx, rawIntent, workspaceID)
		if err != nil {
			// Check if it's a resolution error (not found or ambiguous)
			if resErr, ok := err.(*intent.ResolutionError); ok {
				log.WithFields(log.Fields{
					"pipeline_id": assignment.PipelineID,
					"error_type":  resErr.Type,
					"reference":   resErr.Reference,
					"context":     resErr.Context,
				}).Warn("⚠️  Workspace reference resolution failed")

				// Emit HITL event for connection selection
				metadata := map[string]interface{}{
					"error_type": resErr.Type,
					"reference":  resErr.Reference,
					"context":    resErr.Context,
				}

				if resErr.Type == "not_found" {
					metadata["suggestions"] = resErr.Suggestions
				} else if resErr.Type == "ambiguous" {
					metadata["options"] = resErr.Matches
				}

				_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
					EventType:   "HITL_REQUIRED",
					PipelineID:  assignment.PipelineID,
					ExecutionID: getExecutionID(assignment),
					Stage:       "capability_resolver",
					Status:      "waiting_for_user",
					Message:     resErr.Message,
					Summary:     "Action required",
					Metadata:    metadata,
				})

				// Return blocked result
				return TaskResult{
					TaskID:     assignment.TaskID,
					WorkflowID: assignment.WorkflowID,
					StepID:     assignment.StepID,
					Status:     "blocked",
					Error:      resErr.Message,
					Output: map[string]interface{}{
						"blocking_reason": "connection_config",
						"error_type":      resErr.Type,
						"reference":       resErr.Reference,
						"context":         resErr.Context,
						"suggestions":     resErr.Suggestions,
						"options":         resErr.Matches,
					},
					CompletedAt: time.Now(),
					TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
				}, nil
			}
			// Other errors - log and continue
			log.WithError(err).Warn("Workspace resolution failed with unexpected error")
		} else {
			// Resolution successful - use resolved connection IDs
			log.WithFields(log.Fields{
				"pipeline_id":                assignment.PipelineID,
				"source_connector_type":      resolved.SourceConnectorType,
				"source_connection_id":       resolved.SourceConnectionID,
				"destination_connector_type": resolved.DestinationConnectorType,
				"destination_connection_id":  resolved.DestinationConnectionID,
			}).Info("✅ Workspace reference resolution succeeded")

			// Override connector types with resolved types
			sourceType = resolved.SourceConnectorType
			destinationType = resolved.DestinationConnectorType
			sourceCategory = resolved.SourceConnectorType
			destCategory = resolved.DestinationConnectorType

			// Override connection IDs if resolved
			if resolved.SourceConnectionID != nil {
				sourceConnID = *resolved.SourceConnectionID
			}
			if resolved.DestinationConnectionID != nil {
				destConnID = *resolved.DestinationConnectionID
			}
		}
	}

	log.WithFields(log.Fields{
		"pipeline_id":      assignment.PipelineID,
		"source_category":  sourceCategory,
		"source_type":      sourceType,
		"dest_category":    destCategory,
		"destination_type": destinationType,
	}).Info("🔍 Checking connector availability via registry")

	// Check source connector
	sourceAvailable, sourceConnector := w.isConnectorAvailable(ctx, sourceType, sourceCategory)
	// Check destination connector
	destAvailable, destConnector := w.isConnectorAvailable(ctx, destinationType, destCategory)

	// If both available, proceed to next stage
	if sourceAvailable && destAvailable {
		log.WithField("pipeline_id", assignment.PipelineID).Info("✅ All connectors available")

		// IMPORTANT: Use the resolved connector types (not sourceType/destinationType which may be empty aliases).
		resolvedSourceType := sourceConnector
		resolvedDestType := destConnector

		availableConnectors := []string{}
		if sourceConnector != "" {
			availableConnectors = append(availableConnectors, sourceConnector)
		}
		if destConnector != "" && destConnector != sourceConnector {
			availableConnectors = append(availableConnectors, destConnector)
		}

		// Connection IDs are a UX/HITL boundary: only auto-select when there is EXACTLY one
		// active connection matching the resolved connector type. If there are multiple, leave
		// IDs empty so the workflow will ask the user to choose explicitly.
		if sourceConnID == "" || destConnID == "" {
			log.Info("🔍 Pipeline missing connection IDs, looking up by connector type (only if unambiguous)")

			// Use a small set of connector_type variants to tolerate hyphen/underscore differences.
			sourceCandidates := connectorTypeCandidates(resolvedSourceType)
			destCandidates := connectorTypeCandidates(resolvedDestType)

			// Find source connection ONLY if exactly one match exists
			if sourceConnID == "" {
				func() {
					rows, err := w.db.QueryContext(ctx, `
						SELECT id FROM connections
						WHERE (connector_type = $1 OR connector_type = $2 OR connector_type = $3)
						  AND type = 'source' AND status = 'active'
						  AND user_id = $4
						ORDER BY created_at DESC
						LIMIT 2
					`, sourceCandidates[0], sourceCandidates[1], sourceCandidates[2], assignment.UserID)
					if err != nil {
						log.WithError(err).Warn("⚠️  Failed to lookup source connections for type: " + resolvedSourceType)
						return
					}
					defer rows.Close()

					ids := []string{}
					for rows.Next() {
						var id string
						if err := rows.Scan(&id); err == nil && id != "" {
							ids = append(ids, id)
						}
					}
					if len(ids) == 1 {
						sourceConnID = ids[0]
						log.WithField("source_connection_id", sourceConnID).Info("✅ Auto-selected source connection (only one match)")
					} else if len(ids) > 1 {
						log.WithFields(log.Fields{
							"source_type":       resolvedSourceType,
							"source_candidates": sourceCandidates,
							"match_count":       len(ids),
						}).Info("⏸️ Multiple source connections found; requiring user selection")
					} else {
						log.Warn("⚠️  No active source connection found for type: " + resolvedSourceType)
					}
				}()
			}

			// Find destination connection ONLY if exactly one match exists
			if destConnID == "" {
				func() {
					rows, err := w.db.QueryContext(ctx, `
						SELECT id FROM connections
						WHERE (connector_type = $1 OR connector_type = $2 OR connector_type = $3)
						  AND type = 'destination' AND status = 'active'
						  AND user_id = $4
						ORDER BY created_at DESC
						LIMIT 2
					`, destCandidates[0], destCandidates[1], destCandidates[2], assignment.UserID)
					if err != nil {
						log.WithError(err).Warn("⚠️  Failed to lookup destination connections for type: " + resolvedDestType)
						return
					}
					defer rows.Close()

					ids := []string{}
					for rows.Next() {
						var id string
						if err := rows.Scan(&id); err == nil && id != "" {
							ids = append(ids, id)
						}
					}
					if len(ids) == 1 {
						destConnID = ids[0]
						log.WithField("destination_connection_id", destConnID).Info("✅ Auto-selected destination connection (only one match)")
					} else if len(ids) > 1 {
						log.WithFields(log.Fields{
							"destination_type":       resolvedDestType,
							"destination_candidates": destCandidates,
							"match_count":            len(ids),
						}).Info("⏸️ Multiple destination connections found; requiring user selection")
					} else {
						log.Warn("⚠️  No active destination connection found for type: " + resolvedDestType)
					}
				}()
			}
		}

		// STRICT: If either connection is still missing, this is a HITL boundary (do not proceed).
		if sourceConnID == "" || destConnID == "" {
			missing := []map[string]interface{}{}
			if sourceConnID == "" {
				missing = append(missing, map[string]interface{}{
					"direction":      "source",
					"connector_type": resolvedSourceType,
				})
			}
			if destConnID == "" {
				missing = append(missing, map[string]interface{}{
					"direction":      "destination",
					"connector_type": resolvedDestType,
				})
			}

			log.WithFields(log.Fields{
				"pipeline_id":         assignment.PipelineID,
				"missing_connections": missing,
			}).Warn("⏳ Missing connections - entering HITL")

			_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
				EventType:   "PIPELINE_WAITING",
				PipelineID:  assignment.PipelineID,
				ExecutionID: getExecutionID(assignment),
				Stage:       "connection_validator",
				Status:      "waiting_for_user",
				Message:     "Connections are missing — please configure them to continue",
				Summary:     "Waiting for connection configuration",
				Progress: ProgressInfo{
					Percent:     25,
					CurrentStep: 2,
					TotalSteps:  7,
					Stage:       "connection_validator",
				},
				BlockingReason: &BlockingReason{
					Type:        "connection_config",
					Description: "Configure the required source/destination connections to continue.",
					Details: map[string]interface{}{
						"missing_connections":   missing,
						"source_connector":      resolvedSourceType,
						"destination_connector": resolvedDestType,
					},
				},
				Metadata: map[string]interface{}{
					"missing_connections": missing,
				},
			})

			output := map[string]interface{}{
				"version": 2,
				"resolved_connectors": map[string]interface{}{
					"source":      resolvedSourceType,
					"destination": resolvedDestType,
				},
				"source_connection_id":      sourceConnID,
				"destination_connection_id": destConnID,
				"missing_connections":       missing,
				"requires_user_action":      true,
				"error_code":                "MISSING_CONNECTIONS",
			}

			return TaskResult{
				TaskID:      assignment.TaskID,
				WorkflowID:  assignment.WorkflowID,
				StepID:      assignment.StepID,
				Status:      "needs_input",
				Output:      output,
				NextAction:  "await_connection_configuration",
				CompletedAt: time.Now(),
				TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
			}, nil
		}

		_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_COMPLETED",
			PipelineID:  assignment.PipelineID,
			ExecutionID: getExecutionID(assignment),
			Stage:       "capability_resolver",
			Status:      "processing",
			Message:     "All required connectors are available",
			Summary:     "Connectors ready",
			Progress: ProgressInfo{
				Percent:     25,
				CurrentStep: 2,
				TotalSteps:  7,
				Stage:       "capability_resolver",
			},
			Metadata: map[string]interface{}{
				"source_connector":      sourceConnector,
				"destination_connector": destConnector,
				"available_connectors":  availableConnectors,
				"action_taken":          "found_existing",
			},
		})

		// V2 Contract (Phase F): Return structured resolution result WITH CONNECTION IDs
		output := map[string]interface{}{
			"version": 2,
			"resolved_connectors": map[string]interface{}{
				"source":      sourceConnector,
				"destination": destConnector,
			},
			"source_connection_id":      sourceConnID, // FIXED: Return real IDs
			"destination_connection_id": destConnID,   // FIXED: Return real IDs
			"missing_connectors":        []string{},
			"available_connectors":      availableConnectors,
			"action_taken":              "found_existing",
			"requires_user_action":      false,
			"source_capabilities":       w.getCapabilitiesSummary(sourceConnector),
			"dest_capabilities":         w.getCapabilitiesSummary(destConnector),
		}

		return TaskResult{
			TaskID:      assignment.TaskID,
			WorkflowID:  assignment.WorkflowID,
			StepID:      assignment.StepID,
			Status:      "success",
			Output:      output,
			NextAction:  "validate_connection", // Next: connection validation
			CompletedAt: time.Now(),
			TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
		}, nil
	}

	// If connectors missing, request generation (HITL)
	missingConnectors := []string{}
	missingDetailed := make([]map[string]interface{}, 0, 2)

	// Always include the "required" connectors for UI rendering, even if missing list is empty.
	requiredSource := strings.TrimSpace(sourceType)
	if requiredSource == "" {
		requiredSource = strings.TrimSpace(sourceCategory)
	}
	requiredDest := strings.TrimSpace(destinationType)
	if requiredDest == "" {
		requiredDest = strings.TrimSpace(destCategory)
	}

	requiredConnectors := map[string]interface{}{
		"source":      requiredSource,
		"destination": requiredDest,
	}

	if !sourceAvailable {
		if sourceType != "" {
			missingConnectors = append(missingConnectors, sourceType)
			missingDetailed = append(missingDetailed, map[string]interface{}{"direction": "source", "connector_type": sourceType})
		} else {
			missingConnectors = append(missingConnectors, sourceCategory)
			missingDetailed = append(missingDetailed, map[string]interface{}{"direction": "source", "connector_type": sourceCategory})
		}
	}
	if !destAvailable {
		if destinationType != "" {
			missingConnectors = append(missingConnectors, destinationType)
			missingDetailed = append(missingDetailed, map[string]interface{}{"direction": "destination", "connector_type": destinationType})
		} else {
			missingConnectors = append(missingConnectors, destCategory)
			missingDetailed = append(missingDetailed, map[string]interface{}{"direction": "destination", "connector_type": destCategory})
		}
	}

	log.WithFields(log.Fields{
		"pipeline_id":        assignment.PipelineID,
		"missing_connectors": missingConnectors,
	}).Info("⚠️  Connectors missing, requesting generation")

	// Emit pipeline waiting so UI can show HITL takeover
	_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "PIPELINE_WAITING",
		PipelineID:  assignment.PipelineID,
		ExecutionID: getExecutionID(assignment),
		Stage:       "capability_resolver",
		Status:      "waiting_for_user",
		Message:     "Missing connectors — need your approval to generate them",
		Summary:     "Waiting for connector decision",
		Progress: ProgressInfo{
			Percent:     25,
			CurrentStep: 2,
			TotalSteps:  7,
			Stage:       "capability_resolver",
		},
		BlockingReason: &BlockingReason{
			Type:        "connector_generation",
			Description: "Connectors are missing and must be generated before continuing.",
			Details: map[string]interface{}{
				"missing_connectors":    missingConnectors,
				"source_connector":      sourceConnector,
				"destination_connector": destConnector,
			},
		},
		Metadata: map[string]interface{}{
			"missing_connectors": missingConnectors,
		},
	})

	// V2 Contract (Phase F): Return structured resolution result with policy error
	output := map[string]interface{}{
		"version":                     2,
		"resolved_connectors":         map[string]interface{}{},
		"required_connectors":         requiredConnectors,
		"missing_connectors":          missingConnectors,
		"missing_connectors_detailed": missingDetailed,
		"requires_user_action":        true,
		"error_code":                  "MISSING_CONNECTORS",
	}

	// Add partial resolution if some connectors are available
	if sourceAvailable {
		output["resolved_connectors"].(map[string]interface{})["source"] = sourceConnector
	}
	if destAvailable {
		output["resolved_connectors"].(map[string]interface{})["destination"] = destConnector
	}

	return TaskResult{
		TaskID:      assignment.TaskID,
		WorkflowID:  assignment.WorkflowID,
		StepID:      assignment.StepID,
		Status:      "needs_input", // Trigger HITL
		Output:      output,
		NextAction:  "await_connector_decision", // Pause for HITL
		CompletedAt: time.Now(),
		TraceID:     trace.SpanFromContext(ctx).SpanContext().TraceID().String(),
	}, nil
}

// connectorTypeCandidates returns up to 3 variants of a connector type to tolerate
// hyphen/underscore naming differences across metadata and connection records.
func connectorTypeCandidates(t string) [3]string {
	t = strings.TrimSpace(strings.ToLower(t))
	if t == "" {
		return [3]string{"", "", ""}
	}
	v1 := t
	v2 := strings.ReplaceAll(t, "-", "_")
	v3 := strings.ReplaceAll(t, "_", "-")
	return [3]string{v1, v2, v3}
}

// isConnectorAvailable checks if a connector is available in the registry
// Returns (available, canonicalConnectorType)
//
// Note the canonical form: ``GetCapabilities`` may resolve aliases
// (e.g. "shopify" → "shopify-admin-graphql" via vendor-prefix). We
// return ``caps.ConnectorType`` so downstream code uses the right
// connector_type string when querying the connections table; using
// the raw input would query for non-existent rows and trigger an
// avoidable HITL/failure.
func (w *CapabilityResolverWorker) isConnectorAvailable(ctx context.Context, connectorType, category string) (bool, string) {
	// If specific type provided, check it directly
	if connectorType != "" {
		caps := w.connectorRegistry.GetCapabilities(connectorType)
		if caps != nil {
			log.WithFields(log.Fields{
				"connector":           connectorType,
				"canonical_connector": caps.ConnectorType,
				"category":            caps.ConnectorCategory,
			}).Info("✅ Connector found in registry")
			return true, caps.ConnectorType
		}
		log.WithField("connector", connectorType).Warn("⚠️  Connector not found in registry")
		return false, connectorType
	}

	// If only a category/alias provided (e.g. "s3"), resolve it via the in-memory registry cache.
	// This keeps the system generic even when DB tables are missing/outdated.
	if category != "" {
		if caps, ok := w.connectorRegistry.FindConnector(category); ok {
			log.WithFields(log.Fields{
				"query":     category,
				"connector": caps.ConnectorType,
				"display":   caps.DisplayName,
			}).Info("✅ Resolved connector via registry")
			return true, caps.ConnectorType
		}

		log.WithField("category", category).Warn("⚠️  No connector resolved for category")
		return false, category
	}

	return false, "unknown"
}

// getCapabilitiesSummary returns a summary of connector capabilities
func (w *CapabilityResolverWorker) getCapabilitiesSummary(connectorType string) map[string]interface{} {
	caps := w.connectorRegistry.GetCapabilities(connectorType)
	if caps == nil {
		return map[string]interface{}{
			"available": false,
		}
	}

	return map[string]interface{}{
		"display_name":         caps.DisplayName,
		"category":             caps.ConnectorCategory,
		"supports_source":      caps.SupportsSource,
		"supports_destination": caps.SupportsDestination,
		"operations":           len(caps.Operations),
	}
}
