package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

// KafkaAdapter bridges Kafka messages to Temporal workflow signals
// Pattern: Agents emit results to Kafka → Adapter signals workflows
type KafkaAdapter struct {
	brokers        []string
	temporalClient client.Client
	consumerGroup  sarama.ConsumerGroup
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

// ServiceName is the identity this process presents to the broker.
//
// It becomes the default client.id, which is what a customer-managed cluster
// keys its logs, throttling and quota metrics off. Without it every rsync
// process shares one anonymous default, so a throttled cluster can tell neither
// our services apart nor ours from another tenant's. Exported because the
// producer in cmd/adapter builds its own sarama config: the adapter's consumer
// and producer are the same process and must not present two identities.
// KAFKA_CLIENT_ID still overrides it.
const ServiceName = "temporal-adapter"

// consumerGroupID is the adapter's only consumer group. Named once so the id and its
// qualification cannot drift apart at the single call site that joins it.
const consumerGroupID = "temporal-adapter-consumer"

// ConsumerGroupID returns the group id this process actually joins.
//
// Exported for the same reason NewConsumerConfig is: NewKafkaAdapter dials a broker, so
// this is the only way to assert the joined identity without one. NewKafkaAdapter calls
// THIS function rather than requalifying inline -- a test that re-derived
// kafkaclient.Group(consumerGroupID) itself would assert the constant and keep passing
// with the constructor reverted to the bare literal, which is the exact regression it
// would exist to catch.
func ConsumerGroupID() string {
	return kafkaclient.Group(consumerGroupID)
}

// NewConsumerConfig builds everything the consumer group needs except the
// connection.
//
// Exported and split out so the identity this process presents can be asserted
// without a live broker -- NewKafkaAdapter dials one. Tests that re-derive the
// config instead of calling this are tautological: they pass even when this
// function stops naming the service, which is exactly the regression that
// matters.
func NewConsumerConfig(brokers string) (*sarama.Config, kafkaclient.Config, error) {
	security, err := kafkaclient.FromEnvForService(ServiceName, brokers)
	if err != nil {
		return nil, kafkaclient.Config{}, fmt.Errorf("invalid Kafka security configuration: %w", err)
	}
	security = security.WithBrokers(brokers)

	config := sarama.NewConfig()
	config.Version = sarama.V3_4_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Split, not wrapped: []string{brokers} made a multi-broker CSV a single
	// unresolvable host. Apply is a strict no-op for PLAINTEXT.
	if err := saramaauth.Apply(config, security); err != nil {
		return nil, kafkaclient.Config{}, fmt.Errorf("invalid Kafka security configuration: %w", err)
	}
	return config, security, nil
}

// NewKafkaAdapter creates a new Kafka adapter
func NewKafkaAdapter(brokers string, temporalClient client.Client) (*KafkaAdapter, error) {
	config, security, err := NewConsumerConfig(brokers)
	if err != nil {
		return nil, err
	}
	brokerList := security.Brokers
	// Qualified for the same reason the topic on the Consume call is: group ids and
	// topics share one namespace so a customer grants both with a single PREFIXED ACL
	// (kafkaclient.Group is literally Topic -- shared/go/kafkaclient/groups.go:26-31).
	// Left bare, this one group sat outside the grant that covers everything else this
	// process touches, and the failure is silent in the worst way: JoinGroup is denied,
	// the Consume loop below logs and retries forever, the process stays up and healthy,
	// and agent.control.results is simply never drained -- so no agent result ever
	// signals its Temporal workflow and every pipeline hangs with nothing reported.
	//
	// Renaming the group is a one-time migration cost: Offsets.Initial is OffsetNewest,
	// so the qualified group joins at the tail on first start. Any agent result produced
	// during the upgrade gap is skipped rather than replayed, so deploy this when no
	// pipeline is mid-flight.
	consumer, err := sarama.NewConsumerGroup(brokerList, ConsumerGroupID(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaAdapter{
		brokers:        brokerList,
		temporalClient: temporalClient,
		consumerGroup:  consumer,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

// Start begins consuming agent results and signaling workflows
func (a *KafkaAdapter) Start(ctx context.Context) error {
	log.Info("🚀 Starting Kafka Adapter - Consuming agent results")

	a.wg.Add(1)
	// Consume agent results in background
	go func() {
		defer a.wg.Done()
		topics := kafkaclient.Topics("agent.control.results")
		handler := &agentResultHandler{
			temporalClient: a.temporalClient,
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := a.consumerGroup.Consume(ctx, topics, handler)
				if err != nil {
					log.Errorf("Error consuming agent results: %v", err)
				}
			}
		}
	}()

	log.Info("✅ Kafka Adapter started - listening to agent.control.results")
	return nil
}

// Stop stops the adapter
func (a *KafkaAdapter) Stop() {
	log.Info("🛑 Stopping Kafka Adapter")
	a.cancel()
	if a.consumerGroup != nil {
		_ = a.consumerGroup.Close()
	}
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Warn("Kafka Adapter stop timed out waiting for consumer loop")
	}
}

// agentResultHandler implements sarama.ConsumerGroupHandler
type agentResultHandler struct {
	temporalClient client.Client
}

func (h *agentResultHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *agentResultHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *agentResultHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var result map[string]interface{}
		if err := json.Unmarshal(message.Value, &result); err != nil {
			log.WithError(err).Error("Failed to unmarshal agent result")
			session.MarkMessage(message, "")
			continue
		}

		// Extract workflow ID (Temporal WorkflowID) with fallbacks.
		//
		// IMPORTANT:
		// - Some workers may emit workflow_id="", so treat empty string as missing.
		// - In our architecture, workflow IDs are expected to be unique per execution.
		//   When available, execution_id is the most reliable fallback.
		workflowID, _ := result["workflow_id"].(string)
		if workflowID == "" {
			// Prefer execution_id (new architecture: workflowID == execution_id)
			if execID, ok := result["execution_id"].(string); ok && execID != "" {
				workflowID = execID
			} else if pipelineID, ok := result["pipeline_id"].(string); ok && pipelineID != "" {
				// Backward compatibility for older workflow ID scheme
				workflowID = fmt.Sprintf("pipeline-%s", pipelineID)
			} else {
				log.Warn("Agent result missing workflow_id/execution_id/pipeline_id")
				session.MarkMessage(message, "")
				continue
			}
		}

		// Extract status to determine signal type
		status, _ := result["status"].(string)
		nextAction, _ := result["next_action"].(string)

		log.WithFields(log.Fields{
			"workflow_id": workflowID,
			"status":      status,
			"next_action": nextAction,
		}).Info("📨 Processing agent result")

		// Signal the workflow with the result
		signalName := "agent_result"
		if status == "needs_input" {
			// Special handling for HITL checkpoints
			if nextAction == "await_connector_decision" {
				signalName = "connector_decision_needed"
			} else if nextAction == "await_connection_decision" {
				signalName = "connection_decision_needed"
			}
		}

		err := h.temporalClient.SignalWorkflow(
			context.Background(),
			workflowID,
			"", // Run ID (empty means current run)
			signalName,
			result,
		)

		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"workflow_id": workflowID,
				"signal_name": signalName,
			}).Error("Failed to signal workflow")
		} else {
			log.WithFields(log.Fields{
				"workflow_id": workflowID,
				"signal_name": signalName,
			}).Info("✅ Signaled workflow with agent result")
		}

		session.MarkMessage(message, "")
	}
	return nil
}
