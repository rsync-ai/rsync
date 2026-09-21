package workflows

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// When a pipeline needs a connector that is not installed, the workflow asks the
// generator (llm-service tool-generator) to build it. Two answers cannot change
// on retry, so the activity must fail once and the user must see why instead of
// "Connector generation failed for <type>":
//
//   - no LLM set up: 503 {"error":"llm_not_configured","message":"Set up an LLM first: ..."}
//     (llm-service/src/utils/llm_gate.py; nested under "detail" when the route
//     has no handler registered);
//   - a refusal: 401 {"detail":"invalid_internal_secret"} from
//     require_internal_secret, 422 {"detail":"Cannot reliably generate ..."}
//     from the spec-required gate, 400 {"detail":"..."} for internal-only
//     connectors, or the community scaffold's 400 {"error":..., "error_message":...}.
//
// Every other generator failure stays retryable.

// The built-in sentence, used when the generator names no LLM message.
const llmGateSentence = "Set up an LLM first: add OPENAI_API_KEY (or another provider's key) to .env, " +
	"or set LLM_PROVIDER=ollama for a local model, then restart rsync."

// Wording the generator may send instead; it differs from the built-in sentence,
// so a test can tell a relayed message from the fallback.
const llmGateCustomSentence = "Set up an LLM first: set LLM_PROVIDER=ollama and start Ollama, then restart rsync."

// The spec-required gate's refusal (llm-service agents/integration.py).
const specGateRefusal = "Cannot reliably generate a connector for 'gcs' without a machine-readable contract. " +
	"Provide one of: an OpenAPI/Swagger spec (openapi_spec_url or openapi_spec), a GraphQL endpoint " +
	"(graphql_endpoint), API docs (docs_url / docs_text), or paste a sample request (curl). " +
	"Generating from the description alone produces unreliable, hallucinated endpoints."

// The community scaffold's refusal for a request with no OpenAPI document
// (llm-service lifecycle/scaffold_routes.py _unsupported_input + _refusal).
const scaffoldRefusal = "No OpenAPI document was supplied. Send the API's OpenAPI 3.x or Swagger 2.0 " +
	"specification inline as 'openapi_spec' (JSON or YAML). This service generates connectors " +
	"deterministically from that document alone. The hosted service at rsync.ai runs the agentic pipeline that does."

const scaffoldRefusalBody = `{"success":false,"status":"refused","connector_name":"gcs",` +
	`"error_message":"` + scaffoldRefusal + `","error_stage":"input_validation",` +
	`"suggestions":["Most APIs publish one at /openapi.json, /swagger.json or /v3/api-docs"],` +
	`"error":"` + scaffoldRefusal + `"}`

const generatorAuthSentence = "The connector generator did not accept this service's internal secret. " +
	"Set the same INTERNAL_SERVICE_SECRET for temporal-adapter and tool-generator, then restart both."

const generatorNoReasonSentence = "The connector generator refused to build a connector for gcs and gave no reason. " +
	"Check the tool-generator logs, then try again."

const testGeneratorSecret = "unit-test-generator-internal-secret"

type seenSecretHeader struct {
	present bool
	value   string
}

type llmGateGenerator struct {
	mu      sync.Mutex
	hits    int
	paths   []string
	secrets []seenSecretHeader
	status  int
	body    string
	// requiredSecret, when set, makes the stub act like require_internal_secret
	// with INTERNAL_SERVICE_SECRET set: 401 unless X-Internal-Secret matches.
	requiredSecret string
}

// newLLMGateGenerator serves every request with status and body, and points the
// generate-connector activity at it.
func newLLMGateGenerator(t *testing.T, status int, body string) *llmGateGenerator {
	t.Helper()
	return startLLMGateGenerator(t, &llmGateGenerator{status: status, body: body})
}

func startLLMGateGenerator(t *testing.T, g *llmGateGenerator) *llmGateGenerator {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values, present := r.Header["X-Internal-Secret"]
		g.mu.Lock()
		g.hits++
		g.paths = append(g.paths, r.Method+" "+r.URL.Path)
		g.secrets = append(g.secrets, seenSecretHeader{present: present, value: strings.Join(values, ",")})
		required := g.requiredSecret
		g.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// Like the real generator app: a body without a JSON Content-Type is not
		// read as the request model, so the request is refused with 422.
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"detail":[{"type":"model_attributes_type","loc":["body"],"msg":"Input should be a valid dictionary or object to extract fields from"}]}`))
			return
		}
		if required != "" && strings.TrimSpace(r.Header.Get("X-Internal-Secret")) != required {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"detail":"invalid_internal_secret"}`))
			return
		}
		w.WriteHeader(g.status)
		_, _ = w.Write([]byte(g.body))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("TOOL_GENERATOR_URL", srv.URL)
	return g
}

func (g *llmGateGenerator) requests() (int, []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.hits, append([]string(nil), g.paths...)
}

func (g *llmGateGenerator) secretHeaders() []seenSecretHeader {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]seenSecretHeader(nil), g.secrets...)
}

func runGenerateConnectorActivity(t *testing.T) error {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(GenerateConnectorActivityV2)
	_, err := env.ExecuteActivity(GenerateConnectorActivityV2, GenerateConnectorRequest{
		CorrelationID: "pipe-llm-1-exec-llm-1",
		ConnectorType: "gcs",
	})
	return err
}

// requireFailedOnce checks the activity called POST /v1/generate once and failed
// with a non-retryable ApplicationError of errType carrying want.
func requireFailedOnce(t *testing.T, gen *llmGateGenerator, err error, errType, want string) {
	t.Helper()
	hits, paths := gen.requests()
	if hits != 1 || paths[0] != "POST /v1/generate" {
		t.Fatalf("expected one POST /v1/generate, got %d requests %v (err %v)", hits, paths, err)
	}
	if err == nil {
		t.Fatal("expected the activity to fail")
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected an ApplicationError, got %T: %v", err, err)
	}
	if appErr.Type() != errType {
		t.Fatalf("error type = %q, want %s (%v)", appErr.Type(), errType, err)
	}
	if !appErr.NonRetryable() {
		t.Fatalf("a %s failure must not be retried: %v", errType, err)
	}
	if appErr.Message() != want {
		t.Fatalf("message = %q, want %q", appErr.Message(), want)
	}
}

func TestGenerateConnectorActivityV2_NoLLMFailsOnceWithTheSetupSentence(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "flat body",
			body: `{"error":"llm_not_configured","message":"` + llmGateSentence + `"}`,
			want: llmGateSentence,
		},
		{
			name: "nested under detail",
			body: `{"detail":{"error":"llm_not_configured","message":"` + llmGateSentence + `"}}`,
			want: llmGateSentence,
		},
		{
			name: "nested under detail with the generator's own wording",
			body: `{"detail":{"error":"llm_not_configured","message":"` + llmGateCustomSentence + `"}}`,
			want: llmGateCustomSentence,
		},
		{
			name: "flat body wins over an unrelated detail object",
			body: `{"error":"llm_not_configured","message":"` + llmGateCustomSentence + `","detail":{"error":"other","message":"ignored"}}`,
			want: llmGateCustomSentence,
		},
		{
			name: "no message falls back to the sentence",
			body: `{"error":"llm_not_configured"}`,
			want: llmGateSentence,
		},
		{
			name: "the generator's own wording is relayed trimmed",
			body: `{"error":"llm_not_configured","message":"  ` + llmGateCustomSentence + `  "}`,
			want: llmGateCustomSentence,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gen := newLLMGateGenerator(t, http.StatusServiceUnavailable, tc.body)
			err := runGenerateConnectorActivity(t)
			requireFailedOnce(t, gen, err, "llm_not_configured", tc.want)
		})
	}
}

// tool-generator requires X-Internal-Secret on /v1/generate whenever
// INTERNAL_SERVICE_SECRET is set, and checks it before the LLM gate.
func TestGenerateConnectorActivityV2_SendsTheInternalSecret(t *testing.T) {
	gateBody := `{"error":"llm_not_configured","message":"` + llmGateCustomSentence + `"}`

	t.Run("matching secret reaches the LLM gate", func(t *testing.T) {
		// Surrounding whitespace, as an env file can leave, is not sent.
		t.Setenv("INTERNAL_SERVICE_SECRET", "  "+testGeneratorSecret+"\n")
		gen := startLLMGateGenerator(t, &llmGateGenerator{
			status: http.StatusServiceUnavailable, body: gateBody, requiredSecret: testGeneratorSecret,
		})
		err := runGenerateConnectorActivity(t)
		requireFailedOnce(t, gen, err, "llm_not_configured", llmGateCustomSentence)
		if got := gen.secretHeaders(); len(got) != 1 || !got[0].present || got[0].value != testGeneratorSecret {
			t.Fatalf("X-Internal-Secret sent = %+v, want the trimmed secret", got)
		}
	})

	t.Run("a secret the generator does not accept is refused once", func(t *testing.T) {
		t.Setenv("INTERNAL_SERVICE_SECRET", "unit-test-some-other-secret")
		gen := startLLMGateGenerator(t, &llmGateGenerator{
			status: http.StatusServiceUnavailable, body: gateBody, requiredSecret: testGeneratorSecret,
		})
		err := runGenerateConnectorActivity(t)
		requireFailedOnce(t, gen, err, "connector_generation_refused", generatorAuthSentence)
	})

	t.Run("control: no secret set sends no header", func(t *testing.T) {
		t.Setenv("INTERNAL_SERVICE_SECRET", "")
		gen := newLLMGateGenerator(t, http.StatusServiceUnavailable, gateBody)
		err := runGenerateConnectorActivity(t)
		requireFailedOnce(t, gen, err, "llm_not_configured", llmGateCustomSentence)
		if got := gen.secretHeaders(); len(got) != 1 || got[0].present {
			t.Fatalf("X-Internal-Secret sent = %+v, want no header", got)
		}
	})
}

func TestGenerateConnectorActivityV2_RefusalFailsOnceWithItsReason(t *testing.T) {
	longReason := "The connector rendered from this specification did not validate: " + strings.Repeat("field x is required; ", 80)
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "401 bad internal secret", status: http.StatusUnauthorized, body: `{"detail":"invalid_internal_secret"}`, want: generatorAuthSentence},
		{name: "422 spec-required gate", status: http.StatusUnprocessableEntity, body: `{"detail":"` + specGateRefusal + `"}`, want: specGateRefusal},
		{name: "400 community scaffold refusal", status: http.StatusBadRequest, body: scaffoldRefusalBody, want: scaffoldRefusal},
		{
			name:   "400 internal-only connector",
			status: http.StatusBadRequest,
			body:   `{"detail":"Connector 'gcs' is internal-only and cannot be generated via this API. Set developer_mode=true only if you are intentionally regenerating internal connectors."}`,
			want:   "Connector 'gcs' is internal-only and cannot be generated via this API. Set developer_mode=true only if you are intentionally regenerating internal connectors.",
		},
		{name: "blank error_message falls through to detail", status: http.StatusBadRequest, body: `{"error_message":"   ","detail":"` + specGateRefusal + `"}`, want: specGateRefusal},
		{name: "line breaks and runs of spaces collapse", status: http.StatusBadRequest, body: `{"detail":"  Rendering refused:\n\tthe spec has   no paths.\n"}`, want: "Rendering refused: the spec has no paths."},
		{name: "422 request validation list has no sentence", status: http.StatusUnprocessableEntity, body: `{"detail":[{"loc":["body","api_name"],"msg":"field required","type":"value_error.missing"}]}`, want: generatorNoReasonSentence},
		{name: "400 without a JSON body", status: http.StatusBadRequest, body: `Bad Request`, want: generatorNoReasonSentence},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gen := newLLMGateGenerator(t, tc.status, tc.body)
			err := runGenerateConnectorActivity(t)
			requireFailedOnce(t, gen, err, "connector_generation_refused", tc.want)
		})
	}

	t.Run("a long reason is cut", func(t *testing.T) {
		gen := newLLMGateGenerator(t, http.StatusBadRequest, `{"detail":"`+longReason+`"}`)
		err := runGenerateConnectorActivity(t)
		collapsed := strings.Join(strings.Fields(longReason), " ")
		if utf8.RuneCountInString(collapsed) <= 1000 {
			t.Fatalf("fixture reason is %d runes; it must exceed the 1000-rune cap", utf8.RuneCountInString(collapsed))
		}
		requireFailedOnce(t, gen, err, "connector_generation_refused", string([]rune(collapsed)[:1000])+"...")
	})
}

func TestGenerateConnectorActivityV2_OtherFailuresStayRetryable(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "plain 503 while starting", status: http.StatusServiceUnavailable, body: `Service Unavailable`, want: "generator failed with status: 503"},
		{name: "503 with a detail string", status: http.StatusServiceUnavailable, body: `{"detail":"internal_secret_not_configured"}`, want: "generator failed with status: 503"},
		{name: "503 with another error code", status: http.StatusServiceUnavailable, body: `{"error":"overloaded","message":"try again later"}`, want: "generator failed with status: 503"},
		{name: "500 carrying the gate body", status: http.StatusInternalServerError, body: `{"error":"llm_not_configured","message":"` + llmGateSentence + `"}`, want: "generator failed with status: 500"},
		{name: "500 render failure with a refusal body", status: http.StatusInternalServerError, body: scaffoldRefusalBody, want: "generator failed with status: 500"},
		{name: "502 empty", status: http.StatusBadGateway, body: ``, want: "generator failed with status: 502"},
		{name: "403", status: http.StatusForbidden, body: `{"detail":"Forbidden"}`, want: "generator failed with status: 403"},
		{name: "404", status: http.StatusNotFound, body: `{"detail":"Not Found"}`, want: "generator failed with status: 404"},
		{name: "409", status: http.StatusConflict, body: `{"detail":"generation already running"}`, want: "generator failed with status: 409"},
		{name: "429", status: http.StatusTooManyRequests, body: `{"detail":"rate limited"}`, want: "generator failed with status: 429"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gen := newLLMGateGenerator(t, tc.status, tc.body)
			err := runGenerateConnectorActivity(t)

			if hits, paths := gen.requests(); hits != 1 {
				t.Fatalf("expected one generator request, got %d %v", hits, paths)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected an error containing %q, got %v", tc.want, err)
			}
			var appErr *temporal.ApplicationError
			if errors.As(err, &appErr) && (appErr.NonRetryable() || appErr.Type() == "llm_not_configured" || appErr.Type() == "connector_generation_refused") {
				t.Fatalf("a %d must stay retryable, got type %q non-retryable=%v", tc.status, appErr.Type(), appErr.NonRetryable())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The workflow: NLPipelineWorkflowV2 with a missing destination connector.
// ---------------------------------------------------------------------------

// The change ID histories carry. It is spelled out here, not taken from the
// constant, so renaming the constant's value (which would break replay of
// histories that carry the marker) fails these tests.
const connectorGenReasonChangeID = "connector-gen-user-facing-reason"

type llmGateStageFailure struct {
	stage        string
	errorMessage string
}

type llmGateStatusWrite struct {
	status string
	errMsg string
}

type llmGateHarness struct {
	mu                sync.Mutex
	availabilityCalls int
	genAttempts       int
	stageFailures     []llmGateStageFailure
	lastUpdate        StateUpdateInput
	updates           int
	statusWrites      []llmGateStatusWrite

	// genErr, when set, replaces the real generate activity with one that fails
	// with this error on every attempt.
	genErr error
}

func (h *llmGateHarness) intent(_ context.Context, _ NLPipelineWorkflowV2Input) (map[string]interface{}, error) {
	return map[string]interface{}{"source_type": "mongodb", "destination_type": "gcs"}, nil
}

func (h *llmGateHarness) resolver(_ context.Context, _ NLPipelineWorkflowV2Input, _ *WorkflowState) (map[string]interface{}, error) {
	return map[string]interface{}{
		"resolved_connectors": map[string]interface{}{"source": "mongodb", "destination": "gcs"},
	}, nil
}

func (h *llmGateHarness) availability(_ context.Context, req ConnectorCheckRequest) (*ConnectorCheckResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.availabilityCalls++
	return &ConnectorCheckResult{
		CorrelationID:            req.CorrelationID,
		SourceConnectorAvailable: true,
		MissingConnectors: []MissingConnector{
			{Type: req.DestinationType, Direction: "destination", CanGenerate: true},
		},
	}, nil
}

func (h *llmGateHarness) fakeGenerate(_ context.Context, _ GenerateConnectorRequest) (*GenerateConnectorResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.genAttempts++
	return nil, h.genErr
}

func (h *llmGateHarness) stateUpdate(_ context.Context, in StateUpdateInput) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.updates++
	h.lastUpdate = in
	if in.EventType == "STAGE_FAILED" {
		msg, _ := in.Metadata["error_message"].(string)
		h.stageFailures = append(h.stageFailures, llmGateStageFailure{stage: in.Stage, errorMessage: msg})
	}
	return nil
}

func (h *llmGateHarness) pipelineStatus(_ context.Context, _, _, status, errMsg string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statusWrites = append(h.statusWrites, llmGateStatusWrite{status: status, errMsg: errMsg})
	return nil
}

func (h *llmGateHarness) domainEvent(_ context.Context, _ map[string]interface{}) error {
	return nil
}

type llmGateRun struct {
	err               error
	availabilityCalls int
	genAttempts       int
	stageFailures     []llmGateStageFailure
	lastUpdate        StateUpdateInput
	updates           int
	statusWrites      []llmGateStatusWrite
}

func runLLMGatePipeline(t *testing.T, h *llmGateHarness, setup func(env *testsuite.TestWorkflowEnvironment)) llmGateRun {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(NLPipelineWorkflowV2, workflow.RegisterOptions{Name: NLPipelineWorkflowV2Name})
	var generate interface{} = GenerateConnectorActivityV2
	if h.genErr != nil {
		generate = h.fakeGenerate
	}
	for name, fn := range map[string]interface{}{
		"IntentActivityV2":                h.intent,
		"ConnectorResolverActivityV2":     h.resolver,
		"ConnectorAvailabilityActivityV2": h.availability,
		"GenerateConnectorActivityV2":     generate,
		"StateUpdateActivity":             h.stateUpdate,
		"UpdatePipelineStatusActivity":    h.pipelineStatus,
		"EmitDomainEventActivity":         h.domainEvent,
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	if setup != nil {
		setup(env)
	}

	env.ExecuteWorkflow(NLPipelineWorkflowV2, NLPipelineWorkflowV2Input{
		PipelineID:       "pipe-llm-1",
		ExecutionID:      "exec-llm-1",
		UserID:           "user-1",
		Message:          "sync my mongodb orders to gcs",
		ExecutorDispatch: ExecutorDispatchTemporal,
	})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	return llmGateRun{
		err:               env.GetWorkflowError(),
		availabilityCalls: h.availabilityCalls,
		genAttempts:       h.genAttempts,
		stageFailures:     append([]llmGateStageFailure(nil), h.stageFailures...),
		lastUpdate:        h.lastUpdate,
		updates:           h.updates,
		statusWrites:      append([]llmGateStatusWrite(nil), h.statusWrites...),
	}
}

// countReasonVersion answers GetVersion for the change ID with version 1, as a
// new history would, and counts how often the workflow asks.
func countReasonVersion(calls *int) func(env *testsuite.TestWorkflowEnvironment) {
	return func(env *testsuite.TestWorkflowEnvironment) {
		env.OnGetVersion(connectorGenReasonChangeID, workflow.DefaultVersion, 1).Return(
			func(string, workflow.Version, workflow.Version) workflow.Version {
				*calls++
				return 1
			})
	}
}

// requireReachedGeneration is the denominator: the run got to the connector
// check, found the destination missing and failed in connector generation.
func requireReachedGeneration(t *testing.T, r llmGateRun) {
	t.Helper()
	if r.availabilityCalls != 1 {
		t.Fatalf("connector availability ran %d times, want 1; the run never reached connector generation", r.availabilityCalls)
	}
	if r.err == nil {
		t.Fatal("expected the run to fail in connector generation")
	}
	if len(r.stageFailures) != 2 || r.stageFailures[0].stage != "connector_generation" || r.stageFailures[1].stage != "connector_check" {
		t.Fatalf("expected STAGE_FAILED for connector_generation then connector_check, got %+v", r.stageFailures)
	}
	if r.lastUpdate.EventType != "STAGE_FAILED" || r.lastUpdate.Stage != "connector_check" {
		t.Fatalf("the last state write must be the connector_check failure, got %s %s", r.lastUpdate.Stage, r.lastUpdate.EventType)
	}
	if len(r.statusWrites) != 1 || r.statusWrites[0].status != "failed" {
		t.Fatalf("expected exactly one terminal status write of \"failed\", got %+v", r.statusWrites)
	}
}

// requireUserSees checks what the chat and the pipeline row show: the
// api-gateway reads error_message from the last state write's metadata, and
// pipelines.error_message comes from the terminal status write.
func requireUserSees(t *testing.T, r llmGateRun, want string) {
	t.Helper()
	if got, _ := r.lastUpdate.Metadata["error_message"].(string); got != want {
		t.Fatalf("chat error_message = %q, want %q", got, want)
	}
	if r.stageFailures[0].errorMessage != want {
		t.Fatalf("connector_generation error_message = %q, want %q", r.stageFailures[0].errorMessage, want)
	}
	if r.statusWrites[0].errMsg != want {
		t.Fatalf("pipeline status error = %q, want %q", r.statusWrites[0].errMsg, want)
	}
}

// requireOldMessages checks the messages the workflow recorded before this change.
func requireOldMessages(t *testing.T, r llmGateRun, errType string) {
	t.Helper()
	if got, _ := r.lastUpdate.Metadata["error_message"].(string); got != "Connector generation failed for gcs" {
		t.Fatalf("chat error_message = %q, want the old message", got)
	}
	if got := r.stageFailures[0].errorMessage; !strings.Contains(got, "(type: "+errType) {
		t.Fatalf("connector_generation error_message = %q, want the raw activity error text", got)
	}
	if !strings.HasPrefix(r.statusWrites[0].errMsg, "Connector generation failed for gcs: ") {
		t.Fatalf("pipeline status error = %q, want the old reason", r.statusWrites[0].errMsg)
	}
}

func TestNLPipelineV2_ConnectorGenerationWithoutLLMTellsTheUserToSetOneUp(t *testing.T) {
	gen := newLLMGateGenerator(t, http.StatusServiceUnavailable, `{"error":"llm_not_configured","message":"`+llmGateCustomSentence+`"}`)
	versionCalls := 0
	r := runLLMGatePipeline(t, &llmGateHarness{}, countReasonVersion(&versionCalls))

	requireReachedGeneration(t, r)
	if hits, paths := gen.requests(); hits != 1 {
		t.Fatalf("the generator was called %d times %v; a missing LLM must not be retried", hits, paths)
	}
	if versionCalls != 1 {
		t.Fatalf("GetVersion(%q) asked %d times, want 1", connectorGenReasonChangeID, versionCalls)
	}
	requireUserSees(t, r, llmGateCustomSentence)

	var appErr *temporal.ApplicationError
	if !errors.As(r.err, &appErr) || appErr.Type() != "llm_not_configured" {
		t.Fatalf("workflow error should carry the llm_not_configured failure, got %v", r.err)
	}
}

func TestNLPipelineV2_ConnectorGenerationRefusedTellsTheUserWhy(t *testing.T) {
	gen := newLLMGateGenerator(t, http.StatusUnprocessableEntity, `{"detail":"`+specGateRefusal+`"}`)
	versionCalls := 0
	r := runLLMGatePipeline(t, &llmGateHarness{}, countReasonVersion(&versionCalls))

	requireReachedGeneration(t, r)
	if hits, paths := gen.requests(); hits != 1 {
		t.Fatalf("the generator was called %d times %v; a refusal must not be retried", hits, paths)
	}
	if versionCalls != 1 {
		t.Fatalf("GetVersion(%q) asked %d times, want 1", connectorGenReasonChangeID, versionCalls)
	}
	requireUserSees(t, r, specGateRefusal)

	var appErr *temporal.ApplicationError
	if !errors.As(r.err, &appErr) || appErr.Type() != "connector_generation_refused" {
		t.Fatalf("workflow error should carry the refusal, got %v", r.err)
	}
}

func TestNLPipelineV2_ConnectorGenerationPlain503IsRetriedAndReportedAsBefore(t *testing.T) {
	gen := newLLMGateGenerator(t, http.StatusServiceUnavailable, `Service Unavailable`)
	versionCalls := 0
	r := runLLMGatePipeline(t, &llmGateHarness{}, countReasonVersion(&versionCalls))

	requireReachedGeneration(t, r)
	if hits, paths := gen.requests(); hits != 3 {
		t.Fatalf("the generator was called %d times %v; a plain 503 must use all 3 attempts", hits, paths)
	}
	// Other failures record no version marker, so their histories stay as they were.
	if versionCalls != 0 {
		t.Fatalf("GetVersion(%q) asked %d times for a plain 503, want 0", connectorGenReasonChangeID, versionCalls)
	}
	if got, _ := r.lastUpdate.Metadata["error_message"].(string); got != "Connector generation failed for gcs" {
		t.Fatalf("chat error_message = %q", got)
	}
	if !strings.HasPrefix(r.statusWrites[0].errMsg, "Connector generation failed for gcs: ") {
		t.Fatalf("pipeline status error = %q", r.statusWrites[0].errMsg)
	}
}

// Histories recorded before the change have no version marker, so replay takes
// DefaultVersion and must emit the messages the old code recorded.
func TestNLPipelineV2_ConnectorGenerationKeepsOldMessagesOnDefaultVersion(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		errType string
	}{
		{name: "no LLM", status: http.StatusServiceUnavailable, body: `{"error":"llm_not_configured","message":"` + llmGateCustomSentence + `"}`, errType: "llm_not_configured"},
		{name: "refused", status: http.StatusUnprocessableEntity, body: `{"detail":"` + specGateRefusal + `"}`, errType: "connector_generation_refused"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gen := newLLMGateGenerator(t, tc.status, tc.body)
			r := runLLMGatePipeline(t, &llmGateHarness{}, func(env *testsuite.TestWorkflowEnvironment) {
				env.OnGetVersion(connectorGenReasonChangeID, workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
			})

			requireReachedGeneration(t, r)
			if hits, _ := gen.requests(); hits != 1 {
				t.Fatalf("the generator was called %d times, want 1", hits)
			}
			requireOldMessages(t, r, tc.errType)
		})
	}
}

// The retry policy itself stops on the type, so a retryable failure of either
// type (for example from a worker running older activity code) is not retried.
func TestNLPipelineV2_GenerateConnectorRetryPolicy(t *testing.T) {
	t.Run("llm_not_configured is not retried even when marked retryable", func(t *testing.T) {
		h := &llmGateHarness{genErr: temporal.NewApplicationError(llmGateCustomSentence, "llm_not_configured")}
		r := runLLMGatePipeline(t, h, nil)
		requireReachedGeneration(t, r)
		if r.genAttempts != 1 {
			t.Fatalf("generate ran %d times, want 1", r.genAttempts)
		}
		requireUserSees(t, r, llmGateCustomSentence)
	})
	t.Run("connector_generation_refused is not retried even when marked retryable", func(t *testing.T) {
		h := &llmGateHarness{genErr: temporal.NewApplicationError(scaffoldRefusal, "connector_generation_refused")}
		r := runLLMGatePipeline(t, h, nil)
		requireReachedGeneration(t, r)
		if r.genAttempts != 1 {
			t.Fatalf("generate ran %d times, want 1", r.genAttempts)
		}
		requireUserSees(t, r, scaffoldRefusal)
	})
	t.Run("llm_not_configured with no message still says how to set one up", func(t *testing.T) {
		h := &llmGateHarness{genErr: temporal.NewNonRetryableApplicationError("  ", "llm_not_configured", nil)}
		r := runLLMGatePipeline(t, h, nil)
		requireReachedGeneration(t, r)
		requireUserSees(t, r, llmGateSentence)
	})
	t.Run("a refusal with no message names the connector", func(t *testing.T) {
		h := &llmGateHarness{genErr: temporal.NewNonRetryableApplicationError("", "connector_generation_refused", nil)}
		r := runLLMGatePipeline(t, h, nil)
		requireReachedGeneration(t, r)
		requireUserSees(t, r, generatorNoReasonSentence)
	})
	t.Run("the relayed message is trimmed", func(t *testing.T) {
		h := &llmGateHarness{genErr: temporal.NewNonRetryableApplicationError("\n "+specGateRefusal+" \n", "connector_generation_refused", nil)}
		r := runLLMGatePipeline(t, h, nil)
		requireReachedGeneration(t, r)
		requireUserSees(t, r, specGateRefusal)
	})
	t.Run("control: another retryable type uses all attempts", func(t *testing.T) {
		h := &llmGateHarness{genErr: temporal.NewApplicationError("generator busy", "generator_busy")}
		versionCalls := 0
		r := runLLMGatePipeline(t, h, countReasonVersion(&versionCalls))
		requireReachedGeneration(t, r)
		if r.genAttempts != 3 {
			t.Fatalf("generate ran %d times, want 3", r.genAttempts)
		}
		if versionCalls != 0 {
			t.Fatalf("GetVersion(%q) asked %d times for another error type, want 0", connectorGenReasonChangeID, versionCalls)
		}
		if got, _ := r.lastUpdate.Metadata["error_message"].(string); got != "Connector generation failed for gcs" {
			t.Fatalf("chat error_message = %q", got)
		}
	})
}
