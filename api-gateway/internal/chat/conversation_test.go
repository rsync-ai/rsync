package chat

import (
	"testing"
)

// =============================================================================
// STATE MACHINE TESTS
// =============================================================================

func TestNewConversationContext(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	if conv.ID != "session-456" {
		t.Errorf("expected ID 'session-456', got '%s'", conv.ID)
	}
	if conv.UserID != "user-123" {
		t.Errorf("expected UserID 'user-123', got '%s'", conv.UserID)
	}
	if conv.State != StateIdle {
		t.Errorf("expected initial state 'idle', got '%s'", conv.State)
	}
	if conv.PendingIntent != nil {
		t.Errorf("expected PendingIntent to be nil initially")
	}
}

func TestConversationContext_SetState(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	conv.SetState(StateAwaitingSource)
	if conv.State != StateAwaitingSource {
		t.Errorf("expected state 'awaiting_source', got '%s'", conv.State)
	}

	conv.SetState(StateAwaitingDestination)
	if conv.State != StateAwaitingDestination {
		t.Errorf("expected state 'awaiting_destination', got '%s'", conv.State)
	}

	conv.SetState(StateAwaitingConfirmation)
	if conv.State != StateAwaitingConfirmation {
		t.Errorf("expected state 'awaiting_confirmation', got '%s'", conv.State)
	}
}

func TestConversationContext_SetPendingIntent(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	intent := &PendingIntent{
		Action:          "sync",
		SourceType:      "mysql",
		DestinationType: "s3",
	}
	conv.SetPendingIntent(intent)

	if conv.PendingIntent == nil {
		t.Fatal("expected PendingIntent to be set")
	}
	if conv.PendingIntent.SourceType != "mysql" {
		t.Errorf("expected source 'mysql', got '%s'", conv.PendingIntent.SourceType)
	}
	if conv.PendingIntent.DestinationType != "s3" {
		t.Errorf("expected destination 's3', got '%s'", conv.PendingIntent.DestinationType)
	}
}

func TestConversationContext_Reset(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	// Set up some state
	conv.SetState(StateAwaitingConfirmation)
	conv.SetPendingIntent(&PendingIntent{
		SourceType:      "mysql",
		DestinationType: "s3",
	})

	// Reset
	conv.Reset()

	if conv.State != StateIdle {
		t.Errorf("expected state to be 'idle' after reset, got '%s'", conv.State)
	}
	if conv.PendingIntent != nil {
		t.Errorf("expected PendingIntent to be nil after reset")
	}
}

func TestPendingIntent_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		intent   *PendingIntent
		expected bool
	}{
		{
			name: "valid intent with both source and dest",
			intent: &PendingIntent{
				SourceType:      "mysql",
				DestinationType: "s3",
			},
			expected: true,
		},
		{
			name: "invalid - missing destination",
			intent: &PendingIntent{
				SourceType: "mysql",
			},
			expected: false,
		},
		{
			name: "invalid - missing source",
			intent: &PendingIntent{
				DestinationType: "s3",
			},
			expected: false,
		},
		{
			name:     "invalid - empty",
			intent:   &PendingIntent{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.intent.IsValid(); got != tt.expected {
				t.Errorf("IsValid() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// =============================================================================
// CONNECTOR VALIDATION TESTS
// =============================================================================

func TestIsValidConnectorName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Valid connectors
		{"mysql", "mysql", true},
		{"postgresql", "postgresql", true},
		{"s3", "s3", true},
		{"snowflake", "snowflake", true},
		{"bigquery", "bigquery", true},

		// Stopwords - should be rejected
		{"want", "want", false},
		{"create", "create", false},
		{"Want uppercase", "Want", false},
		{"CREATE uppercase", "CREATE", false},
		{"setup", "setup", false},
		{"pipeline", "pipeline", false},
		{"sync", "sync", false},

		// Empty and whitespace
		{"empty string", "", false},
		{"whitespace only", "   ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidConnectorName(tt.input); got != tt.expected {
				t.Errorf("IsValidConnectorName(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNormalizeConnectorName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// Standard normalization
		{"s3", "aws-s3"},
		{"S3", "aws-s3"},
		{"amazon-s3", "aws-s3"},
		{"postgres", "postgresql"},
		{"POSTGRES", "postgresql"},
		{"mongo", "mongodb"},
		{"gcs", "google-cloud-storage"},
		{"bq", "bigquery"},
		{"es", "elasticsearch"},
		{"mssql", "sqlserver"},
		{"sql-server", "sqlserver"},

		// Already normalized
		{"mysql", "mysql"},
		{"postgresql", "postgresql"},
		{"mongodb", "mongodb"},
		{"aws-s3", "aws-s3"},

		// Passthrough for unknown
		{"custom-connector", "custom-connector"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := NormalizeConnectorName(tt.input); got != tt.expected {
				t.Errorf("NormalizeConnectorName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// =============================================================================
// CONFIRMATION PARSING TESTS
// =============================================================================

func TestParseConfirmation(t *testing.T) {
	tests := []struct {
		input    string
		expected ConfirmationResult
	}{
		// Yes patterns
		{"yes", ConfirmationYes},
		{"Yes", ConfirmationYes},
		{"YES", ConfirmationYes},
		{"y", ConfirmationYes},
		{"yeah", ConfirmationYes},
		{"yep", ConfirmationYes},
		{"sure", ConfirmationYes},
		{"ok", ConfirmationYes},
		{"okay", ConfirmationYes},
		{"go", ConfirmationYes},
		{"go ahead", ConfirmationYes},
		{"confirm", ConfirmationYes},
		{"do it", ConfirmationYes},
		{"proceed", ConfirmationYes},
		{"start", ConfirmationYes},
		{"run", ConfirmationYes},
		{"looks good", ConfirmationYes},
		{"lgtm", ConfirmationYes},

		// No patterns
		{"no", ConfirmationNo},
		{"No", ConfirmationNo},
		{"NO", ConfirmationNo},
		{"n", ConfirmationNo},
		{"nope", ConfirmationNo},
		{"nah", ConfirmationNo},
		{"cancel", ConfirmationNo},
		{"stop", ConfirmationNo},
		{"wait", ConfirmationNo},
		{"hold on", ConfirmationNo},
		{"wrong", ConfirmationNo},
		{"start over", ConfirmationNo},

		// Unclear patterns
		{"maybe", ConfirmationUnclear},
		{"hmm", ConfirmationUnclear},
		{"tell me more", ConfirmationUnclear},
		{"what does that mean", ConfirmationUnclear},
		{"mysql", ConfirmationUnclear}, // connector name, not yes/no
		// Edit verbs are deferred to the handler for an in-place pair edit (#4),
		// NOT treated as an outright cancel — and the edit must win over a yes/no
		// prefix ("okay change …", "no, change …").
		{"change", ConfirmationUnclear},
		{"change the destination to bigquery", ConfirmationUnclear},
		{"okay change dest to bigquery", ConfirmationUnclear},
		{"use bigquery instead", ConfirmationUnclear},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseConfirmation(tt.input); got != tt.expected {
				t.Errorf("ParseConfirmation(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// =============================================================================
// CONVERSATION FLOW TESTS
// =============================================================================

// Test: idle + "sync data to s3" → sets awaiting_source and stores pending dest
func TestConversationFlow_PartialIntent_DestOnly(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	// Simulate receiving "sync data to s3" - destination is valid, source is not
	// In real flow, handler would set this based on LLM response
	pendingIntent := &PendingIntent{
		Action:          "sync",
		DestinationType: "aws-s3", // normalized
	}
	conv.SetPendingIntent(pendingIntent)
	conv.SetState(StateAwaitingSource)

	if conv.State != StateAwaitingSource {
		t.Errorf("expected state 'awaiting_source', got '%s'", conv.State)
	}
	if conv.PendingIntent.DestinationType != "aws-s3" {
		t.Errorf("expected pending dest 'aws-s3', got '%s'", conv.PendingIntent.DestinationType)
	}
	if conv.PendingIntent.SourceType != "" {
		t.Errorf("expected pending source to be empty")
	}
}

// Test: awaiting_source + "mysql" → fills source and moves to awaiting_confirmation
func TestConversationFlow_FillSource_MovesToConfirmation(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	// Setup: already waiting for source, dest is known
	conv.SetPendingIntent(&PendingIntent{
		DestinationType: "aws-s3",
	})
	conv.SetState(StateAwaitingSource)

	// Simulate user saying "mysql"
	conv.PendingIntent.SourceType = "mysql"

	// Check if intent is now valid
	if !conv.PendingIntent.IsValid() {
		t.Error("expected intent to be valid after filling source")
	}

	// Move to confirmation
	conv.SetState(StateAwaitingConfirmation)

	if conv.State != StateAwaitingConfirmation {
		t.Errorf("expected state 'awaiting_confirmation', got '%s'", conv.State)
	}
}

// Test: awaiting_confirmation + "yes" → should trigger pipeline creation
func TestConversationFlow_Confirmation_Yes(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	// Setup: awaiting confirmation with full intent
	conv.SetPendingIntent(&PendingIntent{
		SourceType:      "mysql",
		DestinationType: "aws-s3",
	})
	conv.SetState(StateAwaitingConfirmation)

	// Parse "yes"
	result := ParseConfirmation("yes")
	if result != ConfirmationYes {
		t.Fatalf("expected ConfirmationYes, got %v", result)
	}

	// In real handler, this would create pipeline and reset
	// Simulate reset after pipeline creation
	conv.Reset()

	if conv.State != StateIdle {
		t.Errorf("expected state to be 'idle' after confirmation, got '%s'", conv.State)
	}
	if conv.PendingIntent != nil {
		t.Error("expected PendingIntent to be nil after confirmation")
	}
}

// Regression test: "I want to create a pipeline" should NOT become want/create connectors
func TestConversationFlow_Stopwords_NotExtracted(t *testing.T) {
	// These are common phrases that should NOT result in connector extraction
	stopwordPhrases := []string{
		"want",
		"create",
		"I want to create a pipeline",
		"setup new pipeline",
		"make data pipeline",
		"build sync",
	}

	for _, phrase := range stopwordPhrases {
		// Extract words and check none are valid connectors
		words := []string{"want", "create", "setup", "make", "build", "new", "pipeline", "sync", "data"}
		for _, word := range words {
			if IsValidConnectorName(word) {
				t.Errorf("word '%s' from phrase '%s' should NOT be a valid connector", word, phrase)
			}
		}
	}
}

// Test full flow: idle → awaiting_source → awaiting_destination → awaiting_confirmation → idle
func TestConversationFlow_FullFlow(t *testing.T) {
	conv := NewConversationContext("user-123", "session-456")

	// Step 1: Initial message with no clear connectors
	if conv.State != StateIdle {
		t.Errorf("expected initial state 'idle', got '%s'", conv.State)
	}

	// Step 2: User intent has no connectors, ask for source
	conv.SetPendingIntent(&PendingIntent{Action: "sync"})
	conv.SetState(StateAwaitingSource)
	if conv.State != StateAwaitingSource {
		t.Errorf("expected 'awaiting_source', got '%s'", conv.State)
	}

	// Step 3: User provides source "mysql"
	conv.PendingIntent.SourceType = "mysql"
	conv.SetState(StateAwaitingDestination)
	if conv.State != StateAwaitingDestination {
		t.Errorf("expected 'awaiting_destination', got '%s'", conv.State)
	}

	// Step 4: User provides destination "s3"
	conv.PendingIntent.DestinationType = "aws-s3"
	if !conv.PendingIntent.IsValid() {
		t.Error("expected valid intent after filling both slots")
	}
	conv.SetState(StateAwaitingConfirmation)
	if conv.State != StateAwaitingConfirmation {
		t.Errorf("expected 'awaiting_confirmation', got '%s'", conv.State)
	}

	// Step 5: User confirms
	result := ParseConfirmation("yes, go ahead")
	if result != ConfirmationYes {
		t.Errorf("expected ConfirmationYes, got %v", result)
	}

	// Reset after confirmation
	conv.Reset()
	if conv.State != StateIdle {
		t.Errorf("expected 'idle' after reset, got '%s'", conv.State)
	}
}
