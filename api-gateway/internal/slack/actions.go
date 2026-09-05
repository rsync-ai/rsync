// Package slack holds the shared pieces of the Slack drift-approval integration:
// the interactive-button action ids (shared between the outbound notifier and
// the inbound receiver), Slack request-signature verification, and a minimal
// Web API client for mapping a Slack user id to their verified email.
//
// This package deliberately contains NO business logic — the receiver handler
// (internal/handlers/slack_interactions.go) owns identity mapping,
// authorization, and the approval call. Keeping the crypto + the action-id
// constants here lets the notifier (outbound) and the handler (inbound) agree
// on the wire contract without a package cycle.
package slack

const (
	// ActionApproveSchemaChange / ActionRejectSchemaChange are the Block Kit
	// button action_ids. The notifier stamps them onto the outbound message and
	// the receiver matches on them — they MUST stay identical on both sides.
	ActionApproveSchemaChange = "approve_schema_change"
	ActionRejectSchemaChange  = "reject_schema_change"
)
