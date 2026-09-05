package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"api-gateway/internal/security"
	"api-gateway/internal/slack"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// SlackInteractionsHandler serves Slack Block Kit interactivity callbacks — the
// inbound side of the drift-approval Slack buttons.
//
// This endpoint is UNAUTHENTICATED at the gin layer (Slack carries no rsync
// session), so it defends itself in depth and FAILS CLOSED at every step:
//  1. Authenticity — the Slack request signature (HMAC over the raw body) is
//     verified before any field is trusted; a bad/absent/stale signature is
//     rejected outright.
//  2. Identity — the acting Slack user id is mapped to a verified email via
//     Slack users.info, then to an rsync user by email. No mapping ⇒ no action.
//  3. Authorization — the mapped user must hold ≥ member in THAT pipeline's
//     workspace (not a session's active workspace, which doesn't exist here).
//  4. Action — the approval runs through the SAME core the UI uses
//     (approveSchemaChangeCore), so the destructive-DDL guard and "no new
//     auto-apply" contract are preserved; nothing here can bypass them.
type SlackInteractionsHandler struct {
	db            *sql.DB
	kafka         KafkaProducer
	signingSecret string
	// resolveEmail maps a Slack user id to their Slack-verified email. It is nil
	// when no bot token is configured — in which case the handler verifies the
	// signature but refuses to approve (it cannot establish identity). Injectable
	// so tests don't hit the Slack API.
	resolveEmail func(ctx context.Context, slackUserID string) (string, error)
	// now is injectable for deterministic signature-window tests.
	now func() time.Time
}

// NewSlackInteractionsHandler wires the handler from env-provided credentials.
//   - signingSecret == "" disables the endpoint entirely (it responds
//     "not configured" and never mutates).
//   - botToken == "" leaves identity resolution unavailable, so the handler
//     verifies signatures but refuses to approve (fails closed).
func NewSlackInteractionsHandler(database *sql.DB, kafka KafkaProducer, signingSecret, botToken string) *SlackInteractionsHandler {
	h := &SlackInteractionsHandler{
		db:            database,
		kafka:         kafka,
		signingSecret: strings.TrimSpace(signingSecret),
		now:           time.Now,
	}
	if strings.TrimSpace(botToken) != "" {
		h.resolveEmail = slack.NewClient(strings.TrimSpace(botToken)).LookupUserEmail
	}
	return h
}

// slackApprovalAction is the minimal projection of a Slack block_actions payload
// that the drift-approval flow needs.
type slackApprovalAction struct {
	actionID    string
	slackUserID string
	pipelineID  string
}

// HandleInteractions is the POST /api/v1/slack/interactions endpoint.
func (h *SlackInteractionsHandler) HandleInteractions(c *gin.Context) {
	// (0) Disabled: without a signing secret we cannot verify authenticity, so
	// we trust nothing and do nothing. Respond benignly so a misconfigured app
	// surfaces the reason instead of retry-storming.
	if h.signingSecret == "" {
		c.JSON(http.StatusOK, slackEphemeral("Slack approvals are not configured on this server."))
		return
	}

	// (1) Read the RAW body verbatim — the HMAC is computed over these exact
	// bytes, so this must happen before any form parsing.
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "could not read request body"})
		return
	}

	// (2) Verify the Slack signature (authenticity + replay window).
	if err := slack.VerifyRequest(
		h.signingSecret,
		c.GetHeader("X-Slack-Request-Timestamp"),
		c.GetHeader("X-Slack-Signature"),
		body,
		h.now(),
	); err != nil {
		log.WithError(err).Warn("slack interactions: signature verification failed")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid signature"})
		return
	}

	// (3) Parse + validate the interaction payload.
	action, ok := parseSlackApprovalAction(body)
	if !ok {
		// Authentic Slack request we don't handle (or malformed action) — ack so
		// Slack doesn't retry, but do nothing.
		c.JSON(http.StatusOK, slackEphemeral("This action isn't something rsync-ai can handle."))
		return
	}

	// (4) Identity: Slack user id → verified email → rsync user.
	if h.resolveEmail == nil {
		c.JSON(http.StatusOK, slackEphemeral("Approving from Slack isn't available — this server has no Slack bot token to verify who you are. Open the change in rsync-ai to approve."))
		return
	}
	email, err := h.resolveEmail(c.Request.Context(), action.slackUserID)
	if err != nil || strings.TrimSpace(email) == "" {
		log.WithError(err).Warn("slack interactions: could not resolve actor email")
		c.JSON(http.StatusOK, slackEphemeral("Couldn't verify your Slack identity. Open the change in rsync-ai to approve."))
		return
	}
	userID, rsyncEmail, found, err := userByEmail(c.Request.Context(), h.db, email)
	if err != nil {
		c.JSON(http.StatusOK, slackEphemeral("Something went wrong verifying your account."))
		return
	}
	if !found {
		c.JSON(http.StatusOK, slackEphemeral("Your Slack email isn't linked to an rsync-ai account with approval rights."))
		return
	}

	// (5) Authorization: the mapped user must hold ≥ member in the PIPELINE'S
	// workspace. This is the IDOR/tenancy gate — a member of some other
	// workspace can never approve here.
	role, member, err := pipelineWorkspaceRoleForUser(c.Request.Context(), h.db, userID, action.pipelineID)
	if err != nil {
		c.JSON(http.StatusOK, slackEphemeral("Something went wrong checking your permissions."))
		return
	}
	if !member || !role.Meets(security.WSMember) {
		c.JSON(http.StatusOK, slackEphemeral("You don't have permission to approve schema changes for this pipeline."))
		return
	}

	// (6) Resolve the target change. One-click is unambiguous only when exactly
	// one change is pending; otherwise send the user to the app to disambiguate
	// (never guess which change to action).
	changeID, pending, err := singlePendingChange(c.Request.Context(), h.db, action.pipelineID)
	if err != nil {
		c.JSON(http.StatusOK, slackEphemeral("Something went wrong loading the pending change."))
		return
	}
	if pending == 0 {
		c.JSON(http.StatusOK, slackReplace("No schema change is pending for this pipeline — it may already have been handled."))
		return
	}
	if pending > 1 {
		c.JSON(http.StatusOK, slackEphemeral("Multiple schema changes are pending for this pipeline. Open rsync-ai to review them individually."))
		return
	}

	// (7) Act via the SAME core the UI uses.
	switch action.actionID {
	case slack.ActionApproveSchemaChange:
		actioned, dispatched, err := approveSchemaChangeCore(c.Request.Context(), h.db, h.kafka, action.pipelineID, changeID, rsyncEmail)
		if err != nil {
			log.WithError(err).Error("slack interactions: approve failed")
			c.JSON(http.StatusOK, slackEphemeral("Approval failed — please try again from rsync-ai."))
			return
		}
		if !actioned {
			c.JSON(http.StatusOK, slackReplace("That schema change was already actioned."))
			return
		}
		log.WithFields(log.Fields{"pipeline_id": action.pipelineID, "change_id": changeID, "by": rsyncEmail, "dispatched": dispatched}).Info("slack interactions: schema change approved")
		// Say which of the two things approval did. A bare "approved" on DDL nobody
		// will run reads as "handled" and the change quietly never happens.
		msg := "✅ Schema change *approved* by " + rsyncEmail + " via Slack."
		if !dispatched {
			msg = "✅ Schema change *approved* by " + rsyncEmail +
				" via Slack — decision recorded only. This DDL is *not auto-applied*; run it manually on the destination."
		}
		c.JSON(http.StatusOK, slackReplace(msg))
	case slack.ActionRejectSchemaChange:
		actioned, err := rejectSchemaChangeCore(c.Request.Context(), h.db, action.pipelineID, changeID, rsyncEmail)
		if err != nil {
			log.WithError(err).Error("slack interactions: reject failed")
			c.JSON(http.StatusOK, slackEphemeral("Rejection failed — please try again from rsync-ai."))
			return
		}
		if !actioned {
			c.JSON(http.StatusOK, slackReplace("That schema change was already actioned."))
			return
		}
		log.WithFields(log.Fields{"pipeline_id": action.pipelineID, "change_id": changeID, "by": rsyncEmail}).Info("slack interactions: schema change rejected")
		c.JSON(http.StatusOK, slackReplace("❌ Schema change *rejected* by "+rsyncEmail+" via Slack."))
	default:
		c.JSON(http.StatusOK, slackEphemeral("Unknown action."))
	}
}

// parseSlackApprovalAction extracts + validates the drift-approval action from a
// Slack block_actions payload (form-encoded `payload` field of JSON). Returns
// ok=false for any payload that isn't a well-formed approve/reject action with a
// UUID pipeline id in its value.
func parseSlackApprovalAction(body []byte) (slackApprovalAction, bool) {
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return slackApprovalAction{}, false
	}
	raw := form.Get("payload")
	if raw == "" {
		return slackApprovalAction{}, false
	}

	var p struct {
		Type string `json:"type"`
		User struct {
			ID string `json:"id"`
		} `json:"user"`
		Actions []struct {
			ActionID string `json:"action_id"`
			Value    string `json:"value"`
		} `json:"actions"`
	}
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return slackApprovalAction{}, false
	}
	if p.Type != "block_actions" || len(p.Actions) == 0 || strings.TrimSpace(p.User.ID) == "" {
		return slackApprovalAction{}, false
	}

	act := p.Actions[0]
	if act.ActionID != slack.ActionApproveSchemaChange && act.ActionID != slack.ActionRejectSchemaChange {
		return slackApprovalAction{}, false
	}
	pipelineID := strings.TrimSpace(act.Value)
	if _, err := uuid.Parse(pipelineID); err != nil {
		return slackApprovalAction{}, false
	}

	return slackApprovalAction{
		actionID:    act.ActionID,
		slackUserID: p.User.ID,
		pipelineID:  pipelineID,
	}, true
}

// userByEmail resolves an rsync user id + canonical email from an email address
// (case-insensitive). found=false means no such user — the caller must not act.
func userByEmail(ctx context.Context, database *sql.DB, email string) (userID, canonicalEmail string, found bool, err error) {
	err = database.QueryRowContext(ctx, `
		SELECT id::text, email FROM users WHERE lower(email) = lower($1)
	`, strings.TrimSpace(email)).Scan(&userID, &canonicalEmail)
	if err == sql.ErrNoRows {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	return userID, canonicalEmail, true, nil
}

// pipelineWorkspaceRoleForUser returns the user's role in the workspace that
// OWNS the pipeline. member=false means the pipeline doesn't exist or the user
// isn't a member of its workspace — either way, not authorized. Mirrors the
// pipelines⋈workspace_members join used by requireResourceRole, but keyed on the
// pipeline's own workspace instead of a session's active workspace.
func pipelineWorkspaceRoleForUser(ctx context.Context, database *sql.DB, userID, pipelineID string) (role security.WorkspaceRole, member bool, err error) {
	var raw string
	err = database.QueryRowContext(ctx, `
		SELECT wm.role
		FROM pipelines p
		JOIN workspace_members wm ON wm.workspace_id = p.workspace_id
		WHERE p.id = $1 AND wm.user_id = $2
	`, pipelineID, userID).Scan(&raw)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return security.WorkspaceRole(raw), true, nil
}

// singlePendingChange returns the newest pending change id for a pipeline plus
// the total pending count. The count lets the caller distinguish the
// unambiguous one-click case (exactly 1) from "nothing pending" (0) and
// "ambiguous, go to the app" (>1).
func singlePendingChange(ctx context.Context, database *sql.DB, pipelineID string) (changeID string, pending int, err error) {
	rows, err := database.QueryContext(ctx, `
		SELECT id::text FROM schema_change_approvals
		WHERE pipeline_id = $1 AND status = 'pending'
		ORDER BY created_at DESC
	`, pipelineID)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", 0, err
		}
		if pending == 0 {
			changeID = id // newest (ORDER BY created_at DESC)
		}
		pending++
	}
	if err := rows.Err(); err != nil {
		return "", 0, err
	}
	return changeID, pending, nil
}

// slackReplace replaces the original Slack message (both the actor and everyone
// in the channel see the outcome — the buttons are gone).
func slackReplace(text string) gin.H {
	return gin.H{"replace_original": true, "text": text}
}

// slackEphemeral replies only to the actor and leaves the original message (with
// its buttons) in place — used for "can't do that" outcomes so the change stays
// actionable by someone authorized.
func slackEphemeral(text string) gin.H {
	return gin.H{"response_type": "ephemeral", "replace_original": false, "text": text}
}
