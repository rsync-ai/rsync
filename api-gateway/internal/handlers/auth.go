package handlers

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/bcrypt"

	sharedcrypto "github.com/rsync-ai/shared/crypto"

	"api-gateway/internal/db"
	"api-gateway/internal/email"
)

// bcryptCost is the work factor for bcrypt. 12 rounds ≈ 350ms on a modern CPU —
// slow enough to resist offline dictionary attacks, fast enough for interactive login.
// DefaultCost (10) is the bare minimum; 12 is more appropriate for production.
const bcryptCost = 12

// emailClient is the package-level Resend sender.
// Configured from RESEND_API_KEY on first use (lazy, no constructor needed in main.go).
var emailClient = email.New()

// EmailConfigStatus exposes the mail client's configuration for a startup log line.
// emailClient is package-private and constructed at init, so main.go has no other way
// to report it — and an unreported half-configuration is the failure mode that costs
// an operator the most, since it looks exactly like working email until nobody can
// complete a signup.
func EmailConfigStatus() string { return emailClient.ConfigStatus() }

type AuthHandler struct {
	db *sql.DB
}

func NewAuthHandler() *AuthHandler {
	return &AuthHandler{
		db: db.GetDB(),
	}
}

func setAuthCookie(c *gin.Context, token string, expiresAt time.Time) {
	secure := cookieSecure()
	// Default SameSite=Lax (safe default; works for most self-hosted deployments).
	// Can be tightened later alongside CSRF protection.
	sameSite := http.SameSiteLaxMode

	maxAge := int(time.Until(expiresAt).Seconds())
	if maxAge < 0 {
		maxAge = 0
	}

	http.SetCookie(c.Writer, &http.Cookie{
		Name:     "auth_token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: sameSite,
		Expires:  expiresAt.UTC(),
		MaxAge:   maxAge,
	})
}

func clearAuthCookie(c *gin.Context) {
	secure := cookieSecure()
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     "auth_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Unix(0, 0).UTC(),
		MaxAge:   -1,
	})
}

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type RegisterRequest struct {
	Email         string `json:"email" binding:"required,email"`
	Password      string `json:"password" binding:"required,min=8"`
	Name          string `json:"name"`
	FirstName     string `json:"first_name"`
	LastName      string `json:"last_name"`
	Company       string `json:"company"`
	Phone         string `json:"phone"`
	Country       string `json:"country"`
	EmployeeRange string `json:"employee_range"`
}

type AuthResponse struct {
	UserID        string `json:"user_id"`
	Email         string `json:"email"`
	Name          string `json:"name"`
	Role          string `json:"role"`
	Status        string `json:"status"`
	Token         string `json:"token"`
	ExpiresAt     int64  `json:"expires_at"`
	EmailVerified bool   `json:"email_verified"`
}

// stampLastLogin records that this user just authenticated.
//
// Called from every path that mints a session -- Login and Register -- because
// this column is now the only durable record of a login. It replaced
// `MAX(sessions.created_at)`, which admin_users.go read as "last login" while
// four call sites deleted session rows underneath it (Logout, "sign out other
// devices", admin deactivate, admin delete), so signing out walked a user's last
// login backwards. See migration 092.
//
// Returns nothing on purpose. The stamp is reporting metadata; a user must not be
// denied a session because a bookkeeping UPDATE failed, and a void signature makes
// that invariant unrepresentable rather than merely tested. The error is logged
// rather than dropped so a persistent failure is still visible.
func stampLastLogin(database *sql.DB, userID string) {
	if database == nil {
		return
	}
	if _, err := database.Exec(`UPDATE users SET last_login_at = NOW() WHERE id = $1`, userID); err != nil {
		log.Warnf("auth: failed to stamp last_login_at for user %s: %v", userID, err)
	}
}

// Login handles user login
func (h *AuthHandler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Normalize input to avoid case/whitespace mismatches in DB lookups.
	// (UI input often includes trailing spaces; email comparison should be case-insensitive in practice.)
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))

	// Query user by email
	var userID, email, passwordHash, role, status, name string
	var emailVerified bool
	err := h.db.QueryRow(`
		SELECT id, email, password_hash, role, COALESCE(status, 'active'), COALESCE(name, ''),
		       COALESCE(email_verified, true)
		FROM users
		WHERE email = $1
	`, req.Email).Scan(&userID, &email, &passwordHash, &role, &status, &name, &emailVerified)

	if err == sql.ErrNoRows {
		// Audit: failed login attempt (user not found)
		logAudit(c, "login_failed", "auth", "", map[string]interface{}{
			"email":  req.Email,
			"reason": "user_not_found",
		})
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid email or password"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Check if account is deactivated
	if status == "deactivated" {
		logAudit(c, "login_failed", "auth", userID, map[string]interface{}{
			"email":  req.Email,
			"reason": "account_deactivated",
		})
		c.JSON(http.StatusForbidden, gin.H{"error": "Account has been deactivated"})
		return
	}

	// Verify password
	err = bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(req.Password))
	if err != nil {
		// Audit: failed login attempt (wrong password)
		logAudit(c, "login_failed", "auth", userID, map[string]interface{}{
			"email":  req.Email,
			"reason": "invalid_password",
		})
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid email or password"})
		return
	}

	// The token the client gets. uuid.New() is crypto/rand-backed, so this is 122
	// bits of unguessable value -- which is why the stored form only needs a single
	// SHA-256 rather than a KDF. See sharedcrypto.HashSessionToken.
	token := uuid.New().String()
	expiresAt := time.Now().Add(24 * time.Hour).Unix()

	// Store session in database
	_, err = h.db.Exec(`
		INSERT INTO sessions (id, user_id, token, expires_at, created_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (token) DO UPDATE SET expires_at = $4
	`, uuid.New().String(), userID, sharedcrypto.HashSessionToken(token), time.Unix(expiresAt, 0))

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create session"})
		return
	}

	stampLastLogin(h.db, userID)

	// Audit: successful login
	logAudit(c, "login_success", "auth", userID, map[string]interface{}{
		"email": email,
		"role":  role,
	})

	// Set HttpOnly auth cookie (preferred)
	setAuthCookie(c, token, time.Unix(expiresAt, 0))
	ensureCSRFCookie(c)

	// Return response
	c.JSON(http.StatusOK, AuthResponse{
		UserID:        userID,
		Email:         email,
		Name:          name,
		Role:          role,
		Status:        status,
		Token:         token,
		ExpiresAt:     expiresAt,
		EmailVerified: emailVerified,
	})
}

// Register handles user registration
// Workspace team-invite (workspace_invites) signup integration. These are DISTINCT
// from the instance-level `invitations` registration gate handled inline in
// Register: a workspace invite adds the new user to an existing team workspace with
// a workspace role; it does not gate registration or set the instance (users.role)
// role.

// Problem codes returned by validateWorkspaceInviteForSignup ("" = OK / no token).
const (
	wsInviteProblemNotFound      = "invalid_workspace_invite"
	wsInviteProblemUnusable      = "workspace_invite_unavailable"
	wsInviteProblemExpired       = "workspace_invite_expired"
	wsInviteProblemEmailMismatch = "workspace_invite_email_mismatch"
)

// validateWorkspaceInviteForSignup checks a workspace team-invite token BEFORE the
// account is created, so a bad or foreign invite rejects cleanly without leaving a
// half-provisioned user. token=="" is a no-op (returns "", nil). A non-empty
// problem string means reject registration; a non-nil error is a real DB failure.
// The email-bind is a hard check: the invite must be addressed to the signup email.
func validateWorkspaceInviteForSignup(database *sql.DB, token, signupEmail string) (string, error) {
	if token == "" {
		return "", nil
	}
	var inviteEmail, status string
	var expiresAt time.Time
	err := database.QueryRow(
		"SELECT lower(email), status, expires_at FROM workspace_invites WHERE token = $1",
		token,
	).Scan(&inviteEmail, &status, &expiresAt)
	if err == sql.ErrNoRows {
		return wsInviteProblemNotFound, nil
	}
	if err != nil {
		return "", err
	}
	if status != "pending" {
		return wsInviteProblemUnusable, nil
	}
	if time.Now().After(expiresAt) {
		return wsInviteProblemExpired, nil
	}
	if !strings.EqualFold(strings.TrimSpace(signupEmail), inviteEmail) {
		return wsInviteProblemEmailMismatch, nil
	}
	return "", nil
}

// acceptWorkspaceInviteAtSignup joins a just-registered user to the workspace named
// by a pending team-invite token, in one transaction. It mirrors
// AcceptWorkspaceInvite's guarantees: an atomic single-use claim (status='pending'
// AND not expired) with an email-bind re-check in the WHERE (defense in depth
// against a TOCTOU vs the up-front validation), then the membership insert. A claim
// that matches nothing — the invite was consumed/expired/email-drifted between
// validation and here — is a BENIGN no-join: it returns ("", "", nil) so the caller
// keeps the already-valid account (with just its personal workspace). A non-nil
// error is a tx/DB failure the caller should log.
func acceptWorkspaceInviteAtSignup(database *sql.DB, token, userID, userEmail string) (string, string, error) {
	tx, err := database.Begin()
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback() //nolint:errcheck

	var workspaceID, role string
	err = tx.QueryRow(
		`UPDATE workspace_invites SET status = 'accepted', accepted_at = NOW(), accepted_by = $1
		WHERE token = $2 AND status = 'pending' AND expires_at > NOW() AND lower(email) = lower($3)
		RETURNING workspace_id, role`,
		userID, token, userEmail,
	).Scan(&workspaceID, &role)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}

	if _, err := tx.Exec(
		`INSERT INTO workspace_members (workspace_id, user_id, role)
		VALUES ($1, $2, $3)
		ON CONFLICT (workspace_id, user_id) DO NOTHING`,
		workspaceID, userID, role,
	); err != nil {
		return "", "", err
	}

	if err := tx.Commit(); err != nil {
		return "", "", err
	}
	return workspaceID, role, nil
}

func (h *AuthHandler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Normalize input (must match Login normalization).
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))
	req.FirstName = strings.TrimSpace(req.FirstName)
	req.LastName = strings.TrimSpace(req.LastName)
	req.Company = strings.TrimSpace(req.Company)
	req.Phone = strings.TrimSpace(req.Phone)
	req.Country = strings.TrimSpace(req.Country)
	req.EmployeeRange = strings.TrimSpace(req.EmployeeRange)
	// Prefer split first/last name over legacy single name field.
	if req.FirstName != "" || req.LastName != "" {
		req.Name = strings.TrimSpace(req.FirstName + " " + req.LastName)
	} else {
		req.Name = strings.TrimSpace(req.Name)
	}

	// Check registration mode
	inviteToken := strings.TrimSpace(c.Query("invite"))
	registrationMode, err := getInstanceSetting(h.db, "registration_mode", "open")
	if err != nil {
		// We could not read the policy, so we do not know whether this instance
		// accepts self-service signups. Refuse rather than assume "open": the
		// failure mode that matters is a scoped one (instance_settings
		// unreadable while users stays writable), where guessing creates a real
		// account on an invite-only instance.
		log.Printf("register: cannot read registration_mode: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "registration_unavailable",
			"message": "Registration is temporarily unavailable. Please try again shortly.",
		})
		return
	}

	if registrationMode == "invite_only" && inviteToken == "" {
		c.JSON(http.StatusForbidden, gin.H{
			"error":   "invite_required",
			"message": "Registration is invite-only. Please use an invite link.",
		})
		return
	}

	// Validate invite token if provided
	var inviteRole string
	var inviteID string
	if inviteToken != "" {
		var expiresAt time.Time
		var usedAt sql.NullTime
		err := h.db.QueryRow(`
			SELECT id, role, expires_at, used_at
			FROM invitations
			WHERE token = $1
		`, inviteToken).Scan(&inviteID, &inviteRole, &expiresAt, &usedAt)

		if err == sql.ErrNoRows {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invalid_invite",
				"message": "Invitation not found",
			})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
		if usedAt.Valid {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invite_used",
				"message": "This invitation has already been used",
			})
			return
		}
		if time.Now().After(expiresAt) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "invite_expired",
				"message": "This invitation has expired",
			})
			return
		}
	}

	// Validate a workspace team invite (workspace_invites) up-front — distinct from
	// the instance-level `invitations` gate above. A bad/foreign/expired invite
	// rejects registration here so no half-provisioned account is created; the join
	// itself runs after the user + personal workspace exist.
	workspaceInviteToken := strings.TrimSpace(c.Query("workspace_invite"))
	if problem, vErr := validateWorkspaceInviteForSignup(h.db, workspaceInviteToken, req.Email); vErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	} else if problem == wsInviteProblemEmailMismatch {
		c.JSON(http.StatusForbidden, gin.H{
			"error":   problem,
			"message": "This workspace invite was sent to a different email address.",
		})
		return
	} else if problem != "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   problem,
			"message": "This workspace invite is not valid.",
		})
		return
	}

	// Check if user exists
	var exists bool
	err = h.db.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)", req.Email).Scan(&exists)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	if exists {
		c.JSON(http.StatusConflict, gin.H{"error": "Email already registered"})
		return
	}

	// Hash password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to hash password"})
		return
	}

	// Determine role.
	//
	// Default for self-service signup is "user" (the regular writer role),
	// not "viewer". Viewer is intended for read-only audit access — but
	// the create/run endpoints don't enforce that today, so labelling
	// every self-signup as "viewer" was a misleading lie about what the
	// account can actually do. Until RBAC is added on writes, default
	// signups should be tagged as the role that matches their effective
	// permissions. Invitations can still downgrade to "viewer" explicitly.
	role := "user"

	// First-user-is-admin: if no users exist, assign admin role
	var userCount int64
	if err := h.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount); err == nil && userCount == 0 {
		role = "admin"
	}

	// Invite role overrides default (but not first-user-admin)
	if inviteRole != "" && role != "admin" {
		role = inviteRole
	}

	// Email verification: generate a 32-byte (64 hex-char) cryptographically-random token.
	// Skip if RESEND_API_KEY is not set (self-hosted / air-gapped) or first admin.
	emailVerified := false
	var verifyToken sql.NullString
	var verifyExpires sql.NullTime
	resendConfigured := emailClient.IsConfigured()
	if !resendConfigured || userCount == 0 {
		// Auto-verify: no email service configured, or this is the first admin
		// (bootstrap: without verified email the admin can't log in to configure anything).
		emailVerified = true
	} else {
		rawToken, tokenErr := generateVerifyToken()
		if tokenErr == nil {
			verifyToken = sql.NullString{String: rawToken, Valid: true}
			verifyExpires = sql.NullTime{Time: time.Now().Add(24 * time.Hour), Valid: true}
		} else {
			// If we can't generate a token, auto-verify rather than blocking signup.
			log.Errorf("auth: failed to generate email verify token for %s: %v", req.Email, tokenErr)
			emailVerified = true
		}
	}

	// Assign subscription plan. Admins (incl. the first-user bootstrap) get the
	// unlimited 'pro' plan with no expiry so operators are never trial-capped.
	// Regular signups start directly on 'free' with an expiry of free.duration_days
	// from now (read from the plans table so it's configurable). The 'free' plan
	// grants 2 pipelines for 30 days and has no downgrade target, so access is
	// blocked on expiry until the user upgrades. See migration 060_user_plans.sql.
	userPlan := "free"
	var planExpires sql.NullTime
	if role == "admin" {
		userPlan = "pro"
	} else {
		freeDays := 30
		if err := h.db.QueryRow(`SELECT duration_days FROM plans WHERE name = 'free'`).Scan(&freeDays); err != nil || freeDays <= 0 {
			freeDays = 30
		}
		planExpires = sql.NullTime{Time: time.Now().Add(time.Duration(freeDays) * 24 * time.Hour), Valid: true}
	}

	// Create user
	userID := uuid.New().String()
	_, err = h.db.Exec(`
		INSERT INTO users (id, email, password_hash, role, name, company, phone, country, employee_range, status,
		                   email_verified, email_verify_token, email_verify_expires_at,
		                   plan, plan_expires_at,
		                   created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'active', $10, $11, $12, $13, $14, NOW(), NOW())
	`, userID, req.Email, string(hashedPassword), role, req.Name, req.Company, req.Phone, req.Country, req.EmployeeRange,
		emailVerified, verifyToken, verifyExpires, userPlan, planExpires)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create user"})
		return
	}

	// Provision the user's personal workspace + owner membership so they have a
	// workspace context from their first authenticated request (the signup-time
	// counterpart to migration 069). Every authenticated route now requires a
	// workspace, so a failure here must fail registration — and we roll back the
	// orphaned user row (FK cascades clean up any partial workspace) so the
	// email isn't locked to a broken account.
	if _, wsErr := provisionPersonalWorkspace(h.db, userID, req.Email); wsErr != nil {
		log.Errorf("auth: failed to provision personal workspace for %s: %v", req.Email, wsErr)
		if _, delErr := h.db.Exec(`DELETE FROM users WHERE id = $1`, userID); delErr != nil {
			log.Errorf("auth: failed to roll back user %s after workspace provisioning failure: %v", userID, delErr)
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create user"})
		return
	}

	// Mark invitation as used
	if inviteID != "" {
		h.db.Exec(`
			UPDATE invitations SET used_at = NOW(), used_by = $1 WHERE id = $2
		`, userID, inviteID)
	}

	// Join the workspace named by a team invite (atomic single-use claim +
	// membership), now that the user + personal workspace exist. Best-effort: a
	// race that consumed the invite first leaves the account valid with just its
	// personal workspace. The up-front validation already rejected bad/foreign
	// invites, so a no-join here is a rare concurrent-consume, not user error.
	if workspaceInviteToken != "" {
		if joinedWS, joinedRole, jErr := acceptWorkspaceInviteAtSignup(h.db, workspaceInviteToken, userID, req.Email); jErr != nil {
			log.Errorf("auth: failed to join workspace from invite for %s: %v", req.Email, jErr)
		} else if joinedWS != "" {
			log.Infof("auth: user %s joined workspace %s as %s via team invite", req.Email, joinedWS, joinedRole)
		}
	}

	// Send verification email asynchronously (don't fail registration if email delivery fails).
	if verifyToken.Valid {
		go func() {
			appURL := strings.TrimSpace(os.Getenv("APP_BASE_URL"))
			if appURL == "" {
				appURL = "http://localhost:3000"
			}
			verifyURL := fmt.Sprintf("%s/verify-email?token=%s", appURL, verifyToken.String)
			if sendErr := emailClient.SendVerification(
				context.Background(), req.Email, req.Name, verifyURL,
			); sendErr != nil {
				log.Errorf("auth: failed to send verification email to %s: %v", req.Email, sendErr)
			}
		}()
	}

	// Onboarding emails: welcome the new user, and alert admins of the signup.
	// Best-effort and fully async — registration never blocks or fails on email.
	if emailClient.IsConfigured() {
		newEmail, newName, newUserID := req.Email, req.Name, userID
		db := h.db
		go func() {
			ctx := context.Background()
			appURL := strings.TrimSpace(os.Getenv("APP_BASE_URL"))
			if appURL == "" {
				appURL = "http://localhost:3000"
			}
			if sendErr := emailClient.SendWelcome(ctx, newEmail, newName, appURL); sendErr != nil {
				log.Errorf("auth: failed to send welcome email to %s: %v", newEmail, sendErr)
			}

			// Notify all admins (excluding the new user, in case they're the bootstrap admin).
			var adminEmails []string
			if db != nil {
				rows, qerr := db.Query(`SELECT email FROM users WHERE role = 'admin' AND id != $1`, newUserID)
				if qerr != nil {
					log.Errorf("auth: failed to load admin emails for signup alert: %v", qerr)
					return
				}
				defer rows.Close()
				for rows.Next() {
					var e string
					if rows.Scan(&e) == nil {
						adminEmails = append(adminEmails, e)
					}
				}
			}
			if sendErr := emailClient.SendNewSignupAdminAlert(ctx, adminEmails, newEmail, newName); sendErr != nil {
				log.Errorf("auth: failed to send signup admin alert for %s: %v", newEmail, sendErr)
			}
		}()
	}

	// Generate session
	token := uuid.New().String()
	expiresAt := time.Now().Add(24 * time.Hour).Unix()

	_, err = h.db.Exec(`
		INSERT INTO sessions (id, user_id, token, expires_at, created_at)
		VALUES ($1, $2, $3, $4, NOW())
	`, uuid.New().String(), userID, sharedcrypto.HashSessionToken(token), time.Unix(expiresAt, 0))

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create session"})
		return
	}

	// Registration signs the new user straight in, so it is a login like any other.
	// Omitting it here would leave every freshly-registered user reading "-" until
	// their second visit.
	stampLastLogin(h.db, userID)

	// Audit: user registered
	logAudit(c, "register", "auth", userID, map[string]interface{}{
		"email":             req.Email,
		"role":              role,
		"invite_used":       inviteToken != "",
		"first_user":        userCount == 0,
		"email_verified":    emailVerified,
		"verification_sent": verifyToken.Valid,
	})

	// Set HttpOnly auth cookie (preferred)
	setAuthCookie(c, token, time.Unix(expiresAt, 0))
	ensureCSRFCookie(c)

	c.JSON(http.StatusCreated, AuthResponse{
		UserID:        userID,
		Email:         req.Email,
		Name:          req.Name,
		Role:          role,
		Status:        "active",
		Token:         token,
		ExpiresAt:     expiresAt,
		EmailVerified: emailVerified,
	})
}

// ValidateInvite checks if an invitation token is valid without consuming it.
// GET /api/v1/auth/invite/:token
func (h *AuthHandler) ValidateInvite(c *gin.Context) {
	token := strings.TrimSpace(c.Param("token"))
	if token == "" {
		c.JSON(http.StatusBadRequest, gin.H{"valid": false, "reason": "not_found"})
		return
	}

	var emailHint sql.NullString
	var role string
	var expiresAt time.Time
	var usedAt sql.NullTime

	err := h.db.QueryRow(`
		SELECT email_hint, role, expires_at, used_at
		FROM invitations
		WHERE token = $1
	`, token).Scan(&emailHint, &role, &expiresAt, &usedAt)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusOK, gin.H{"valid": false, "reason": "not_found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if usedAt.Valid {
		c.JSON(http.StatusOK, gin.H{"valid": false, "reason": "used"})
		return
	}
	if time.Now().After(expiresAt) {
		c.JSON(http.StatusOK, gin.H{"valid": false, "reason": "expired"})
		return
	}

	hint := ""
	if emailHint.Valid {
		hint = emailHint.String
	}

	c.JSON(http.StatusOK, gin.H{
		"valid":      true,
		"email_hint": hint,
		"role":       role,
		"expires_at": expiresAt,
	})
}

// Logout handles user logout
func (h *AuthHandler) Logout(c *gin.Context) {
	token := normalizeAuthTokenAny(c.GetHeader("Authorization"))
	if token == "" {
		if cv, err := c.Cookie("auth_token"); err == nil {
			token = normalizeAuthTokenAny(cv)
		}
	}
	if token == "" {
		clearAuthCookie(c)
		clearCSRFCookie(c)
		c.JSON(http.StatusOK, gin.H{"message": "Logged out"})
		return
	}

	// Get user info before deleting session for audit
	var userID string
	h.db.QueryRow("SELECT user_id FROM sessions WHERE token = $1", sharedcrypto.HashSessionToken(token)).Scan(&userID)

	// Delete session
	_, err := h.db.Exec("DELETE FROM sessions WHERE token = $1", sharedcrypto.HashSessionToken(token))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to logout"})
		return
	}

	// Audit: user logged out
	if userID != "" {
		logAudit(c, "logout", "auth", userID, nil)
	}

	clearAuthCookie(c)
	clearCSRFCookie(c)
	c.JSON(http.StatusOK, gin.H{"message": "Logged out successfully"})
}

// Me returns current user info
func (h *AuthHandler) Me(c *gin.Context) {
	token := normalizeAuthTokenAny(c.GetHeader("Authorization"))
	if token == "" {
		if cv, err := c.Cookie("auth_token"); err == nil {
			token = normalizeAuthTokenAny(cv)
		}
	}
	if token == "" {
		// Dev: keep prior behavior when auth isn't configured.
		// Prod: this will be enforced by AuthRequiredMiddleware.
		if strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT"))) != "production" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "No authorization token"})
			return
		}
		c.JSON(http.StatusUnauthorized, gin.H{"error": "No authorization token"})
		return
	}

	var userID, email, role, status, name string
	var meVerified bool
	err := h.db.QueryRow(`
		SELECT u.id, u.email, u.role, COALESCE(u.status, 'active'), COALESCE(u.name, ''),
		       COALESCE(u.email_verified, true)
		FROM users u
		JOIN sessions s ON s.user_id = u.id
		WHERE s.token = $1 AND s.expires_at > NOW()
	`, sharedcrypto.HashSessionToken(token)).Scan(&userID, &email, &role, &status, &name, &meVerified)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Plan entitlements for the trial / upgrade banner (PR 103/105). Entitlements
	// are now per-WORKSPACE (the billable tenant), but /auth/me runs outside
	// WorkspaceContextMiddleware, so resolve the caller's PERSONAL workspace
	// (every user has exactly one — migration 069) and report ITS effective plan
	// (after any lazy expiry downgrade), expiry, and pipeline count — keeping
	// used/limit on the same axis as the enforcement gate (checkPipelineCreateOK).
	// Permissive on every hop: a billing or migration hiccup must never break
	// /auth/me, so missing data just yields a pro-equivalent (no banner).
	var bannerWS string
	_ = h.db.QueryRow(
		`SELECT id FROM workspaces WHERE owner_id = $1 AND is_personal = true LIMIT 1`,
		userID,
	).Scan(&bannerWS)

	quota := resolvePlanQuota(c.Request.Context(), h.db, bannerWS)

	var planExpiresAt sql.NullTime
	_ = h.db.QueryRow(`SELECT plan_expires_at FROM workspaces WHERE id = $1`, bannerWS).Scan(&planExpiresAt)

	var pipelinesUsed int
	_ = h.db.QueryRow(`SELECT COUNT(*) FROM pipelines WHERE workspace_id = $1`, bannerWS).Scan(&pipelinesUsed)

	resp := gin.H{
		"user_id":        userID,
		"email":          email,
		"role":           role,
		"status":         status,
		"name":           name,
		"email_verified": meVerified,
		"plan":           quota.plan,
		"pipelines_used": pipelinesUsed,
	}
	// pipelines_limit: null means unlimited (pro). The frontend treats null as no cap.
	if quota.effectiveLimit >= 0 {
		resp["pipelines_limit"] = quota.effectiveLimit
	} else {
		resp["pipelines_limit"] = nil
	}
	// trial_ends_at: ISO-8601, only set while a dated plan (trial/free) is active.
	if planExpiresAt.Valid {
		resp["trial_ends_at"] = planExpiresAt.Time.UTC().Format(time.RFC3339)
	} else {
		resp["trial_ends_at"] = nil
	}
	c.JSON(http.StatusOK, resp)
}

// UpdateMe updates the current user's mutable profile fields (name only for now).
func (h *AuthHandler) UpdateMe(c *gin.Context) {
	token := normalizeAuthTokenAny(c.GetHeader("Authorization"))
	if token == "" {
		if cv, err := c.Cookie("auth_token"); err == nil {
			token = normalizeAuthTokenAny(cv)
		}
	}
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "No authorization token"})
		return
	}

	var userID string
	err := h.db.QueryRow(`
		SELECT u.id FROM users u
		JOIN sessions s ON s.user_id = u.id
		WHERE s.token = $1 AND s.expires_at > NOW()
	`, sharedcrypto.HashSessionToken(token)).Scan(&userID)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	var body struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	name := strings.TrimSpace(body.Name)
	_, err = h.db.Exec(`UPDATE users SET name = $1, updated_at = NOW() WHERE id = $2`, name, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update profile"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"name": name})
}

// ChangePassword verifies the current password and replaces it with a new bcrypt hash.
func (h *AuthHandler) ChangePassword(c *gin.Context) {
	token := normalizeAuthTokenAny(c.GetHeader("Authorization"))
	if token == "" {
		if cv, err := c.Cookie("auth_token"); err == nil {
			token = normalizeAuthTokenAny(cv)
		}
	}
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "No authorization token"})
		return
	}

	var userID string
	var passwordHash string
	err := h.db.QueryRow(`
		SELECT u.id, u.password_hash FROM users u
		JOIN sessions s ON s.user_id = u.id
		WHERE s.token = $1 AND s.expires_at > NOW()
	`, sharedcrypto.HashSessionToken(token)).Scan(&userID, &passwordHash)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	var body struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	if strings.TrimSpace(body.CurrentPassword) == "" || strings.TrimSpace(body.NewPassword) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "current_password and new_password are required"})
		return
	}
	if len(body.NewPassword) < 8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "New password must be at least 8 characters"})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(body.CurrentPassword)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Current password is incorrect"})
		return
	}

	newHash, err := bcrypt.GenerateFromPassword([]byte(body.NewPassword), bcryptCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to hash password"})
		return
	}

	_, err = h.db.Exec(`UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2`, string(newHash), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update password"})
		return
	}

	// SEC-M-02: revoke all OTHER sessions so a password change logs out every other
	// device. Mirrors admin_users.go:331 (fire-and-forget DELETE FROM sessions).
	// token <> $2 preserves the current caller's session so they stay logged in.
	h.db.Exec(`DELETE FROM sessions WHERE user_id = $1 AND token <> $2`, userID, sharedcrypto.HashSessionToken(token))

	c.JSON(http.StatusOK, gin.H{"message": "Password updated successfully"})
}

// getInstanceSetting reads a single value from instance_settings, returning
// defaultVal if the key is not set.
//
// "Not set" and "could not read" are different answers and the caller has to be
// able to tell them apart. sql.ErrNoRows means the key genuinely has no value,
// and defaultVal is then correct. Any other error means we do not know what the
// setting says — returning defaultVal there is a guess, and for
// registration_mode the guess is "anyone may create an account".
func getInstanceSetting(database *sql.DB, key, defaultVal string) (string, error) {
	var value string
	err := database.QueryRow("SELECT value FROM instance_settings WHERE key = $1", key).Scan(&value)
	if err == sql.ErrNoRows {
		return defaultVal, nil
	}
	if err != nil {
		return "", err
	}
	return value, nil
}

// generateInviteToken creates a cryptographically random 32-byte hex token.
func generateInviteToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// generateVerifyToken creates a cryptographically random 32-byte (64 hex-char) token
// suitable for one-time email verification links.
func generateVerifyToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// VerifyEmail handles GET /api/v1/auth/verify-email?token=<token>
// Marks the user's email as verified and clears the verification token.
func (h *AuthHandler) VerifyEmail(c *gin.Context) {
	token := strings.TrimSpace(c.Query("token"))
	if token == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing_token", "message": "Verification token is required"})
		return
	}

	var userID string
	var expires time.Time
	err := h.db.QueryRow(`
		SELECT id, email_verify_expires_at
		FROM users
		WHERE email_verify_token = $1 AND email_verified = false
	`, token).Scan(&userID, &expires)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "invalid_token",
			"message": "Verification link is invalid or has already been used",
		})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if time.Now().After(expires) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "token_expired",
			"message": "Verification link has expired. Please request a new one.",
		})
		return
	}

	_, err = h.db.Exec(`
		UPDATE users
		SET email_verified = true,
		    email_verify_token = NULL,
		    email_verify_expires_at = NULL,
		    updated_at = NOW()
		WHERE id = $1
	`, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify email"})
		return
	}

	logAudit(c, "email_verified", "auth", userID, nil)
	c.JSON(http.StatusOK, gin.H{"message": "Email verified successfully. You can now access all features."})
}

// ResendVerification handles POST /api/v1/auth/resend-verification
// Generates a fresh verification token and resends the email.
// Requires a valid session (the user must be logged in but unverified).
func (h *AuthHandler) ResendVerification(c *gin.Context) {
	token := normalizeAuthTokenAny(c.GetHeader("Authorization"))
	if token == "" {
		if cv, err := c.Cookie("auth_token"); err == nil {
			token = normalizeAuthTokenAny(cv)
		}
	}
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "No authorization token"})
		return
	}

	var userID, email, name string
	var alreadyVerified bool
	err := h.db.QueryRow(`
		SELECT u.id, u.email, COALESCE(u.name,''), u.email_verified
		FROM users u
		JOIN sessions s ON s.user_id = u.id
		WHERE s.token = $1 AND s.expires_at > NOW()
	`, sharedcrypto.HashSessionToken(token)).Scan(&userID, &email, &name, &alreadyVerified)

	if err == sql.ErrNoRows {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired session"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}
	if alreadyVerified {
		c.JSON(http.StatusOK, gin.H{"message": "Email is already verified"})
		return
	}

	if !emailClient.IsConfigured() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "email_not_configured",
			"message": "Email service is not configured on this instance",
		})
		return
	}

	newToken, err := generateVerifyToken()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}
	newExpires := time.Now().Add(24 * time.Hour)

	_, err = h.db.Exec(`
		UPDATE users
		SET email_verify_token = $1,
		    email_verify_expires_at = $2,
		    updated_at = NOW()
		WHERE id = $3
	`, newToken, newExpires, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update token"})
		return
	}

	appURL := strings.TrimSpace(os.Getenv("APP_BASE_URL"))
	if appURL == "" {
		appURL = "http://localhost:3000"
	}
	verifyURL := fmt.Sprintf("%s/verify-email?token=%s", appURL, newToken)

	go func() {
		if sendErr := emailClient.SendVerification(
			context.Background(), email, name, verifyURL,
		); sendErr != nil {
			log.Errorf("auth: failed to resend verification email to %s: %v", email, sendErr)
		}
	}()

	logAudit(c, "verification_resent", "auth", userID, map[string]interface{}{"email": email})
	c.JSON(http.StatusOK, gin.H{"message": "Verification email sent. Please check your inbox."})
}
