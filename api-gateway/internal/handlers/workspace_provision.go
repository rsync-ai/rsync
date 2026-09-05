package handlers

import (
	"database/sql"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

var nonSlugChars = regexp.MustCompile(`[^a-z0-9]`)

func emailLocalPart(email string) string {
	if i := strings.IndexByte(email, '@'); i >= 0 {
		return email[:i]
	}
	return email
}

// personalWorkspaceName matches migration 069's naming: "<local-part>'s workspace".
func personalWorkspaceName(email string) string {
	return emailLocalPart(email) + "'s workspace"
}

// personalWorkspaceSlug derives a URL-safe, collision-resistant slug for a
// user's personal workspace: the sanitized email local-part plus a short random
// suffix. The suffix is required because slug is UNIQUE and two users can share
// a local-part across different domains.
func personalWorkspaceSlug(email string) string {
	base := nonSlugChars.ReplaceAllString(strings.ToLower(emailLocalPart(email)), "-")
	return base + "-" + randomSlugSuffix()
}

// randomSlugSuffix returns 8 random hex characters for disambiguating a slug that
// is already taken. Shared with CreateWorkspace's retry loop so both paths resolve
// a collision the same way: a fresh value on every call, never a clock- or
// id-derived one that repeats and lets a taken slug reach the UNIQUE constraint.
func randomSlugSuffix() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
}

// provisionPersonalWorkspace creates a user's personal workspace and the owner
// membership row in one shot, returning the new workspace id. This is the
// signup-time counterpart to what migration 069 did for pre-existing users, so
// every user has a workspace context from their very first authenticated request.
func provisionPersonalWorkspace(database *sql.DB, userID, email string) (string, error) {
	var wsID string
	if err := database.QueryRow(`
		INSERT INTO workspaces (name, slug, owner_id, plan, is_personal)
		VALUES ($1, $2, $3, 'free', true)
		RETURNING id
	`, personalWorkspaceName(email), personalWorkspaceSlug(email), userID).Scan(&wsID); err != nil {
		return "", err
	}
	if _, err := database.Exec(`
		INSERT INTO workspace_members (workspace_id, user_id, role)
		VALUES ($1, $2, 'owner')
		ON CONFLICT (workspace_id, user_id) DO NOTHING
	`, wsID, userID); err != nil {
		return "", err
	}
	return wsID, nil
}
