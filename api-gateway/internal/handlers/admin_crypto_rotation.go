package handlers

import (
	"database/sql"
	"net/http"
	"strings"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/shared/crypto"
)

type rotateEncryptionRequest struct {
	DryRun bool `json:"dry_run"`
	Limit  int  `json:"limit,omitempty"` // safety cap (0 = no cap)
}

type rotateEncryptionStats struct {
	Scanned  int `json:"scanned"`
	Rotated  int `json:"rotated"`
	Failed   int `json:"failed"`
	Skipped  int `json:"skipped"`
	Duration int `json:"duration_ms"`
}

// AdminRotateEncryptionKeys re-encrypts stored secrets with the current primary encryption key.
// POST /api/v1/admin/encryption/rotate
func AdminRotateEncryptionKeys(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	var req rotateEncryptionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}
	if req.Limit < 0 {
		req.Limit = 0
	}
	if req.Limit > 50000 {
		req.Limit = 50000
	}

	start := time.Now()
	stats := map[string]*rotateEncryptionStats{}

	rotateTable := func(name string, query string, update string, scan func(*sql.Rows) (id string, ciphertext string, err error)) {
		st := &rotateEncryptionStats{}
		stats[name] = st

		rows, err := database.Query(query)
		if err != nil {
			st.Failed++
			log.WithError(err).Errorf("[AdminRotateEncryptionKeys] failed to query %s", name)
			return
		}
		defer rows.Close()

		for rows.Next() {
			if req.Limit > 0 && st.Scanned >= req.Limit {
				break
			}
			id, ciphertext, err := scan(rows)
			if err != nil {
				st.Failed++
				continue
			}
			id = strings.TrimSpace(id)
			ciphertext = strings.TrimSpace(ciphertext)
			if id == "" || ciphertext == "" {
				st.Skipped++
				continue
			}
			st.Scanned++

			newCipher, rotated, err := crypto.RotateCiphertextIfNeeded(ciphertext)
			if err != nil {
				st.Failed++
				continue
			}
			if !rotated {
				st.Skipped++
				continue
			}

			if req.DryRun {
				st.Rotated++
				continue
			}

			if _, err := database.Exec(update, newCipher, id); err != nil {
				st.Failed++
				continue
			}
			st.Rotated++
		}
		st.Duration = int(time.Since(start).Milliseconds())
	}

	rotateTable(
		"connections.config",
		`SELECT id::text, config FROM connections WHERE config IS NOT NULL AND config <> ''`,
		`UPDATE connections SET config = $1, updated_at = NOW() WHERE id = $2::uuid`,
		func(rows *sql.Rows) (string, string, error) {
			var id string
			var cfg string
			return id, cfg, rows.Scan(&id, &cfg)
		},
	)

	rotateTable(
		"oauth_tokens.access_token",
		`SELECT id, access_token FROM oauth_tokens WHERE access_token IS NOT NULL AND access_token <> ''`,
		`UPDATE oauth_tokens SET access_token = $1, updated_at = NOW() WHERE id = $2`,
		func(rows *sql.Rows) (string, string, error) {
			var id string
			var tok string
			return id, tok, rows.Scan(&id, &tok)
		},
	)

	rotateTable(
		"oauth_tokens.refresh_token",
		`SELECT id, refresh_token FROM oauth_tokens WHERE refresh_token IS NOT NULL AND refresh_token <> ''`,
		`UPDATE oauth_tokens SET refresh_token = $1, updated_at = NOW() WHERE id = $2`,
		func(rows *sql.Rows) (string, string, error) {
			var id string
			var tok string
			return id, tok, rows.Scan(&id, &tok)
		},
	)

	// oauth_apps.client_secret is encrypted at rest (per-user BYO OAuth apps,
	// migration 067) and was previously omitted from rotation. The table has a
	// composite PK (user_id, provider); encode it as "user_id|provider" and
	// split it back out in the UPDATE (uuid + simple provider name never
	// contain '|').
	rotateTable(
		"oauth_apps.client_secret",
		`SELECT user_id::text || '|' || provider AS id, client_secret FROM oauth_apps WHERE client_secret IS NOT NULL AND client_secret <> ''`,
		`UPDATE oauth_apps SET client_secret = $1, updated_at = NOW() WHERE user_id = split_part($2, '|', 1)::uuid AND provider = split_part($2, '|', 2)`,
		func(rows *sql.Rows) (string, string, error) {
			var id string
			var sec string
			return id, sec, rows.Scan(&id, &sec)
		},
	)

	c.JSON(http.StatusOK, gin.H{
		"success":  true,
		"dry_run":  req.DryRun,
		"limit":    req.Limit,
		"duration": int(time.Since(start).Milliseconds()),
		"stats":    stats,
	})
}
