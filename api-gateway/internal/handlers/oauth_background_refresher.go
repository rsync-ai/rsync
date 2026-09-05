package handlers

import (
	"context"
	"database/sql"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
)

// BackgroundTokenRefresher proactively refreshes OAuth tokens that are approaching expiry.
// It runs as a long-lived goroutine and uses the same RefreshTokenByID path as the
// proactive check in enrichConfigWithOAuthToken, so the pg_try_advisory_lock pattern
// prevents double-refresh across multiple api-gateway instances.
type BackgroundTokenRefresher struct {
	db          *sql.DB
	refreshFunc func(ctx context.Context, tokenID string) error
	interval    time.Duration // how often to scan (default: 10 minutes)
	lookAhead   time.Duration // refresh tokens expiring within this window (default: 30 minutes)
}

// NewBackgroundTokenRefresher creates a refresher that scans every 10 minutes for
// tokens expiring within 30 minutes and refreshes them.
func NewBackgroundTokenRefresher(db *sql.DB, refreshFunc func(context.Context, string) error) *BackgroundTokenRefresher {
	return &BackgroundTokenRefresher{
		db:          db,
		refreshFunc: refreshFunc,
		interval:    10 * time.Minute,
		lookAhead:   30 * time.Minute,
	}
}

// Start runs the background refresh loop in a goroutine until ctx is cancelled.
func (r *BackgroundTokenRefresher) Start(ctx context.Context) {
	go r.loop(ctx)
	log.Infof("oauth: BackgroundTokenRefresher started (interval=%s, look_ahead=%s)", r.interval, r.lookAhead)
}

func (r *BackgroundTokenRefresher) loop(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// Run immediately on start so tokens are fresh at cold boot too.
	r.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("oauth: BackgroundTokenRefresher stopped")
			return
		case <-ticker.C:
			r.runOnce(ctx)
		}
	}
}

func (r *BackgroundTokenRefresher) runOnce(ctx context.Context) {
	// Find tokens that:
	//   - expire within lookAhead
	//   - have a refresh_token stored
	//   - were not updated in the last 5 minutes (skip recently refreshed)
	//   - belong to a real user (user_id IS NOT NULL — orphaned tokens are inaccessible)
	rows, err := r.db.QueryContext(ctx, `
		SELECT id
		FROM oauth_tokens
		WHERE expires_at < NOW() + $1
		  AND refresh_token IS NOT NULL
		  AND refresh_token != ''
		  AND updated_at < NOW() - INTERVAL '5 minutes'
		  AND user_id IS NOT NULL
	`, r.lookAhead)
	if err != nil {
		log.WithError(err).Warn("oauth: BackgroundTokenRefresher query failed")
		return
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}

	if len(ids) == 0 {
		return
	}

	log.Infof("oauth: BackgroundTokenRefresher found %d token(s) to refresh", len(ids))

	for _, tokenID := range ids {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := r.refreshFunc(ctx, tokenID); err != nil {
			// ErrTokenFresh is not a real error — another path already refreshed it.
			if errors.Is(err, ErrTokenFresh) {
				continue
			}
			// ErrNoRefreshToken means the user must re-authenticate; not actionable here.
			if errors.Is(err, ErrNoRefreshToken) {
				log.Debugf("oauth: BackgroundTokenRefresher skipping %s — no refresh_token (re-auth required)", tokenID)
				continue
			}
			if errors.Is(err, ErrRefreshTokenExpired) {
				// Refresh token permanently rejected — clear it so we stop retrying.
				// ListTokens will return expired:true (expires_at < NOW()) so the
				// frontend can surface a "Reconnect" prompt.
				log.WithError(err).Errorf("oauth: refresh token permanently rejected for %s — clearing to stop retry loop", tokenID)
				r.db.ExecContext(ctx,
					`UPDATE oauth_tokens SET refresh_token = '', updated_at = NOW() WHERE id = $1`,
					tokenID,
				)
				continue
			}
			log.WithError(err).Warnf("oauth: BackgroundTokenRefresher failed to refresh token %s", tokenID)
		}
	}
}
