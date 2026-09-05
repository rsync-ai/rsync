package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// migrationLockKey is the pg_advisory_lock key every replica's migration runner
// serializes on. The value is arbitrary; what matters is that all replicas use
// the same one. Nothing else in this codebase takes an advisory lock, so there
// is no collision to reason about.
const migrationLockKey int64 = 8410372615409821

// migrationLockTimeout bounds how long a replica waits for the winner to finish.
// Generous on purpose: a cold install applies 100+ files, and the failure this
// bound exists for is a wedged holder, not a slow one.
const migrationLockTimeout = 5 * time.Minute

// Migrate runs database migrations from the specified directory
func Migrate(migrationsDir string) error {
	db := GetDB()
	if db == nil {
		return fmt.Errorf("database not connected")
	}

	// Serialize the runner across replicas before touching anything.
	//
	// apiGateway.replicaCount defaults to 2 in the Helm chart, and both replicas
	// unblock from the same `nc -z postgres` init wait, so on a cold install they
	// enter this function within seconds of each other. Everything below is
	// check-then-act with no locking -- SELECT EXISTS(...) in applyMigration and
	// then a bare INSERT INTO schema_migrations -- so whoever loses either
	// duplicate-keys on the INSERT or re-runs the same DDL. Either way it returns
	// early, never reaches markSchemaReady(), and (because main.go logs the error
	// and starts serving anyway) is held out of the Service endpoints by a 503
	// schema_not_migrated readiness probe. The deployment lands 1/2 Ready.
	//
	// An advisory lock makes the loser WAIT instead: it wakes once the winner has
	// committed, walks the same list, finds every file already applied, skips them
	// all, and marks itself ready. 2/2, with no change to the migration files.
	//
	// The lock is taken BEFORE the CREATE TABLE below, not after: two sessions
	// issuing CREATE TABLE IF NOT EXISTS concurrently can still collide on the
	// catalog insert, which is the same race one statement earlier.
	//
	// It must be taken and released on ONE connection -- advisory locks are
	// session-scoped and database/sql hands out an arbitrary pooled connection per
	// call. The migrations themselves keep running on the pool; only this runner
	// takes this key, so nothing else contends for it.
	lockCtx, cancelLock := context.WithTimeout(context.Background(), migrationLockTimeout)
	defer cancelLock()

	conn, err := db.Conn(lockCtx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection for migration lock: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(lockCtx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("failed to acquire migration advisory lock: %w", err)
	}
	defer func() {
		// Fresh context: lockCtx may already be past its deadline by now.
		//
		// This unlock is the ONLY release, so it is not best-effort. A session
		// advisory lock is released when the *session* ends, and sql.Conn.Close()
		// does not end one -- it returns the connection to the pool, still open
		// and still holding the lock. Measured against postgres:16 with the
		// checker on a separate *sql.DB (distinct backend PIDs, because advisory
		// locks are re-entrant within a session and asking on the same one
		// answers "free" no matter what): after unlock+Close the key is free,
		// after Close alone it is still held.
		//
		// A dropped unlock therefore leaks the lock onto a pooled connection that
		// unrelated queries then borrow, and the next Migrate() in this process
		// blocks for the full migrationLockTimeout on a lock nothing will release.
		// So if the unlock fails, take the session down deliberately: returning
		// driver.ErrBadConn from Raw marks the driver connection bad, and Close
		// then destroys it rather than pooling it. That genuinely ends the session
		// -- which is the fallback this comment used to claim already existed.
		unlockCtx, cancelUnlock := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelUnlock()
		if _, err := conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", migrationLockKey); err != nil {
			log.Warnf("failed to release migration advisory lock; discarding the connection "+
				"so the session ends and Postgres releases it: %v", err)
			if rawErr := conn.Raw(func(any) error { return driver.ErrBadConn }); rawErr != nil && rawErr != driver.ErrBadConn {
				log.Warnf("could not discard the migration lock connection: %v", rawErr)
			}
		}
	}()

	// 1. Create migrations table if it doesn't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	// 2. Read migration files
	files, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory '%s': %w", migrationsDir, err)
	}

	var migrationFiles []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".sql") {
			migrationFiles = append(migrationFiles, f.Name())
		}
	}
	sort.Strings(migrationFiles)

	// 3. Apply migrations
	for _, file := range migrationFiles {
		if err := applyMigration(db, migrationsDir, file); err != nil {
			return err
		}
	}

	// Only now is the database actually usable. /ready reads this.
	markSchemaReady()
	return nil
}

func applyMigration(db *sql.DB, dir, filename string) error {
	// Check if already applied
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", filename).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check migration status for %s: %w", filename, err)
	}

	if exists {
		log.Printf("Migration %s already applied, skipping.", filename)
		return nil
	}

	log.Printf("Applying migration %s...", filename)

	// Read SQL
	content, err := os.ReadFile(filepath.Join(dir, filename))
	if err != nil {
		return fmt.Errorf("failed to read migration file %s: %w", filename, err)
	}

	sqlContent := string(content)

	// Some migration files manage their own transaction (BEGIN; ... COMMIT;).
	// Running those inside the runner's db.Begin() wrapper causes the file's
	// COMMIT to commit the outer transaction early — the subsequent INSERT
	// INTO schema_migrations then runs on a dead tx and fails with
	// "unexpected transaction status idle".
	//
	// Detection: look for "BEGIN;" anywhere in the file (case-insensitive).
	// Files with comment headers before BEGIN are handled correctly because
	// we search the whole content, not just the prefix.
	selfManaged := strings.Contains(strings.ToUpper(sqlContent), "BEGIN;")

	if selfManaged {
		// Execute the SQL as-is — it commits its own transaction.
		if _, err := db.Exec(sqlContent); err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", filename, err)
		}
		// Record the version in a fresh, separate transaction.
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin record transaction for %s: %w", filename, err)
		}
		defer tx.Rollback()
		if _, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES ($1)", filename); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", filename, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit record transaction for %s: %w", filename, err)
		}
	} else {
		// Runner wraps the SQL in a single transaction so the schema_migrations
		// INSERT is atomic with the migration itself.
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()

		if _, err := tx.Exec(sqlContent); err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", filename, err)
		}
		if _, err := tx.Exec("INSERT INTO schema_migrations (version) VALUES ($1)", filename); err != nil {
			return fmt.Errorf("failed to record migration %s: %w", filename, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction for %s: %w", filename, err)
		}
	}

	log.Printf("✅ Migration %s applied successfully.", filename)
	return nil
}

