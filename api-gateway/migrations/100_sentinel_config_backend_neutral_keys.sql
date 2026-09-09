-- 100: rename the sentinel_config telemetry keys off a specific vendor
--
-- Migration 011 originally seeded three rows whose keys and descriptions named a
-- particular observability product. This repo ships no observability backend --
-- the sentinel exports OpenTelemetry metrics over OTLP to whatever collector is
-- configured, and the seeded endpoint was already the generic
-- `http://otel-collector:4318`. The vendor name was never more than a label.
--
-- 011's seed block is corrected in place, so a fresh database seeds the new keys
-- directly and every statement below matches nothing. This file exists for a
-- database that already ran the old 011.
--
-- Safe by construction: nothing reads sentinel_config. The Go struct these rows
-- shadow (SentinelConfig in backend-orchestrator/internal/agents/sentinel) is
-- populated from DefaultSentinelConfig(), never from this table, so renaming a
-- key here cannot change any runtime behaviour. The rename exists so an operator
-- reading the config table is not told to go install a product this repo does
-- not ship.
--
-- Idempotent: each statement is a no-op once applied, and the DELETEs guard
-- against a partially-seeded table where both the old and the new key exist.
--
-- GUARDED BY to_regclass, and this is not defensive dressing -- it is the only
-- reason the file can be applied at all. `013_cleanup_unused_tables.sql` runs
-- `DROP TABLE IF EXISTS sentinel_config CASCADE` and nothing recreates it, so
-- the table is ABSENT on every schema this migration ever meets: a fresh install
-- creates it at 011 and drops it 2 files later, and an upgrade arrives with 013
-- long since applied. Unguarded, the DELETE below raises
-- `relation "sentinel_config" does not exist` (SQLSTATE 42P01); migrate.go
-- returns on the first failure, so migration 100 halts the whole sequence, the
-- schema never reaches ready, and /ready answers 503 schema_not_migrated
-- forever. Measured against postgres:16 by replaying all 105 files in the
-- runner's order: 104 applied, this one failed. Same shape and same cause as the
-- to_regclass guards in 077 and 078, which cover the other tables 013 dropped.
--
-- The runner auto-wraps this file in one transaction; do NOT add a literal
-- "BEGIN;" (it trips migrate.go's self-managed-txn detection).

DO $$
BEGIN
    IF to_regclass('public.sentinel_config') IS NOT NULL THEN

        -- Drop any new-name row that already exists, so the rename below cannot
        -- collide with the config_key primary key on a re-run.
        DELETE FROM sentinel_config
         WHERE config_key = 'enable_metrics_export'
           AND EXISTS (SELECT 1 FROM sentinel_config WHERE config_key = 'enable_signoz_export');

        DELETE FROM sentinel_config
         WHERE config_key = 'metrics_otlp_endpoint'
           AND EXISTS (SELECT 1 FROM sentinel_config WHERE config_key = 'signoz_endpoint');

        UPDATE sentinel_config
           SET config_key  = 'enable_metrics_export',
               description = 'Enable OpenTelemetry metrics export over OTLP'
         WHERE config_key = 'enable_signoz_export';

        UPDATE sentinel_config
           SET config_key  = 'metrics_otlp_endpoint',
               description = 'OTLP endpoint for exported metrics'
         WHERE config_key = 'signoz_endpoint';

        UPDATE sentinel_config
           SET description = 'Interval for exporting metrics over OTLP'
         WHERE config_key = 'metric_export_interval';

    END IF;
END $$;
