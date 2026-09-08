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

BEGIN;

-- Drop any new-name row that already exists, so the rename below cannot collide
-- with the config_key primary key on a re-run.
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

COMMIT;
