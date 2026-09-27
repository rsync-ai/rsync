-- 112_instance_notifications.sql
--
-- Let pipeline_notifications hold an alert that is about the INSTANCE rather
-- than about one pipeline.
--
-- Why: some alert producers raise ops-level problems that belong to no
-- pipeline. healthwatch (backend-orchestrator/internal/agents/healthwatch/
-- watchdog.go) publishes its connector-version-regression alert with the
-- synthetic pipeline_id "system". "system" is not a uuid, so the notifier's
-- `SELECT created_by FROM pipelines WHERE id = $1` failed on it every single
-- time and the alert was logged and dropped. There was no row it could have
-- written even had the lookup succeeded: pipeline_id was NOT NULL with an FK
-- to pipelines, and an instance alert has no pipeline to point at.
--
-- After this, pipeline_id IS NULL means "about this instance".
--
-- user_id deliberately stays NOT NULL. The notifier fans an instance alert out
-- to one row per active admin instead of writing a single ownerless row, so
-- the per-user partial index (idx_pipeline_notifications_user_unread) and the
-- bell's `WHERE user_id = $1` predicate keep working untouched, and each admin
-- can read and clear their own copy.
--
-- Readers beware: every query that joined pipelines to scope the bell to a
-- workspace must now LEFT JOIN and admit the NULL rows
-- (api-gateway/internal/handlers/notifications.go). An inner join silently
-- hides exactly the rows this migration makes possible — which is the same
-- class of invisible-alert bug the migration exists to fix.

ALTER TABLE pipeline_notifications
    ALTER COLUMN pipeline_id DROP NOT NULL;

COMMENT ON COLUMN pipeline_notifications.pipeline_id IS
    'The pipeline this alert is about. NULL means an instance-level alert that belongs to no pipeline; the notifier fans those out to one row per active admin.';
