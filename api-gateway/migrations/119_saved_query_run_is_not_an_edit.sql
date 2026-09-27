-- Migration 119: a model run no longer moves its saved query's updated_at.
--
-- 084 gave saved_queries the shared update_updated_at_column() trigger, which sets
-- updated_at = NOW() on every UPDATE. A model run UPDATEs its saved query to stamp
-- the outcome (last_run_at, last_run_status, last_run_error:
-- saved_query_models.go stampLastRunOutcome, saved_query_run_failure.go
-- stampLastRunFailureIfLatest), and its first rebuild claims the target table
-- (target_owned, saved_query_models.go recordTargetOwnership). So updated_at meant
-- "last run" on any model that runs:
--   * the model page's "Updated" repeated "Last run";
--   * the Saved list, ORDER BY updated_at DESC, floated every running model to the
--     top on every run;
--   * the saved-query editor uses updated_at as its concurrency token
--     (saved_queries.go UpdateSavedQuery), so an edit dialog left open across a
--     scheduled run was refused as a stale write that nobody had made.
--
-- Same fix, same shape, as 109 gave connections for a test result: an UPDATE that
-- changes only that run bookkeeping keeps the old updated_at; any other change
-- still stamps NOW(). The bookkeeping set is listed, not the edit set, on purpose:
-- a column added later counts as an edit, which is what 084 did for every column.
--
-- The concurrency token stays sound: the editor never writes a bookkeeping column,
-- so a change it could overwrite always moves updated_at.
--
-- Idempotent: CREATE OR REPLACE the function, drop and re-create the trigger.
-- Other tables keep update_updated_at_column().

CREATE OR REPLACE FUNCTION saved_queries_touch_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    run_only_columns text[] := ARRAY['last_run_at', 'last_run_status', 'last_run_error', 'target_owned', 'updated_at'];
BEGIN
    IF (to_jsonb(NEW) - run_only_columns) IS NOT DISTINCT FROM (to_jsonb(OLD) - run_only_columns) THEN
        NEW.updated_at = OLD.updated_at;
    ELSE
        NEW.updated_at = NOW();
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS update_saved_queries_updated_at ON saved_queries;
CREATE TRIGGER update_saved_queries_updated_at BEFORE UPDATE ON saved_queries
    FOR EACH ROW EXECUTE FUNCTION saved_queries_touch_updated_at();

COMMENT ON FUNCTION saved_queries_touch_updated_at() IS 'saved_queries.updated_at = last edit: an UPDATE that changes only last_run_at/_status/_error or target_owned (a model run) keeps the old value';
