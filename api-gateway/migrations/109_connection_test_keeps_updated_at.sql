-- 109_connection_test_keeps_updated_at.sql
-- Running "Test Connection" changed the connection's "Last Updated" time.
--
-- The test writes only last_tested_at / last_test_status / last_test_error
-- (handlers/connections.go TestConnection), but the BEFORE UPDATE trigger from
-- 001 stamps updated_at = NOW() on every UPDATE. A test result is not an edit,
-- so an update that changes only those three columns now keeps the old
-- updated_at. Any other change (name, config, status, …) still stamps NOW().
--
-- Idempotent: CREATE OR REPLACE the function, drop and re-create the trigger.

CREATE OR REPLACE FUNCTION connections_touch_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    test_only_columns text[] := ARRAY['last_tested_at', 'last_test_status', 'last_test_error', 'updated_at'];
BEGIN
    IF (to_jsonb(NEW) - test_only_columns) IS NOT DISTINCT FROM (to_jsonb(OLD) - test_only_columns) THEN
        NEW.updated_at = OLD.updated_at;
    ELSE
        NEW.updated_at = NOW();
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS update_connections_updated_at ON connections;
CREATE TRIGGER update_connections_updated_at BEFORE UPDATE ON connections
    FOR EACH ROW EXECUTE FUNCTION connections_touch_updated_at();
