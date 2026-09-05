-- Migration 088: a third materialization mode — 'statement'.
--
-- 085 gave a model exactly one shape: a query wrapped in CREATE TABLE … AS. That shape
-- assumes the SQL is a SELECT and that the result belongs in a new table, and it is the
-- wrong shape for the statement people actually schedule most often — a MERGE, an
-- UPDATE, an INSERT … SELECT. Those already name their own destination in the SQL, so
-- the modal's "write to table" question has no answer, and users were being asked to
-- invent one before rsync would let them schedule anything at all.
--
-- 'statement' runs the stored SQL as written: no wrapping, no staging table, no swap,
-- no target_table. The two modes therefore partition by what the SQL is, which is also
-- how the run path authorizes them (authorizeModelRun): 'table' requires a read, and
-- 'statement' requires a write whose class the run-as user's CURRENT role clears.
--
-- 'incremental' remains ABSENT for the reason 085 gave: it needs a merge key and a
-- watermark, and shipping the enum value before the behaviour would let a user pick a
-- mode that silently does something else.

-- Replace rather than add: the 085 constraint enumerates the modes, so the new value
-- has to go through a drop. DROP … IF EXISTS first makes the pair idempotent.
ALTER TABLE saved_queries DROP CONSTRAINT IF EXISTS saved_queries_materialization_check;
ALTER TABLE saved_queries
    ADD CONSTRAINT saved_queries_materialization_check
    CHECK (materialization IN ('none', 'table', 'statement'));

-- The target-table requirement was written as "everything except 'none' must name a
-- destination". That was equivalent while 'table' was the only other mode and is wrong
-- now: a 'statement' model names its destination inside its own SQL and must be allowed
-- to carry no target_table at all. Stated positively so a fourth mode has to opt IN to
-- needing a target rather than inheriting the requirement by accident.
ALTER TABLE saved_queries DROP CONSTRAINT IF EXISTS saved_queries_target_table_required;
ALTER TABLE saved_queries
    ADD CONSTRAINT saved_queries_target_table_required
    CHECK (materialization <> 'table' OR NULLIF(TRIM(target_table), '') IS NOT NULL);

COMMENT ON COLUMN saved_queries.materialization IS
    'none = plain saved query; table = CREATE TABLE AS rebuild into target_table; statement = run the stored SQL as written';
