-- 096_saved_query_pending_edits.sql
--
-- An approval gate for the SQL of a saved query that is on a schedule.
--
-- The problem this closes: a scheduled saved query is production infrastructure —
-- it rebuilds a table other people read. But editing one was a single PUT, and the
-- next run silently used the new SQL. Anyone who could edit the query could change
-- what a table means without anybody reviewing it, and the only trace was a version
-- row nobody had reason to look at.
--
-- Why a separate table instead of columns on saved_queries:
--
--   saved_queries.sql_text remains THE APPROVED TEXT, always. That is deliberate and
--   it is the whole design. The run path reads sq.sql_text directly
--   (loadSavedQueryModel in saved_query_models.go), so leaving that column meaning
--   exactly what it meant before means the execution path — which re-classifies the
--   statement and re-checks the run-as user's role on every run — needs no change at
--   all. A proposed edit cannot reach a scheduled run because it is not in the column
--   the run reads. An approval gate whose enforcement lives in a handler could be
--   bypassed by any future code path that writes sql_text; this one cannot, because
--   there is nothing to bypass.
--
-- Rows are kept after review rather than deleted: a rejected proposal is the most
-- interesting kind. "Who tried to change this and who said no" is exactly the
-- question an audit asks, and it costs one narrow table to be able to answer it.
--
-- The runner (internal/db/migrate.go) wraps this file in one transaction. Do NOT
-- start one here: migrate.go decides a file is self-managed by searching the raw
-- text for a transaction-start keyword and does not skip comments while looking, so
-- even naming that keyword in a comment changes how this file is applied (see the
-- header of 084, where exactly that happened).
-- Idempotent: CREATE TABLE/INDEX IF NOT EXISTS throughout.

CREATE TABLE IF NOT EXISTS saved_query_pending_edits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saved_query_id UUID NOT NULL REFERENCES saved_queries(id) ON DELETE CASCADE,

    -- The proposal itself.
    sql_text TEXT NOT NULL,

    -- Advisory only, exactly as on saved_queries: a snapshot for display. Approval
    -- re-classifies the text it is about to write, so a stale or forged class here
    -- can never widen what the approved query is allowed to do.
    statement_class TEXT NOT NULL DEFAULT 'unknown',

    -- The live sql_text at the moment this was proposed. Kept so the reviewer can be
    -- told the query moved underneath the proposal — someone restored a version, or
    -- another proposal was approved first. Without it, approving a stale proposal
    -- silently reverts whatever landed in between, and the diff the approver read
    -- would not be the change the approval actually made.
    base_sql_text TEXT NOT NULL,

    -- Why the proposer thinks this should change. Optional, but it is the field that
    -- makes the review a review rather than a rubber stamp.
    note TEXT,

    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'rejected')),

    -- No FK on any of these, matching saved_queries.created_by (084): a proposal and
    -- its verdict stay readable after the account that made it is removed.
    proposed_by UUID NOT NULL,
    proposed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_by UUID,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,

    -- A verdict is a reviewer plus a time, or it is not a verdict. Pairing them in one
    -- constraint stops a half-written review from reading as "approved by nobody".
    CONSTRAINT saved_query_pending_edits_review_complete CHECK (
        (status =  'pending' AND reviewed_by IS     NULL AND reviewed_at IS     NULL) OR
        (status <> 'pending' AND reviewed_by IS NOT NULL AND reviewed_at IS NOT NULL)
    )
);

-- At most one open proposal per query. Two proposals in flight would mean the second
-- approval silently discards the first, and there is no sensible merge of two SQL
-- rewrites. The next proposer sees the open one and takes it up with its author.
-- Partial, so reviewed rows accumulate freely as history.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sq_pending_edits_one_open
    ON saved_query_pending_edits(saved_query_id)
    WHERE status = 'pending';

-- The read is always "this query's proposals, newest first".
CREATE INDEX IF NOT EXISTS idx_sq_pending_edits_query
    ON saved_query_pending_edits(saved_query_id, proposed_at DESC);

COMMENT ON TABLE saved_query_pending_edits IS
    'Proposed SQL edits to scheduled saved queries, awaiting admin approval; saved_queries.sql_text stays the approved text';
COMMENT ON COLUMN saved_query_pending_edits.base_sql_text IS
    'Live sql_text when proposed — lets the reviewer be warned the query changed since';
COMMENT ON COLUMN saved_query_pending_edits.statement_class IS
    'Advisory snapshot for UI badges only — approval re-classifies the text it writes';
