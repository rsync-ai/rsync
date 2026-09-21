-- Record the connection ids of pipelines created before the projector learned to.
--
-- When the pipeline workflow resolved a connection by connector type, it kept the id
-- in its own state and never wrote it to the pipelines row, so the pipeline list
-- showed "— → —" for a pipeline that ran end to end. The projector now copies the ids
-- from the connection_validation STAGE_COMPLETED event as it arrives
-- (maybePersistConnectionIDs). This fills the rows that predate that, from the same
-- event already stored in pipeline_run_events.
--
-- The rules match the projector's:
--   * only a NULL column is filled; an id already recorded is never replaced;
--   * the connection must still exist and belong to the pipeline's workspace;
--   * each side is filled independently.
-- When a pipeline ran more than once, the latest event naming a live connection
-- for that side wins.
--
-- The ids are compared as text rather than cast to uuid: the runner stops at the
-- first failing migration, and a malformed id in one old payload must not do that.
-- updated_at is left alone so the backfill does not reorder the pipeline list.

WITH resolved AS (
    SELECT DISTINCT ON (p.id, side.name)
           p.id AS pipeline_id,
           side.name AS side,
           c.id AS connection_id
    FROM pipeline_run_events e
    JOIN pipelines p ON p.id = e.pipeline_id
    CROSS JOIN LATERAL (VALUES
        ('source', e.payload -> 'metadata' ->> 'source_connection_id'),
        ('destination', e.payload -> 'metadata' ->> 'destination_connection_id')
    ) AS side(name, connection_id)
    JOIN connections c
      ON c.id::text = lower(btrim(side.connection_id))
     AND c.workspace_id = p.workspace_id
    WHERE e.event_type = 'STAGE_COMPLETED'
      AND lower(btrim(e.stage_id)) = 'connection_validation'
    ORDER BY p.id, side.name, e.occurred_at DESC, e.seq DESC NULLS LAST
)
UPDATE pipelines p
SET source_connection_id = COALESCE(p.source_connection_id,
        (SELECT r.connection_id FROM resolved r WHERE r.pipeline_id = p.id AND r.side = 'source')),
    destination_connection_id = COALESCE(p.destination_connection_id,
        (SELECT r.connection_id FROM resolved r WHERE r.pipeline_id = p.id AND r.side = 'destination'))
WHERE EXISTS (
    SELECT 1 FROM resolved r
    WHERE r.pipeline_id = p.id
      AND ((r.side = 'source' AND p.source_connection_id IS NULL)
        OR (r.side = 'destination' AND p.destination_connection_id IS NULL))
);
