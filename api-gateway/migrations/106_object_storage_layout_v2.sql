-- Migration 106: schema for object-storage layout v2 (per-pipeline layout version,
-- LOAD file counters and LOAD reservations).
--
-- Additive only. Nothing reads or writes these objects yet, and every pipeline,
-- including a pipeline created after this migration, stays on layout 1. A later
-- migration changes the column default to 2; this one must not.
--
-- pipelines.storage_layout_version
--   Which object-storage key layout a pipeline writes. 1 = the layout every pipeline
--   uses today; 2 = layout v2. A key-builder change is gated on this value so a live
--   pipeline never moves to a new folder shape without a reload.
--
-- object_load_counters: one row per (pipeline, table).
--   table_key           '<db>/[<schema>/]<table>' (layout v2 table key).
--   generation          bumped by a reload of that table; LOAD numbering restarts in
--                       each generation.
--   next_seq            the next LOAD number to hand out in this generation
--                       (LOAD%08d, so 1..100000000).
--   cleaned_generation  the latest generation whose table folder delete succeeded.
--                       -1 = never cleaned. A writer must not write a generation
--                       that has not been cleaned yet.
--
-- object_load_reservations: one row per LOAD number handed out.
--   The same reservation_key in the same (pipeline, table, generation) always maps
--   to the same load_seq, so a redelivered message rewrites the same object instead
--   of taking a new number. Both unique keys include generation: a reload starts a
--   fresh number space without deleting the old rows first.
--   reservation_key grammar (ids and offsets only, never row values):
--     batch          b|<execution_id>|<batch_offset>|<write_unit_index>
--     CDC snapshot   s|<topic>|<partition>|<first_offset>
--   dest_key            the object key written for that LOAD number.
--
-- Both tables cascade with the pipeline. pipelines.id is UUID (001_init_schema.sql).

ALTER TABLE pipelines
    ADD COLUMN IF NOT EXISTS storage_layout_version SMALLINT NOT NULL DEFAULT 1
        CONSTRAINT pipelines_storage_layout_version_check
        CHECK (storage_layout_version IN (1, 2));

CREATE TABLE IF NOT EXISTS object_load_counters (
    pipeline_id        UUID        NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    table_key          TEXT        NOT NULL,
    generation         BIGINT      NOT NULL DEFAULT 0,
    next_seq           BIGINT      NOT NULL DEFAULT 1
        CONSTRAINT object_load_counters_next_seq_check
        CHECK (next_seq BETWEEN 1 AND 100000000),
    cleaned_generation BIGINT      NOT NULL DEFAULT -1,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, table_key)
);

CREATE TABLE IF NOT EXISTS object_load_reservations (
    id              BIGSERIAL     PRIMARY KEY,
    pipeline_id     UUID          NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    table_key       TEXT          NOT NULL,
    generation      BIGINT        NOT NULL,
    reservation_key TEXT          NOT NULL,
    load_seq        BIGINT        NOT NULL
        CONSTRAINT object_load_reservations_load_seq_check
        CHECK (load_seq BETWEEN 1 AND 100000000),
    execution_id    TEXT,
    dest_key        VARCHAR(1024),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_object_load_reservations_key
        UNIQUE (pipeline_id, table_key, generation, reservation_key),
    CONSTRAINT uq_object_load_reservations_seq
        UNIQUE (pipeline_id, table_key, generation, load_seq)
);
