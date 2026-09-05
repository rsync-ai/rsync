-- Add sync_mode, cdc_mode, and description columns to connections table

-- sync_mode: "batch" or "cdc" for source connections
ALTER TABLE connections ADD COLUMN IF NOT EXISTS sync_mode character varying(20) DEFAULT 'batch';

-- cdc_mode: "initial" (full snapshot first) or "streaming_only" (no snapshot)
-- Only applicable when sync_mode = 'cdc'
ALTER TABLE connections ADD COLUMN IF NOT EXISTS cdc_mode character varying(30) DEFAULT 'initial';

-- description: Optional user-provided description
ALTER TABLE connections ADD COLUMN IF NOT EXISTS description text;

-- Add check constraint for sync_mode
ALTER TABLE connections DROP CONSTRAINT IF EXISTS connections_sync_mode_check;
ALTER TABLE connections ADD CONSTRAINT connections_sync_mode_check 
    CHECK (sync_mode IS NULL OR sync_mode IN ('batch', 'cdc'));

-- Add check constraint for cdc_mode
ALTER TABLE connections DROP CONSTRAINT IF EXISTS connections_cdc_mode_check;
ALTER TABLE connections ADD CONSTRAINT connections_cdc_mode_check 
    CHECK (cdc_mode IS NULL OR cdc_mode IN ('initial', 'streaming_only'));

-- Create index for filtering by sync_mode
CREATE INDEX IF NOT EXISTS idx_connections_sync_mode ON connections(sync_mode);
