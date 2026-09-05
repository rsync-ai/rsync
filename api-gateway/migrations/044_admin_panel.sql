-- Migration 044: Admin Panel — user management, invitations, instance settings
-- Adds status/name columns to users, invitations table for invite-link flow,
-- and instance_settings table for runtime configuration.

BEGIN;

-- Add status + name to users (safe with IF NOT EXISTS pattern)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'users' AND column_name = 'status'
  ) THEN
    ALTER TABLE users ADD COLUMN status VARCHAR(20) DEFAULT 'active' NOT NULL;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'users' AND column_name = 'name'
  ) THEN
    ALTER TABLE users ADD COLUMN name VARCHAR(255) DEFAULT '' NOT NULL;
  END IF;
END $$;

-- Invitations table (invite-link flow, no SMTP)
CREATE TABLE IF NOT EXISTS invitations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    token VARCHAR(64) NOT NULL UNIQUE,
    email_hint VARCHAR(255),
    role VARCHAR(50) DEFAULT 'user' NOT NULL,
    created_by UUID NOT NULL REFERENCES users(id),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    used_by UUID REFERENCES users(id),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_invitations_token ON invitations(token);

-- Instance settings (key-value runtime config)
CREATE TABLE IF NOT EXISTS instance_settings (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_by UUID REFERENCES users(id),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMIT;
