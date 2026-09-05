-- Migration: Insert Default User for Development
-- This creates a default user that the system uses when authentication is not enforced

-- Insert default user (idempotent - only if not exists)
INSERT INTO users (id, email, password_hash, role, created_at, updated_at)
VALUES (
    '00000000-0000-0000-0000-000000000000'::uuid,
    'default@rsync-ai.local',
    '$2a$10$wEAsBOF81BxdOovfGBoWqOJz4TlpnEQonn3TLVUbOLr3uozAEMROq', -- bcrypt hash for dev password: password123
    'admin',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Also insert a common test user ID used by orchestrator
INSERT INTO users (id, email, password_hash, role, created_at, updated_at)
VALUES (
    '00000000-0000-0000-0000-000000000001'::uuid,
    'test@rsync-ai.local',
    '$2a$10$wEAsBOF81BxdOovfGBoWqOJz4TlpnEQonn3TLVUbOLr3uozAEMROq', -- bcrypt hash for dev password: password123
    'admin',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Create an index on email if not exists (for login lookups)
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
