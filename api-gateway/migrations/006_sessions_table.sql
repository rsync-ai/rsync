-- Migration: Add sessions table for authentication

CREATE TABLE IF NOT EXISTS sessions (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token character varying(255) NOT NULL UNIQUE,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
