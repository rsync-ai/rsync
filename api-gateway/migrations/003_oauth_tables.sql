-- OAuth Tables Migration
-- Created: December 6, 2025

-- OAuth States (for CSRF protection)
CREATE TABLE IF NOT EXISTS oauth_states (
    state VARCHAR(255) PRIMARY KEY,
    provider VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

-- Create index for cleanup
CREATE INDEX IF NOT EXISTS idx_oauth_states_expires_at ON oauth_states(expires_at);

-- OAuth Tokens
CREATE TABLE IF NOT EXISTS oauth_tokens (
    id VARCHAR(100) PRIMARY KEY,
    provider VARCHAR(50) NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT,
    token_type VARCHAR(50) DEFAULT 'Bearer',
    expires_at TIMESTAMP NOT NULL,
    scopes TEXT,
    account_id VARCHAR(255),
    account_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_oauth_tokens_provider ON oauth_tokens(provider);
CREATE INDEX IF NOT EXISTS idx_oauth_tokens_expires_at ON oauth_tokens(expires_at);

-- Cleanup function for expired states
CREATE OR REPLACE FUNCTION cleanup_expired_oauth_states()
RETURNS void AS $$
BEGIN
    DELETE FROM oauth_states WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-- Add comment for documentation
COMMENT ON TABLE oauth_tokens IS 'Stores encrypted OAuth tokens for external API integrations';
COMMENT ON TABLE oauth_states IS 'Temporary storage for OAuth state tokens (CSRF protection)';

