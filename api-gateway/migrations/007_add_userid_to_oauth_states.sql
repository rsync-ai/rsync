-- Add user_id to oauth_states
ALTER TABLE oauth_states ADD COLUMN IF NOT EXISTS user_id VARCHAR(255);

