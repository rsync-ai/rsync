-- Add subdomain column to oauth_states so per-shop OAuth providers
-- (e.g. Shopify) can carry the shop name through the authorize→callback round-trip.
ALTER TABLE oauth_states ADD COLUMN IF NOT EXISTS subdomain TEXT;
