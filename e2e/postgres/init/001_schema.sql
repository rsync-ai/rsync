-- E2E PostgreSQL seed schema + destination tables
-- Database is created by POSTGRES_DB=e2e_db

-- Destination table for MySQL → Postgres pipeline tests
CREATE TABLE IF NOT EXISTS big_table_copy (
  id SERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Destination table for other tests
CREATE TABLE IF NOT EXISTS test_destination (
  id SERIAL PRIMARY KEY,
  data JSONB,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- CDC destinations (used when sink routes by source table name)
CREATE TABLE IF NOT EXISTS big_table (
  id INTEGER PRIMARY KEY,
  payload TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS digits (
  d SMALLINT PRIMARY KEY
);

-- Grant permissions to e2e_user (created by POSTGRES_USER env var)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO e2e_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO e2e_user;

