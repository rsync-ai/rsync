-- Optional convenience DB for UI/manual testing
-- Ensures `e2e_user` can connect to `fintech_db` when users pick that DB in the UI.

CREATE DATABASE IF NOT EXISTS fintech_db;

-- Ensure user exists + password matches compose env (idempotent).
-- Pin mysql_native_password (see 001_schema.sql for why).
CREATE USER IF NOT EXISTS 'e2e_user'@'%' IDENTIFIED WITH mysql_native_password BY 'e2e_password';
ALTER USER 'e2e_user'@'%' IDENTIFIED WITH mysql_native_password BY 'e2e_password';

GRANT ALL PRIVILEGES ON fintech_db.* TO 'e2e_user'@'%';
FLUSH PRIVILEGES;

