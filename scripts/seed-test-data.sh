#!/usr/bin/env bash
# seed-test-data.sh — populate MySQL source with realistic test tables + rows.
# Run once after docker-compose.test-sources.yml is up.
#
# Usage:
#   ./scripts/seed-test-data.sh

set -euo pipefail

echo "=== waiting for MySQL to be ready ==="
until docker exec rsync-ai-mysql-source mysqladmin ping -h localhost -u rsync -prsyncpass --silent 2>/dev/null; do
  sleep 2
done

echo "=== seeding MySQL source (shop_db) ==="
docker exec rsync-ai-mysql-source mysql -u rsync -prsyncpass shop_db <<'SQL'
CREATE TABLE IF NOT EXISTS orders (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  status      VARCHAR(20) NOT NULL DEFAULT 'pending',
  total_cents INT NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  email      VARCHAR(255) NOT NULL UNIQUE,
  name       VARCHAR(255) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  sku         VARCHAR(50) NOT NULL UNIQUE,
  name        VARCHAR(255) NOT NULL,
  price_cents INT NOT NULL,
  stock       INT NOT NULL DEFAULT 0,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT IGNORE INTO customers (email, name) VALUES
  ('alice@example.com',   'Alice Smith'),
  ('bob@example.com',     'Bob Jones'),
  ('carol@example.com',   'Carol White'),
  ('dan@example.com',     'Dan Brown'),
  ('eve@example.com',     'Eve Davis');

INSERT IGNORE INTO products (sku, name, price_cents, stock) VALUES
  ('SKU-001', 'Widget Pro',    2999, 100),
  ('SKU-002', 'Gadget Basic',  1499, 250),
  ('SKU-003', 'Super Doohick', 4999,  50);

INSERT INTO orders (customer_id, status, total_cents) VALUES
  (1, 'completed', 2999),
  (2, 'pending',   1499),
  (3, 'completed', 9998),
  (1, 'shipped',   4999),
  (4, 'pending',   2999),
  (5, 'completed', 1499),
  (2, 'completed', 7998),
  (3, 'pending',   4999);

SELECT 'customers' AS tbl, COUNT(*) AS rows FROM customers
UNION ALL
SELECT 'products',          COUNT(*)         FROM products
UNION ALL
SELECT 'orders',            COUNT(*)         FROM orders;
SQL

echo "=== MySQL seeded ==="
echo ""
echo "Connection details for rsync-ai UI:"
echo "  Host:     localhost (or host.docker.internal from inside Docker)"
echo "  Port:     3306"
echo "  Database: shop_db"
echo "  User:     rsync"
echo "  Password: rsyncpass"
echo ""
echo "Destination PostgreSQL:"
echo "  Host:     localhost"
echo "  Port:     5433"
echo "  Database: dest_db"
echo "  User:     rsync"
echo "  Password: rsyncpass"
