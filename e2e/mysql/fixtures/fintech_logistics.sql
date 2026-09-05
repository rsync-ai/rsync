-- Fintech + Logistics fixtures for E2E MySQL
-- Creates two databases:
--   - fintech_db (customers, accounts, transactions, chargebacks)
--   - logistics_db (orders, shipments, tracking_events)
--
-- Deterministic seed: uses digits cross-joins and CRC32-derived values.

-- -----------------------------
-- Shared helper: digits 0..9
-- -----------------------------
DROP DATABASE IF EXISTS fintech_db;
DROP DATABASE IF EXISTS logistics_db;
CREATE DATABASE fintech_db;
CREATE DATABASE logistics_db;

CREATE TABLE IF NOT EXISTS fintech_db.digits (d TINYINT NOT NULL PRIMARY KEY);
INSERT INTO fintech_db.digits (d) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
ON DUPLICATE KEY UPDATE d = VALUES(d);

CREATE TABLE IF NOT EXISTS logistics_db.digits (d TINYINT NOT NULL PRIMARY KEY);
INSERT INTO logistics_db.digits (d) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
ON DUPLICATE KEY UPDATE d = VALUES(d);

-- Ensure the non-root E2E user can read/write these databases (for connectors/tests).
GRANT ALL PRIVILEGES ON fintech_db.* TO 'e2e_user'@'%';
GRANT ALL PRIVILEGES ON logistics_db.* TO 'e2e_user'@'%';
FLUSH PRIVILEGES;

-- -----------------------------
-- Fintech domain (MySQL)
-- -----------------------------
USE fintech_db;

CREATE TABLE IF NOT EXISTS fintech_customers (
  customer_id BIGINT NOT NULL PRIMARY KEY,
  full_name VARCHAR(128) NOT NULL,
  email VARCHAR(200) NOT NULL,
  country CHAR(2) NOT NULL,
  kyc_status ENUM('pending','verified','rejected') NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fintech_accounts (
  account_id BIGINT NOT NULL PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  account_type ENUM('checking','savings','card') NOT NULL,
  currency CHAR(3) NOT NULL,
  balance_cents BIGINT NOT NULL,
  status ENUM('active','frozen','closed') NOT NULL,
  opened_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_accounts_customer (customer_id)
);

CREATE TABLE IF NOT EXISTS fintech_merchants (
  merchant_id BIGINT NOT NULL PRIMARY KEY,
  merchant_name VARCHAR(128) NOT NULL,
  mcc INT NOT NULL,
  country CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS fintech_transactions (
  txn_id BIGINT NOT NULL PRIMARY KEY,
  account_id BIGINT NOT NULL,
  merchant_id BIGINT NOT NULL,
  amount_cents BIGINT NOT NULL,
  currency CHAR(3) NOT NULL,
  txn_type ENUM('purchase','refund','p2p','withdrawal','deposit') NOT NULL,
  status ENUM('authorized','settled','failed','reversed') NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NULL,
  KEY idx_txn_account (account_id),
  KEY idx_txn_merchant (merchant_id),
  KEY idx_txn_created (created_at)
);

CREATE TABLE IF NOT EXISTS fintech_chargebacks (
  chargeback_id BIGINT NOT NULL PRIMARY KEY,
  txn_id BIGINT NOT NULL,
  reason_code VARCHAR(16) NOT NULL,
  status ENUM('open','won','lost') NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_cb_txn (txn_id)
);

-- Seed customers (~10k)
INSERT INTO fintech_customers (customer_id, full_name, email, country, kyc_status, created_at)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
  CROSS JOIN digits e
)
SELECT
  n + 1 AS customer_id,
  CONCAT('Customer ', LPAD(n + 1, 5, '0')) AS full_name,
  CONCAT('customer', LPAD(n + 1, 5, '0'), '@example.com') AS email,
  ELT((n % 6) + 1, 'US','IN','GB','DE','SG','AE') AS country,
  ELT((CRC32(CONCAT('kyc-', n)) % 3) + 1, 'pending','verified','rejected') AS kyc_status,
  TIMESTAMPADD(DAY, - (n % 365), CURRENT_TIMESTAMP) AS created_at
FROM nums
WHERE n < 10000;

-- Seed merchants (~250)
INSERT INTO fintech_merchants (merchant_id, merchant_name, mcc, country)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
)
SELECT
  n + 1,
  CONCAT('Merchant ', LPAD(n + 1, 3, '0')),
  4000 + (n % 200),
  ELT((n % 6) + 1, 'US','IN','GB','DE','SG','AE')
FROM nums
WHERE n < 250;

-- Seed accounts (~20k): 2 accounts per customer on average
INSERT INTO fintech_accounts (account_id, customer_id, account_type, currency, balance_cents, status, opened_at)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
  CROSS JOIN digits e
)
SELECT
  n + 1 AS account_id,
  (n % 10000) + 1 AS customer_id,
  ELT((n % 3) + 1, 'checking','savings','card') AS account_type,
  ELT((n % 4) + 1, 'USD','INR','EUR','GBP') AS currency,
  ((CAST(CRC32(CONCAT('bal-', n)) AS SIGNED) % 20000000) - 5000000) AS balance_cents, -- can be negative
  ELT((CRC32(CONCAT('acct-status-', n)) % 3) + 1, 'active','frozen','closed') AS status,
  TIMESTAMPADD(DAY, - (n % 730), CURRENT_TIMESTAMP) AS opened_at
FROM nums
WHERE n < 20000;

-- Seed transactions (~50k)
INSERT INTO fintech_transactions (
  txn_id, account_id, merchant_id, amount_cents, currency, txn_type, status, created_at, updated_at
)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
  CROSS JOIN digits e
)
SELECT
  n + 1 AS txn_id,
  (n % 20000) + 1 AS account_id,
  (n % 250) + 1 AS merchant_id,
  (CRC32(CONCAT('amt-', n)) % 250000) + 100 AS amount_cents,
  ELT((n % 4) + 1, 'USD','INR','EUR','GBP') AS currency,
  ELT((n % 5) + 1, 'purchase','refund','p2p','withdrawal','deposit') AS txn_type,
  ELT((CRC32(CONCAT('txn-status-', n)) % 4) + 1, 'authorized','settled','failed','reversed') AS status,
  TIMESTAMPADD(MINUTE, - (n % 200000), CURRENT_TIMESTAMP) AS created_at,
  NULL AS updated_at
FROM nums
WHERE n < 50000;

-- Seed chargebacks (~2k) tied to transactions
INSERT INTO fintech_chargebacks (chargeback_id, txn_id, reason_code, status, created_at)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
)
SELECT
  n + 1,
  (CRC32(CONCAT('cb-txn-', n)) % 50000) + 1 AS txn_id,
  ELT((n % 5) + 1, 'FRAUD','DUPLICATE','SERVICE','NOT_RECEIVED','OTHER') AS reason_code,
  ELT((CRC32(CONCAT('cb-status-', n)) % 3) + 1, 'open','won','lost') AS status,
  TIMESTAMPADD(DAY, - (n % 180), CURRENT_TIMESTAMP) AS created_at
FROM nums
WHERE n < 2000;

-- -----------------------------
-- Logistics domain (MySQL)
-- -----------------------------
USE logistics_db;

CREATE TABLE IF NOT EXISTS logistics_customers (
  customer_id BIGINT NOT NULL PRIMARY KEY,
  company_name VARCHAR(160) NOT NULL,
  region VARCHAR(32) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS logistics_orders (
  order_id BIGINT NOT NULL PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  order_date DATE NOT NULL,
  status ENUM('created','packed','shipped','delivered','cancelled') NOT NULL,
  total_weight_kg DECIMAL(10,2) NOT NULL,
  priority ENUM('low','normal','high') NOT NULL,
  KEY idx_orders_customer (customer_id),
  KEY idx_orders_date (order_date)
);

CREATE TABLE IF NOT EXISTS logistics_shipments (
  shipment_id BIGINT NOT NULL PRIMARY KEY,
  order_id BIGINT NOT NULL,
  carrier VARCHAR(64) NOT NULL,
  service_level ENUM('ground','air','express') NOT NULL,
  origin VARCHAR(64) NOT NULL,
  destination VARCHAR(64) NOT NULL,
  status ENUM('label_created','in_transit','delivered','exception') NOT NULL,
  shipped_at TIMESTAMP NULL,
  delivered_at TIMESTAMP NULL,
  KEY idx_shipments_order (order_id),
  KEY idx_shipments_status (status)
);

CREATE TABLE IF NOT EXISTS logistics_tracking_events (
  event_id BIGINT NOT NULL PRIMARY KEY,
  shipment_id BIGINT NOT NULL,
  event_time TIMESTAMP NOT NULL,
  event_type ENUM('picked_up','departed','arrived','out_for_delivery','delivered','exception') NOT NULL,
  location VARCHAR(64) NOT NULL,
  details JSON NULL,
  KEY idx_events_shipment (shipment_id),
  KEY idx_events_time (event_time)
);

-- Seed logistics customers (~2k)
INSERT INTO logistics_customers (customer_id, company_name, region, created_at)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
)
SELECT
  n + 1,
  CONCAT('Company ', LPAD(n + 1, 4, '0')),
  ELT((n % 6) + 1, 'NA','EU','APAC','MEA','LATAM','IN') AS region,
  TIMESTAMPADD(DAY, - (n % 365), CURRENT_TIMESTAMP) AS created_at
FROM nums
WHERE n < 2000;

-- Seed orders (~5k)
INSERT INTO logistics_orders (order_id, customer_id, order_date, status, total_weight_kg, priority)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
)
SELECT
  n + 1,
  (n % 2000) + 1,
  DATE_SUB(CURDATE(), INTERVAL (n % 180) DAY) AS order_date,
  ELT((CRC32(CONCAT('order-status-', n)) % 5) + 1, 'created','packed','shipped','delivered','cancelled') AS status,
  ROUND(((CRC32(CONCAT('w-', n)) % 50000) / 100.0) + 0.5, 2) AS total_weight_kg,
  ELT((n % 3) + 1, 'low','normal','high') AS priority
FROM nums
WHERE n < 5000;

-- Seed shipments (~7k) (some orders have multiple shipments)
INSERT INTO logistics_shipments (shipment_id, order_id, carrier, service_level, origin, destination, status, shipped_at, delivered_at)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
)
SELECT
  n + 1,
  (n % 5000) + 1,
  ELT((n % 4) + 1, 'DHL','FedEx','UPS','BlueDart') AS carrier,
  ELT((n % 3) + 1, 'ground','air','express') AS service_level,
  ELT((n % 6) + 1, 'NYC','SFO','LON','FRA','DXB','BLR') AS origin,
  ELT(((n + 3) % 6) + 1, 'NYC','SFO','LON','FRA','DXB','BLR') AS destination,
  ELT((CRC32(CONCAT('ship-status-', n)) % 4) + 1, 'label_created','in_transit','delivered','exception') AS status,
  TIMESTAMPADD(HOUR, - (n % 4000), CURRENT_TIMESTAMP) AS shipped_at,
  NULL
FROM nums
WHERE n < 7000;

-- Seed tracking events (~40k)
INSERT INTO logistics_tracking_events (event_id, shipment_id, event_time, event_type, location, details)
WITH nums AS (
  SELECT (a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d) AS n
  FROM digits a
  CROSS JOIN digits b
  CROSS JOIN digits c
  CROSS JOIN digits d
  CROSS JOIN digits e
)
SELECT
  n + 1,
  (n % 7000) + 1 AS shipment_id,
  TIMESTAMPADD(MINUTE, - (n % 120000), CURRENT_TIMESTAMP) AS event_time,
  ELT((n % 6) + 1, 'picked_up','departed','arrived','out_for_delivery','delivered','exception') AS event_type,
  ELT((n % 6) + 1, 'NYC','SFO','LON','FRA','DXB','BLR') AS location,
  JSON_OBJECT('note', CONCAT('event-', n), 'code', (n % 17)) AS details
FROM nums
WHERE n < 40000;

