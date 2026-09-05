-- Warehouse destination tables in E2E Postgres (e2e_db)
-- These tables are designed to receive batch loads from MySQL sources.

CREATE TABLE IF NOT EXISTS fintech_customers (
  customer_id BIGINT PRIMARY KEY,
  full_name TEXT NOT NULL,
  email TEXT NOT NULL,
  country TEXT NOT NULL,
  kyc_status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fintech_accounts (
  account_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  account_type TEXT NOT NULL,
  currency TEXT NOT NULL,
  balance_cents BIGINT NOT NULL,
  status TEXT NOT NULL,
  opened_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fintech_merchants (
  merchant_id BIGINT PRIMARY KEY,
  merchant_name TEXT NOT NULL,
  mcc INTEGER NOT NULL,
  country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fintech_transactions (
  txn_id BIGINT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  merchant_id BIGINT NOT NULL,
  amount_cents BIGINT NOT NULL,
  currency TEXT NOT NULL,
  txn_type TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fintech_chargebacks (
  chargeback_id BIGINT PRIMARY KEY,
  txn_id BIGINT NOT NULL,
  reason_code TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS logistics_customers (
  customer_id BIGINT PRIMARY KEY,
  company_name TEXT NOT NULL,
  region TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS logistics_orders (
  order_id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  order_date DATE NOT NULL,
  status TEXT NOT NULL,
  total_weight_kg NUMERIC(10,2) NOT NULL,
  priority TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS logistics_shipments (
  shipment_id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  carrier TEXT NOT NULL,
  service_level TEXT NOT NULL,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  status TEXT NOT NULL,
  shipped_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS logistics_tracking_events (
  event_id BIGINT PRIMARY KEY,
  shipment_id BIGINT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL,
  location TEXT NOT NULL,
  details JSONB
);

