-- Optional convenience DB for UI/manual testing
-- Ensures `e2e_user` can connect to `logistics_db` when users pick that DB in the UI.

CREATE DATABASE IF NOT EXISTS logistics_db;

-- Ensure user exists + password matches compose env (idempotent).
-- Pin mysql_native_password (see 001_schema.sql for why) so a re-run here can't
-- silently flip e2e_user back to caching_sha2_password.
CREATE USER IF NOT EXISTS 'e2e_user'@'%' IDENTIFIED WITH mysql_native_password BY 'e2e_password';
ALTER USER 'e2e_user'@'%' IDENTIFIED WITH mysql_native_password BY 'e2e_password';

GRANT ALL PRIVILEGES ON logistics_db.* TO 'e2e_user'@'%';
FLUSH PRIVILEGES;

-- Lightweight sample schema + data so table discovery works in the UI
-- (and CDC/batch tests have something deterministic to sync).
USE logistics_db;CREATE TABLE IF NOT EXISTS logistics_customers (
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
);CREATE TABLE IF NOT EXISTS logistics_shipments (
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
);CREATE TABLE IF NOT EXISTS logistics_tracking_events (
  event_id BIGINT NOT NULL PRIMARY KEY,
  shipment_id BIGINT NOT NULL,
  event_time TIMESTAMP NOT NULL,
  event_type ENUM('picked_up','departed','arrived','out_for_delivery','delivered','exception') NOT NULL,
  location VARCHAR(64) NOT NULL,
  details JSON NULL,
  KEY idx_events_shipment (shipment_id),
  KEY idx_events_time (event_time)
);INSERT INTO logistics_customers (customer_id, company_name, region, created_at) VALUES
  (1, 'Company 0001', 'NA',  NOW() - INTERVAL 30 DAY),
  (2, 'Company 0002', 'EU',  NOW() - INTERVAL 25 DAY),
  (3, 'Company 0003', 'APAC',NOW() - INTERVAL 20 DAY),
  (4, 'Company 0004', 'MEA', NOW() - INTERVAL 15 DAY),
  (5, 'Company 0005', 'LATAM',NOW() - INTERVAL 10 DAY),
  (6, 'Company 0006', 'IN',  NOW() - INTERVAL 9 DAY),
  (7, 'Company 0007', 'NA',  NOW() - INTERVAL 8 DAY),
  (8, 'Company 0008', 'EU',  NOW() - INTERVAL 7 DAY),
  (9, 'Company 0009', 'APAC',NOW() - INTERVAL 6 DAY),
  (10,'Company 0010', 'MEA', NOW() - INTERVAL 5 DAY)
ON DUPLICATE KEY UPDATE company_name=VALUES(company_name), region=VALUES(region), created_at=VALUES(created_at);INSERT INTO logistics_orders (order_id, customer_id, order_date, status, total_weight_kg, priority) VALUES
  (1001, 1,  CURDATE() - INTERVAL 14 DAY, 'created',   12.50, 'normal'),
  (1002, 1,  CURDATE() - INTERVAL 13 DAY, 'packed',    8.20, 'high'),
  (1003, 2,  CURDATE() - INTERVAL 12 DAY, 'shipped',  22.10, 'normal'),
  (1004, 2,  CURDATE() - INTERVAL 11 DAY, 'delivered', 5.75, 'low'),
  (1005, 3,  CURDATE() - INTERVAL 10 DAY, 'shipped',  18.00, 'high'),
  (1006, 3,  CURDATE() - INTERVAL 9  DAY, 'packed',    3.40, 'normal'),
  (1007, 4,  CURDATE() - INTERVAL 8  DAY, 'created',  44.90, 'normal'),
  (1008, 4,  CURDATE() - INTERVAL 7  DAY, 'cancelled', 2.10, 'low'),
  (1009, 5,  CURDATE() - INTERVAL 6  DAY, 'delivered', 9.99, 'normal'),
  (1010, 5,  CURDATE() - INTERVAL 5  DAY, 'shipped',  11.11, 'high'),
  (1011, 6,  CURDATE() - INTERVAL 4  DAY, 'shipped',  16.70, 'normal'),
  (1012, 6,  CURDATE() - INTERVAL 3  DAY, 'packed',    7.30, 'low'),
  (1013, 7,  CURDATE() - INTERVAL 2  DAY, 'created',  30.00, 'high'),
  (1014, 7,  CURDATE() - INTERVAL 2  DAY, 'created',   1.25, 'low'),
  (1015, 8,  CURDATE() - INTERVAL 1  DAY, 'packed',    2.80, 'normal'),
  (1016, 8,  CURDATE() - INTERVAL 1  DAY, 'shipped',   6.60, 'normal'),
  (1017, 9,  CURDATE(),               'created',  19.40, 'high'),
  (1018, 9,  CURDATE(),               'created',   4.40, 'low'),
  (1019, 10, CURDATE(),               'packed',   33.33, 'high'),
  (1020, 10, CURDATE() - INTERVAL 1 DAY, 'delivered',  14.25, 'normal')
ON DUPLICATE KEY UPDATE customer_id=VALUES(customer_id), order_date=VALUES(order_date), status=VALUES(status), total_weight_kg=VALUES(total_weight_kg), priority=VALUES(priority);

INSERT INTO logistics_shipments (shipment_id, order_id, carrier, service_level, origin, destination, status, shipped_at, delivered_at) VALUES
  (5001, 1001, 'DHL',    'ground',  'NYC', 'SFO', 'label_created', NOW() - INTERVAL 2 HOUR, NULL),
  (5002, 1002, 'FedEx',  'express', 'SFO', 'NYC', 'in_transit',    NOW() - INTERVAL 12 HOUR, NULL),
  (5003, 1003, 'UPS',    'air',     'LON', 'FRA', 'in_transit',    NOW() - INTERVAL 1 DAY, NULL),
  (5004, 1004, 'DHL',    'ground',  'FRA', 'LON', 'delivered',     NOW() - INTERVAL 5 DAY, NOW() - INTERVAL 4 DAY),
  (5005, 1005, 'BlueDart','express','DXB', 'BLR', 'in_transit',    NOW() - INTERVAL 8 HOUR, NULL),
  (5006, 1005, 'BlueDart','ground', 'DXB', 'BLR', 'label_created', NOW() - INTERVAL 2 HOUR, NULL),
  (5007, 1006, 'UPS',    'air',     'BLR', 'DXB', 'label_created', NOW() - INTERVAL 3 HOUR, NULL),
  (5008, 1007, 'FedEx',  'ground',  'NYC', 'LON', 'exception',     NOW() - INTERVAL 2 DAY, NULL),
  (5009, 1009, 'DHL',    'air',     'SFO', 'FRA', 'delivered',     NOW() - INTERVAL 6 DAY, NOW() - INTERVAL 5 DAY),
  (5010, 1010, 'UPS',    'express', 'FRA', 'SFO', 'in_transit',    NOW() - INTERVAL 18 HOUR, NULL),
  (5011, 1011, 'DHL',    'ground',  'LON', 'NYC', 'in_transit',    NOW() - INTERVAL 20 HOUR, NULL),
  (5012, 1012, 'FedEx',  'air',     'NYC', 'LON', 'label_created', NOW() - INTERVAL 1 HOUR, NULL),
  (5013, 1013, 'UPS',    'ground',  'DXB', 'NYC', 'label_created', NOW() - INTERVAL 30 MINUTE, NULL),
  (5014, 1014, 'DHL',    'express', 'SFO', 'DXB', 'label_created', NOW() - INTERVAL 45 MINUTE, NULL),
  (5015, 1015, 'BlueDart','ground', 'BLR', 'LON', 'in_transit',    NOW() - INTERVAL 6 HOUR, NULL),
  (5016, 1016, 'FedEx',  'express', 'LON', 'BLR', 'in_transit',    NOW() - INTERVAL 9 HOUR, NULL),
  (5017, 1017, 'UPS',    'air',     'FRA', 'DXB', 'label_created', NOW() - INTERVAL 10 MINUTE, NULL),
  (5018, 1018, 'DHL',    'ground',  'DXB', 'FRA', 'label_created', NOW() - INTERVAL 15 MINUTE, NULL),
  (5019, 1019, 'FedEx',  'ground',  'NYC', 'BLR', 'in_transit',    NOW() - INTERVAL 4 HOUR, NULL),
  (5020, 1020, 'BlueDart','express','BLR', 'NYC', 'delivered',     NOW() - INTERVAL 3 DAY, NOW() - INTERVAL 2 DAY),
  (5021, 1003, 'UPS',    'ground',  'LON', 'FRA', 'label_created', NOW() - INTERVAL 50 MINUTE, NULL),
  (5022, 1006, 'DHL',    'ground',  'BLR', 'DXB', 'in_transit',    NOW() - INTERVAL 7 HOUR, NULL),
  (5023, 1010, 'DHL',    'ground',  'FRA', 'SFO', 'label_created', NOW() - INTERVAL 2 HOUR, NULL),
  (5024, 1011, 'FedEx',  'air',     'LON', 'NYC', 'label_created', NOW() - INTERVAL 2 HOUR, NULL),
  (5025, 1017, 'UPS',    'express', 'FRA', 'DXB', 'in_transit',    NOW() - INTERVAL 1 HOUR, NULL),
  (5026, 1018, 'DHL',    'air',     'DXB', 'FRA', 'in_transit',    NOW() - INTERVAL 1 HOUR, NULL),
  (5027, 1019, 'FedEx',  'express', 'NYC', 'BLR', 'label_created', NOW() - INTERVAL 35 MINUTE, NULL),
  (5028, 1016, 'BlueDart','ground', 'LON', 'BLR', 'label_created', NOW() - INTERVAL 2 HOUR, NULL),
  (5029, 1001, 'DHL',    'ground',  'NYC', 'SFO', 'in_transit',    NOW() - INTERVAL 1 HOUR, NULL),
  (5030, 1002, 'FedEx',  'express', 'SFO', 'NYC', 'exception',     NOW() - INTERVAL 1 DAY, NULL)
ON DUPLICATE KEY UPDATE order_id=VALUES(order_id), carrier=VALUES(carrier), service_level=VALUES(service_level), origin=VALUES(origin), destination=VALUES(destination), status=VALUES(status), shipped_at=VALUES(shipped_at), delivered_at=VALUES(delivered_at);

INSERT INTO logistics_tracking_events (event_id, shipment_id, event_time, event_type, location, details) VALUES
  (9001, 5002, NOW() - INTERVAL 11 HOUR, 'picked_up',        'SFO', JSON_OBJECT('note','Picked up from warehouse','code',1)),
  (9002, 5002, NOW() - INTERVAL 10 HOUR, 'departed',         'SFO', JSON_OBJECT('note','Departed origin','code',2)),
  (9003, 5002, NOW() - INTERVAL 5  HOUR, 'arrived',          'DEN', JSON_OBJECT('note','Arrived hub','code',3)),
  (9004, 5002, NOW() - INTERVAL 1  HOUR, 'out_for_delivery', 'NYC', JSON_OBJECT('note','Out for delivery','code',4)),
  (9005, 5004, NOW() - INTERVAL 5  DAY,  'picked_up',        'FRA', JSON_OBJECT('note','Picked up','code',1)),
  (9006, 5004, NOW() - INTERVAL 4  DAY,  'delivered',        'LON', JSON_OBJECT('note','Delivered','code',0)),
  (9007, 5008, NOW() - INTERVAL 2  DAY,  'picked_up',        'NYC', JSON_OBJECT('note','Picked up','code',1)),
  (9008, 5008, NOW() - INTERVAL 1  DAY,  'exception',       'LON', JSON_OBJECT('note','Customs hold','code',13)),
  (9009, 5020, NOW() - INTERVAL 3  DAY,  'picked_up',        'BLR', JSON_OBJECT('note','Picked up','code',1)),
  (9010, 5020, NOW() - INTERVAL 2  DAY,  'delivered',        'NYC', JSON_OBJECT('note','Delivered','code',0))
ON DUPLICATE KEY UPDATE shipment_id=VALUES(shipment_id), event_time=VALUES(event_time), event_type=VALUES(event_type), location=VALUES(location), details=VALUES(details);