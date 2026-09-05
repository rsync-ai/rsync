-- E2E MySQL seed schema + data
-- Database is created by MYSQL_DATABASE=e2e_db

USE e2e_db;

CREATE TABLE IF NOT EXISTS big_table (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  payload MEDIUMTEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Generate ~100k rows quickly using cross joins (10^5).
-- Each payload is ~200 bytes to exceed the 10MB MinIO staging threshold.
CREATE TABLE IF NOT EXISTS digits (
  d TINYINT NOT NULL PRIMARY KEY
);

INSERT INTO digits (d)
VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
ON DUPLICATE KEY UPDATE d = VALUES(d);

INSERT INTO big_table (payload)
SELECT RPAD(CONCAT(a.d,b.d,c.d,d.d,e.d,'-e2e-payload-'), 1500, 'x') AS payload
FROM digits a
CROSS JOIN digits b
CROSS JOIN digits c
CROSS JOIN digits d
CROSS JOIN digits e;

-- MySQL 8.4 ships mysql_native_password DISABLED (not removed — that's 9.x). The
-- server is started with --mysql-native-password=ON (see docker-compose.e2e.dbs.yml)
-- so we pin e2e_user to it. caching_sha2_password (the 8.4 default) requires a
-- secure/RSA handshake on the cold-cache first auth, which the Go batch executor
-- (go-sql-driver) cannot complete over the plaintext socket — it fails with
-- "2061 Authentication requires secure connection" until another client warms the
-- cache. mysql_native_password works for every client (Go/Python/JDBC) cold or warm.
ALTER USER 'e2e_user'@'%' IDENTIFIED WITH mysql_native_password BY 'e2e_password';
-- Debezium snapshotting may require FLUSH TABLES WITH READ LOCK, which needs RELOAD/FLUSH_TABLES.
-- Also grant replication privileges required for binlog streaming.
GRANT RELOAD, FLUSH_TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'e2e_user'@'%';
FLUSH PRIVILEGES;

