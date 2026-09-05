-- E2E: Enable Postgres logical replication for CDC tests.
-- Debezium provisioning creates publications + replication slots and needs a REPLICATION role.

ALTER ROLE e2e_user WITH REPLICATION;

