-- Session init: run before every script with `sql-client.sh -i /opt/sql/init.sql -f ...`.
-- Flink SQL catalogs are session-scoped (no catalog store here), so each session must
-- (re)create the Fluss catalog. Tables themselves live in Fluss and persist.
CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);
