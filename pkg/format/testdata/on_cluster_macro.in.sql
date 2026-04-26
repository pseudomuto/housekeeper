-- Cluster macro references must be preserved verbatim (no backtick quoting)
CREATE TABLE steam_market_data ON CLUSTER '{cluster}' (ts DateTime64(6), price Int64) ENGINE = ReplicatedReplacingMergeTree() ORDER BY ts;

-- Plain cluster names are backtick-quoted
CREATE TABLE logs ON CLUSTER production (ts DateTime, msg String) ENGINE = MergeTree() ORDER BY ts;

-- DROP with macro cluster
DROP TABLE IF EXISTS events ON CLUSTER '{cluster}';
