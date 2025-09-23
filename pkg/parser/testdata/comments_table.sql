CREATE TABLE demo (
  -- A special field
  id UInt32,
  name LowCardinality(String)
)
-- Use MergeTree engine.
ENGINE = MergeTree
-- Primary key/order by is simple.
PRIMARY KEY(id);