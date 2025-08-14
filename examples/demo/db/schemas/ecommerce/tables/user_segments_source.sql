-- Source table for user segments dictionary
CREATE TABLE ecommerce.user_segments_source ON CLUSTER demo (
    user_id UInt64,
    segment String,
    score Float32,
    last_updated DateTime
) ENGINE = Memory()
COMMENT 'Source data for user segments dictionary';