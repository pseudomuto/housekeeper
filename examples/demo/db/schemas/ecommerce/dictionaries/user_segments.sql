-- User segments dictionary for marketing
CREATE DICTIONARY ecommerce.user_segments_dict ON CLUSTER demo (
    user_id UInt64,
    segment String,
    score Float32 DEFAULT 0.0,
    last_updated DateTime
) PRIMARY KEY user_id
SOURCE(CLICKHOUSE(
    host 'localhost'
    port 9000
    user 'default'
    password ''
    db 'ecommerce'
    table 'user_segments_source'
))
LAYOUT(FLAT())
LIFETIME(1800)
SETTINGS(max_threads = 2)
COMMENT 'User segmentation from ML service';
