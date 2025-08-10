-- User segments dictionary for marketing
CREATE DICTIONARY ecommerce.user_segments_dict ON CLUSTER demo (
    user_id UInt64,
    segment String,
    score Float32 DEFAULT 0.0,
    last_updated DateTime DEFAULT now()
) PRIMARY KEY user_id
SOURCE(HTTP(
    url 'http://ml-service:8080/user-segments'
    format 'TabSeparated'
    headers(list(header(name 'Authorization' value 'Bearer token123')))
))
LAYOUT(FLAT())
LIFETIME(1800)
SETTINGS(max_threads = 2)
COMMENT 'User segmentation from ML service';
