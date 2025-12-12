CREATE DICTIONARY `user_segments_dict` (
    `user_id` UInt64,
    `segment` String,
    `score`   Float64
)
PRIMARY KEY `user_id`
SOURCE(HTTP(url 'http://ml-service:8080/user-segments' format 'TabSeparated' credentials(user 'user' password 'password') headers(header(name 'API-KEY' value 'key'))))
LAYOUT(HASHED())
LIFETIME(3600);
