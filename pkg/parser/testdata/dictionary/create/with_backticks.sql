CREATE DICTIONARY `user-dict`.`order-lookup` (
    `user-id` UInt64 IS_OBJECT_ID,
    `order`   String INJECTIVE,
    `select`  String DEFAULT 'default_value'
)
PRIMARY KEY `user-id`
SOURCE(HTTP(url 'http://api.example.com/orders'))
LAYOUT(HASHED())
LIFETIME(3600);
