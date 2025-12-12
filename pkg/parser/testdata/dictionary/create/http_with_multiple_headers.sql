CREATE DICTIONARY `analytics_dict` (
    `id`   UInt64,
    `data` String
)
PRIMARY KEY `id`
SOURCE(HTTP(url 'https://api.analytics.com/data' format 'JSONEachRow' credentials(user 'api_user' password 'secret123') headers(header(name 'Content-Type' value 'application/json') header(name 'X-Custom-Header' value 'custom-value'))))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 1800);
