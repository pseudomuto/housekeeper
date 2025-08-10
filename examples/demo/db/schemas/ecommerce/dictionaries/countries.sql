-- Country lookup dictionary
CREATE DICTIONARY ecommerce.countries_dict ON CLUSTER demo (
    code String,
    name String,
    continent String,
    population UInt64 DEFAULT 0
) PRIMARY KEY code
SOURCE(HTTP(
    url 'https://restcountries.com/v3.1/all'
    format 'JSONEachRow'
))
LAYOUT(HASHED())
LIFETIME(86400)
COMMENT 'Country code to name mapping';
