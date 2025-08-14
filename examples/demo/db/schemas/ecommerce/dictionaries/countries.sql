-- Countries dictionary with UN membership information using HTTP source
-- Using REST Countries API with cca2, region, and unMember fields
CREATE DICTIONARY ecommerce.countries_dict ON CLUSTER demo (
    cca2 String,
    region String,
    unMember UInt8
) PRIMARY KEY cca2
SOURCE(HTTP(
    url 'https://restcountries.com/v3.1/all?fields=cca2,region,unMember'
    format 'JSONEachRow'
    headers(header(name 'Content-Type' value 'application/json'))
))
LAYOUT(HASHED())
LIFETIME(86400)
COMMENT 'Country codes with regions and UN membership status from REST Countries API using HTTP source';
