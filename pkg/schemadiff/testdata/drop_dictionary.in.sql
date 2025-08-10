-- Current state: temporary dictionary exists
CREATE DICTIONARY analytics.temp_dict (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://temp.com')) LAYOUT(FLAT()) LIFETIME(600);
-- Target state: dictionary should be removed
-- empty target state