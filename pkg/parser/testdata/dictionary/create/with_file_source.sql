CREATE DICTIONARY `simple_dict` (
    `key1`   String,
    `key2`   UInt32,
    `value1` String DEFAULT 'N/A',
    `value2` Float64 EXPRESSION '0.0'
)
PRIMARY KEY `key1`, `key2`
SOURCE(File(path '/data/dict.csv' format 'CSV'))
LAYOUT(COMPLEX_KEY_CACHE(size_in_cells 1000000))
LIFETIME(MAX 1200 MIN 300);
