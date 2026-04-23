SELECT
    `attr_name`,
    `attr_value`
FROM `analytics`.`events`
ARRAY JOIN mapKeys(`attributes`) AS `attr_name`, mapValues(`attributes`) AS `attr_value`;
