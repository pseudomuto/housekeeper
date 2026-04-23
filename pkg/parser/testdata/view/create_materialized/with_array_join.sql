CREATE MATERIALIZED VIEW `analytics`.`attr_totals`
ENGINE = MergeTree() ORDER BY (`event_id`, `attr_name`)
AS SELECT
    `event_id`,
    `attr_name`,
    `attr_value`
FROM `analytics`.`events`
ARRAY JOIN mapKeys(`attributes`) AS `attr_name`, mapValues(`attributes`) AS `attr_value`;
