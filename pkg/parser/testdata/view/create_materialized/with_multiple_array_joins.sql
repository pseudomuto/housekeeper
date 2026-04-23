CREATE MATERIALIZED VIEW `analytics`.`nested_items_mv`
TO `analytics`.`nested_items`
AS SELECT
    `id`,
    `region_id`,
    `org_id`,
    `user_id`,
    `item`.`name` AS `question`,
    `detail`.`label` AS `detail_label`,
    `detail`.`content` AS `detail_content`,
    `detail`.`position` AS `detail_position`,
    parseDateTime64BestEffortOrZero(`inserted_at`, 3, 'UTC') AS `inserted_at`
FROM `analytics`.`raw_events`
ARRAY JOIN `payload`.`items` AS `item`
ARRAY JOIN `item`.`details` AS `detail`;
