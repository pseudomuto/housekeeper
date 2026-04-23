CREATE VIEW `analytics`.`filtered_events`
AS SELECT
    `region_id`,
    `org_id`,
    `user_id`,
    `product`,
    `timestamp`,
    CAST(metadata.category, 'String') AS `category`,
    CAST(metadata.end_date, 'Date') AS `end_date`,
    CAST(metadata.detail.sequence_num, 'Nullable(UInt32)') AS `sequence_num`,
    CAST(metadata.participant_ids, 'Array(String)') AS `participant_ids`
FROM `analytics`.`events`
WHERE CAST(metadata.region, 'String') = 'us_west';
