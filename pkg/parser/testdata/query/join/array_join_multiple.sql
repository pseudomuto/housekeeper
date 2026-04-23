SELECT
    `item`.`name` AS `question`,
    `detail`.`label` AS `detail_label`,
    `detail`.`content` AS `detail_content`
FROM `analytics`.`raw_events`
ARRAY JOIN `payload`.`items` AS `item`
ARRAY JOIN `item`.`details` AS `detail`;
