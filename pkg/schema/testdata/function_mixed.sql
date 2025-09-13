CREATE FUNCTION `new_function` AS (`value`) -> multiply(`value`, 2);

DROP FUNCTION IF EXISTS `old_function`;

ALTER TABLE `analytics`.`events`
    ADD COLUMN `timestamp` DateTime;