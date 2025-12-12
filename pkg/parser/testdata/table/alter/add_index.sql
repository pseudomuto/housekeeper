ALTER TABLE `logs`
    ADD INDEX `level_idx` `level` TYPE `minmax` GRANULARITY 1;
