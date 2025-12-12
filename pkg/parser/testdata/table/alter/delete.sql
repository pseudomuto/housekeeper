ALTER TABLE `logs`
    DELETE WHERE `timestamp` < now();
