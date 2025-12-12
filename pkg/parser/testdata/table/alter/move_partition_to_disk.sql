ALTER TABLE `analytics`.`events`
    MOVE PARTITION '202301' TO DISK 'cold_storage';
