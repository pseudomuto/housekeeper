ALTER TABLE `logs` ON CLUSTER `production`
    ADD COLUMN `server_id` String;
