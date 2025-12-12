CREATE TABLE `events_distributed` ON CLUSTER `production` AS `events_local`
ENGINE = Distributed(`production`, currentDatabase(), `events_local`, rand());
