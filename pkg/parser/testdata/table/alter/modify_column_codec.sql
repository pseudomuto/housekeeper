ALTER TABLE `events`
    MODIFY COLUMN `timestamp` DateTime64(3, UTC) CODEC(DoubleDelta);
