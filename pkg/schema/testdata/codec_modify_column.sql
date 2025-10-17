ALTER TABLE `test_table`
    MODIFY COLUMN `event_occurred_at` DateTime64(3, UTC) CODEC(DoubleDelta),
    MODIFY COLUMN `event_received_at` DateTime64(3, UTC) CODEC(DoubleDelta);