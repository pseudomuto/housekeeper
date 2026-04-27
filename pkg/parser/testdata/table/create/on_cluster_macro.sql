CREATE TABLE `steam_market_data` ON CLUSTER '{cluster}' (
    `ts`    DateTime64(6),
    `price` Int64
)
ENGINE = ReplicatedReplacingMergeTree()
ORDER BY `ts`;
