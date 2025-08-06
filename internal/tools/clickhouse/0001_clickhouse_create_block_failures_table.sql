CREATE TABLE IF NOT EXISTS block_failures (
    `chain_id` UInt256,
    `block_number` UInt256,
    `last_error_timestamp` UInt64 CODEC(Delta, ZSTD),
    `count` UInt16,
    `reason` String,
    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,
    INDEX idx_block_number block_number TYPE minmax GRANULARITY 1,
) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, block_number)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;