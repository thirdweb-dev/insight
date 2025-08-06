CREATE TABLE IF NOT EXISTS block_data (
    `chain_id` UInt256,
    `block_number` UInt256,
    `data` String,
    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,
    INDEX idx_block_number block_number TYPE minmax GRANULARITY 1,
) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, block_number)
PARTITION BY chain_id
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;