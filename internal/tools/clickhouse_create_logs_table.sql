CREATE TABLE logs (
    `chain_id` UInt256,
    `block_number` UInt256,
    `block_hash` FixedString(66),
    `block_timestamp` UInt64 CODEC(Delta, ZSTD),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `log_index` UInt64,
    `address` FixedString(42),
    `data` String,
    `topic_0` String,
    `topic_1` Nullable(String),
    `topic_2` Nullable(String),
    `topic_3` Nullable(String),
    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,
    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_transaction_hash transaction_hash TYPE bloom_filter GRANULARITY 1,
    INDEX idx_block_hash block_hash TYPE bloom_filter GRANULARITY 1,
    INDEX idx_address address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic0 topic_0 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic1 topic_1 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic2 topic_2 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic3 topic_3 TYPE bloom_filter GRANULARITY 1,
) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, block_number, transaction_hash, log_index)
PARTITION BY chain_id
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;