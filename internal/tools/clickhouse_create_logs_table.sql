CREATE TABLE base.logs (
    `chain_id` UInt256,
    `block_number` UInt256,
    `block_hash` FixedString(66),
    `block_timestamp` UInt64 CODEC(Delta, ZSTD),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `log_index` UInt64,
    `address` FixedString(42),
    `data` String,
    `topic_0` Nullable(String),
    `topic_1` Nullable(String),
    `topic_2` Nullable(String),
    `topic_3` Nullable(String),
    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,
    INDEX transaction_hash_idx transaction_hash TYPE bloom_filter GRANULARITY 1,
    INDEX address_idx address TYPE bloom_filter GRANULARITY 1,
    INDEX topic0_idx topic_0 TYPE bloom_filter GRANULARITY 1,
    INDEX topic1_idx topic_1 TYPE bloom_filter GRANULARITY 1,
    INDEX topic2_idx topic_2 TYPE bloom_filter GRANULARITY 1,
    INDEX topic3_idx topic_3 TYPE bloom_filter GRANULARITY 1,
) ENGINE = SharedReplacingMergeTree(
    '/clickhouse/tables/{uuid}/{shard}',
    '{replica}',
    insert_timestamp,
    is_deleted
)
ORDER BY (block_number, transaction_hash, log_index) SETTINGS index_granularity = 8192