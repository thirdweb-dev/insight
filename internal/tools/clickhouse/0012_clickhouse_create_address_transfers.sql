CREATE TABLE IF NOT EXISTS address_transfers (
    `chain_id` UInt256,
    `token_type` LowCardinality(String),
    `token_address` FixedString(42),
    `token_id` UInt256,
    `address` FixedString(42),
    `address_type` Enum8('from' = 1, 'to' = 2),
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `amount` UInt256,
    `log_index` UInt64,
    `batch_index` Nullable(UInt16) DEFAULT NULL,

    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` Int8 DEFAULT 0,

    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_address_type address_type TYPE bloom_filter GRANULARITY 3
) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, address, block_number, transaction_hash, transaction_index)
PARTITION BY (chain_id, toStartOfQuarter(block_timestamp))
SETTINGS deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild';