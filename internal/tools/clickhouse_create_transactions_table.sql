CREATE TABLE base.transactions (
    `chain_id` UInt256,
    `hash` FixedString(66),
    `nonce` UInt64,
    `block_hash` FixedString(66),
    `block_number` UInt256,
    `block_timestamp` UInt64 CODEC(Delta, ZSTD),
    `transaction_index` UInt64,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `value` UInt256,
    `gas` UInt128,
    `gas_price` UInt128,
    `input` String,
    `max_fee_per_gas` UInt128,
    `max_priority_fee_per_gas` UInt128,
    `transaction_type` Int64,
    `is_deleted` UInt8 DEFAULT 0,
    `insert_timestamp` DateTime DEFAULT now(),
    INDEX block_timestamp_idx block_timestamp TYPE minmax GRANULARITY 1,
    INDEX transaction_hash_idx hash TYPE bloom_filter GRANULARITY 1,
) ENGINE = SharedReplacingMergeTree(
    '/clickhouse/tables/{uuid}/{shard}',
    '{replica}',
    insert_timestamp,
    is_deleted
)
ORDER BY (chain_id, block_number) SETTINGS index_granularity = 8192