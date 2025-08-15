CREATE TABLE IF NOT EXISTS address_transactions (
    `chain_id` UInt256,
    `hash` FixedString(66),
    `nonce` UInt64,
    `block_hash` FixedString(66),
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta, ZSTD),
    `transaction_index` UInt64,
    `address` FixedString(42),
    `address_type` Enum8('from' = 1, 'to' = 2),
    `value` UInt256,
    `gas` UInt64,
    `gas_price` UInt256,
    `data` String,
    `function_selector` FixedString(10),
    `max_fee_per_gas` UInt128,
    `max_priority_fee_per_gas` UInt128,
    `max_fee_per_blob_gas` UInt256,
    `blob_versioned_hashes` Array(String),
    `transaction_type` UInt8,
    `r` UInt256,
    `s` UInt256,
    `v` UInt256,
    `access_list` Nullable(String),
    `authorization_list` Nullable(String),
    `contract_address` Nullable(FixedString(42)),
    `gas_used` Nullable(UInt64),
    `cumulative_gas_used` Nullable(UInt64),
    `effective_gas_price` Nullable(UInt256),
    `blob_gas_used` Nullable(UInt64),
    `blob_gas_price` Nullable(UInt256),
    `logs_bloom` Nullable(String),
    `status` Nullable(UInt64),

    `sign` Int8 DEFAULT 1,
    `insert_timestamp` DateTime DEFAULT now(),

    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_address_type address_type TYPE bloom_filter GRANULARITY 3
) ENGINE = VersionedCollapsingMergeTree(sign, insert_timestamp)
ORDER BY (chain_id, address, block_number, hash, transaction_index)
PARTITION BY (chain_id, toStartOfQuarter(block_timestamp))
SETTINGS deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild';