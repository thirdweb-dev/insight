CREATE TABLE IF NOT EXISTS transactions (
    `chain_id` UInt256,
    `hash` FixedString(66),
    `nonce` UInt64,
    `block_hash` FixedString(66),
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta, ZSTD),
    `transaction_index` UInt64,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
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
    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 3,
    INDEX idx_block_hash block_hash TYPE bloom_filter GRANULARITY 3,
    INDEX idx_hash hash TYPE bloom_filter GRANULARITY 3,
    INDEX idx_from_address from_address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_to_address to_address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_function_selector function_selector TYPE bloom_filter GRANULARITY 1,
    PROJECTION txs_chainid_from_address
    (
        SELECT *
        ORDER BY 
          chain_id,
          from_address,
          block_number
    ),
    PROJECTION txs_chainid_to_address
    (
        SELECT *
        ORDER BY 
          chain_id,
          to_address,
          block_number,
          hash
    )
) ENGINE = VersionedCollapsingMergeTree(sign, insert_timestamp)
ORDER BY (chain_id, block_number, hash)
PARTITION BY chain_id
SETTINGS deduplicate_merge_projection_mode = 'drop', lightweight_mutation_projection_mode = 'rebuild';