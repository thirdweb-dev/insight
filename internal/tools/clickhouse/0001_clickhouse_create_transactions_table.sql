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

    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,

    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_block_hash block_hash TYPE bloom_filter GRANULARITY 3,
    INDEX idx_hash hash TYPE bloom_filter GRANULARITY 2,
    INDEX idx_from_address from_address TYPE bloom_filter GRANULARITY 4,
    INDEX idx_to_address to_address TYPE bloom_filter GRANULARITY 4,
    INDEX idx_function_selector function_selector TYPE bloom_filter GRANULARITY 2,

    PROJECTION from_address_projection
    (
        SELECT
          *
        ORDER BY 
          chain_id,
          from_address,
          block_number,
          hash
    ),
    PROJECTION to_address_projection
    (
        SELECT
          *
        ORDER BY
          chain_id,
          to_address,
          block_number,
          hash
    ),
    PROJECTION from_address_state_projection
    (
        SELECT
          chain_id,
          from_address,
          countState() AS tx_count_state,
          minState(block_number) AS min_block_number_state,
          minState(block_timestamp) AS min_block_timestamp_state,
          maxState(block_number) AS max_block_number_state,
          maxState(block_timestamp) AS max_block_timestamp_state
        GROUP BY
          chain_id,
          from_address
    ),
    PROJECTION to_address_state_projection
    (
        SELECT
          chain_id,
          to_address,
          countState() AS tx_count_state,
          minState(block_number) AS min_block_number_state,
          minState(block_timestamp) AS min_block_timestamp_state,
          maxState(block_number) AS max_block_number_state,
          maxState(block_timestamp) AS max_block_timestamp_state
        GROUP BY
          chain_id,
          to_address
    )
) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, block_number, hash)
PARTITION BY (chain_id, toStartOfQuarter(block_timestamp))
SETTINGS deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild';