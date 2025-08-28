CREATE TABLE IF NOT EXISTS blocks (
    `chain_id` UInt256,
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta, ZSTD),
    `hash` FixedString(66),
    `parent_hash` FixedString(66),
    `sha3_uncles` FixedString(66),
    `nonce` FixedString(18),
    `mix_hash` FixedString(66),
    `miner` FixedString(42),
    `state_root` FixedString(66),
    `transactions_root` FixedString(66),
    `receipts_root` FixedString(66),
    `logs_bloom` String,
    `size` UInt64,
    `extra_data` String,
    `difficulty` UInt256,
    `total_difficulty` UInt256,
    `transaction_count` UInt64,
    `gas_limit` UInt256,
    `gas_used` UInt256,
    `withdrawals_root` FixedString(66),
    `base_fee_per_gas` Nullable(UInt64),

    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,

    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_hash hash TYPE bloom_filter GRANULARITY 2,

    PROJECTION chain_state_projection
    (
        SELECT
          chain_id,
          count() AS count,
          uniqExact(block_number) AS unique_block_count,
          min(block_number) AS min_block_number,
          min(block_timestamp) AS min_block_timestamp,
          max(block_number) AS max_block_number,
          max(block_timestamp) AS max_block_timestamp
        GROUP BY
          chain_id
    )


) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, block_number)
PARTITION BY (chain_id, toStartOfQuarter(block_timestamp))
SETTINGS deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild';