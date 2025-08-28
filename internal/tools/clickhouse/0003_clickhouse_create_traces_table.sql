CREATE TABLE IF NOT EXISTS traces (
    `chain_id` UInt256,
    `block_number` UInt256,
    `block_hash` FixedString(66),
    `block_timestamp` DateTime CODEC(Delta, ZSTD),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `subtraces` Int64,
    `trace_address` Array(Int64),
    `type` LowCardinality(String),
    `call_type` LowCardinality(String),
    `error` Nullable(String),
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `gas` UInt64,
    `gas_used` UInt64,
    `input` String,
    `output` Nullable(String),
    `value` UInt256,
    `author` Nullable(FixedString(42)),
    `reward_type` LowCardinality(Nullable(String)),
    `refund_address` Nullable(FixedString(42)),

    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,

    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_block_hash block_hash TYPE bloom_filter GRANULARITY 2,
    INDEX idx_from_address from_address TYPE bloom_filter GRANULARITY 3,
    INDEX idx_to_address to_address TYPE bloom_filter GRANULARITY 3,

    PROJECTION from_address_projection
    (
        SELECT
          _part_offset
        ORDER BY 
          chain_id,
          from_address,
          block_number,
          transaction_hash,
          trace_address
    ),
    PROJECTION to_address_projection
    (
        SELECT
          _part_offset
        ORDER BY 
          chain_id,
          to_address,
          block_number,
          transaction_hash,
          trace_address
    )

) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, transaction_hash, trace_address)
PARTITION BY (chain_id, toStartOfQuarter(block_timestamp))
SETTINGS deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild', allow_part_offset_column_in_projections=1;
