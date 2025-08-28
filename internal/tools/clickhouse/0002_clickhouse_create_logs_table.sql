CREATE TABLE IF NOT EXISTS logs (
    `chain_id` UInt256,
    `block_number` UInt256,
    `block_hash` FixedString(66),
    `block_timestamp` DateTime CODEC(Delta, ZSTD),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `log_index` UInt64,
    `address` FixedString(42),
    `data` String,
    `topic_0` String,
    `topic_1` String,
    `topic_2` String,
    `topic_3` String,

    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,

    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_block_hash block_hash TYPE bloom_filter GRANULARITY 3,
    INDEX idx_transaction_hash transaction_hash TYPE bloom_filter GRANULARITY 2,
    INDEX idx_address address TYPE bloom_filter GRANULARITY 3,
    INDEX idx_topic0 topic_0 TYPE bloom_filter GRANULARITY 3,
    INDEX idx_topic1 topic_1 TYPE bloom_filter GRANULARITY 4,
    INDEX idx_topic2 topic_2 TYPE bloom_filter GRANULARITY 4,
    INDEX idx_topic3 topic_3 TYPE bloom_filter GRANULARITY 4,

    PROJECTION chain_address_topic0_projection
    (
        SELECT
            _part_offset
        ORDER BY 
            chain_id,
            address,
            topic_0,
            block_number,
            transaction_index,
            log_index
    ),
    PROJECTION chain_topic0_projection
    (
        SELECT
            _part_offset
        ORDER BY 
            chain_id,
            topic_0,
            block_number,
            transaction_index,
            log_index,
            address
    ),
    PROJECTION address_topic0_state_projection
    (
        SELECT
            chain_id,
            address,
            topic_0,
            count() AS log_count,
            min(block_number) AS min_block_number,
            min(block_timestamp) AS min_block_timestamp,
            max(block_number) AS max_block_number,
            max(block_timestamp) AS max_block_timestamp
        GROUP BY
            chain_id,
            address,
            topic_0
    )
) ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
ORDER BY (chain_id, block_number, transaction_hash, log_index)
PARTITION BY (chain_id, toStartOfQuarter(block_timestamp))
SETTINGS deduplicate_merge_projection_mode = 'rebuild', lightweight_mutation_projection_mode = 'rebuild', allow_part_offset_column_in_projections=1;
