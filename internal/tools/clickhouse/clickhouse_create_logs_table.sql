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
    `sign` Int8 DEFAULT 1,
    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 3,
    INDEX idx_block_hash block_hash TYPE bloom_filter GRANULARITY 3,
    INDEX idx_transaction_hash transaction_hash TYPE bloom_filter GRANULARITY 3,
    INDEX idx_address address TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic0 topic_0 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic1 topic_1 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic2 topic_2 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_topic3 topic_3 TYPE bloom_filter GRANULARITY 1,
    PROJECTION logs_chainid_topic0_address
    (
        SELECT *
        ORDER BY 
            chain_id,
            topic_0,
            address,
            block_number,
            transaction_index,
            log_index
    )
) ENGINE = VersionedCollapsingMergeTree(sign, insert_timestamp)
ORDER BY (chain_id, block_number, transaction_hash, log_index)
PARTITION BY chain_id
SETTINGS deduplicate_merge_projection_mode = 'drop', lightweight_mutation_projection_mode = 'rebuild';
