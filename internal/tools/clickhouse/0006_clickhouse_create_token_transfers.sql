CREATE TABLE IF NOT EXISTS token_transfers
(
    `chain_id` UInt256,
    `token_type` LowCardinality(String),
    `token_address` FixedString(42),
    `token_id` UInt256,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `transaction_hash` FixedString(66),
    `transaction_index` UInt64,
    `amount` UInt256,
    `log_index` UInt64,
    `batch_index` Nullable(UInt16) DEFAULT NULL,

    `insert_timestamp` DateTime DEFAULT now(),
    `is_deleted` UInt8 DEFAULT 0,

    INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_from_address from_address TYPE bloom_filter GRANULARITY 3,
    INDEX idx_to_address to_address TYPE bloom_filter GRANULARITY 3,
    INDEX idx_transaction_hash transaction_hash TYPE bloom_filter GRANULARITY 4,

    PROJECTION from_address_projection (
        SELECT
            *
        ORDER BY
            chain_id,
            from_address,
            block_number,
            transaction_index,
            log_index
    ),
    PROJECTION to_address_projection (
        SELECT
            *
        ORDER BY
            chain_id,
            to_address,
            block_number,
            transaction_index,
            log_index
    ),
    PROJECTION token_id_projection (
        SELECT 
            *
        ORDER BY
            chain_id,
            token_address,
            token_id,
            block_number,
            transaction_index,
            log_index
    ),
    PROJECTION from_address_state_projection (
        SELECT
            chain_id,
            from_address,
            token_address,
            token_type,
            countState() AS transfer_count_state,
            sumState(toInt256(amount)) AS total_amount_state,
            minState(block_number) AS min_block_number_state,
            minState(block_timestamp) AS min_block_timestamp_state,
            maxState(block_number) AS max_block_number_state,
            maxState(block_timestamp) AS max_block_timestamp_state
        GROUP BY
            chain_id,
            from_address,
            token_address,
            token_type
    ),
    PROJECTION to_address_state_projection (
        SELECT
            chain_id,
            to_address,
            token_address,
            token_type,
            countState() AS transfer_count_state,
            sumState(toInt256(amount)) AS total_amount_state,
            minState(block_number) AS min_block_number_state,
            minState(block_timestamp) AS min_block_timestamp_state,
            maxState(block_number) AS max_block_number_state,
            maxState(block_timestamp) AS max_block_timestamp_state
        GROUP BY
            chain_id,
            to_address,
            token_address,
            token_type
    ),
    PROJECTION token_state_projection (
        SELECT
            chain_id,
            token_address,
            token_id,
            token_type,
            countState() AS transfer_count_state,
            sumState(toInt256(amount)) AS total_volume_state,
            minState(block_number) AS min_block_number_state,
            minState(block_timestamp) AS min_block_timestamp_state,
            maxState(block_number) AS max_block_number_state,
            maxState(block_timestamp) AS max_block_timestamp_state
        GROUP BY
            chain_id,
            token_address,
            token_id,
            token_type
    )
)
ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
PARTITION BY (chain_id, toStartOfQuarter(block_timestamp))
ORDER BY (chain_id, token_address, block_number, transaction_index, log_index)
SETTINGS index_granularity = 8192, lightweight_mutation_projection_mode = 'rebuild', deduplicate_merge_projection_mode = 'rebuild';