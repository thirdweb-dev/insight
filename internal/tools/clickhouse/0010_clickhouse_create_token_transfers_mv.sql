CREATE TABLE IF NOT EXISTS token_transfers
(
    `token_type` LowCardinality(String),
    `chain_id` UInt256,
    `token_address` FixedString(42),
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `transaction_hash` FixedString(66),
    `token_id` UInt256,
    `amount` UInt256,
    `log_index` UInt64,
    `sign` Int8 DEFAULT 1,
    `insert_timestamp` DateTime DEFAULT now(),

    INDEX minmax_block_number block_number TYPE minmax GRANULARITY 16,
    INDEX minmax_block_timestamp block_timestamp TYPE minmax GRANULARITY 16,

    PROJECTION from_address_projection
    (
        SELECT *
        ORDER BY 
            chain_id,
            token_type,
            from_address,
            block_number,
            log_index
    ),
    PROJECTION to_address_projection
    (
        SELECT *
        ORDER BY 
            chain_id,
            token_type,
            to_address,
            block_number,
            log_index
    ),
    PROJECTION transaction_hash_projection
    (
        SELECT *
        ORDER BY 
            chain_id,
            token_type,
            transaction_hash,
            block_number,
            log_index
    ),
    PROJECTION token_aggregation_projection
    (
        SELECT 
            chain_id,
            token_type,
            max(block_number) AS max_block_number,
            count() AS total_count
        GROUP BY 
            chain_id,
            token_type
    )
)
ENGINE = VersionedCollapsingMergeTree(sign, insert_timestamp)
PARTITION BY chain_id
ORDER BY (chain_id, token_type, token_address, block_number, log_index)
SETTINGS index_granularity = 8192, lightweight_mutation_projection_mode = 'rebuild', deduplicate_merge_projection_mode = 'rebuild';

CREATE MATERIALIZED VIEW IF NOT EXISTS logs_to_token_transfers TO token_transfers
(
    `chain_id` UInt256,
    `token_address` FixedString(42),
    `from_address` String,
    `to_address` String,
    `token_type` String,
    `block_number` UInt256,
    `block_timestamp` DateTime,
    `transaction_hash` FixedString(66),
    `log_index` UInt64,
    `sign` Int8,
    `insert_timestamp` DateTime,
    `token_id` UInt256,
    `amount` UInt256
)
AS WITH
    transfer_logs AS
    (
        SELECT
            chain_id,
            address AS token_address,
            topic_0,
            topic_1,
            topic_2,
            topic_3,
            (topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') AND (topic_3 = '') AS is_erc20,
            (topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') AND (topic_3 != '') AS is_erc721,
            topic_0 IN ('0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62', '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb') AS is_erc1155,
            multiIf(is_erc20, 'erc20', is_erc721, 'erc721', 'erc1155') AS token_type,
            if(is_erc1155, concat('0x', substring(topic_2, 27, 40)), concat('0x', substring(topic_1, 27, 40))) AS from_address,
            if(is_erc1155, concat('0x', substring(topic_3, 27, 40)), concat('0x', substring(topic_2, 27, 40))) AS to_address,
            data,
            block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            sign,
            insert_timestamp
        FROM logs
        WHERE topic_0 IN ('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62', '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb')
    ),
    batch_transfer_metadata AS
    (
        SELECT
            *,
            3 + (2 * 64) AS ids_length_idx,
            ids_length_idx + 64 AS ids_values_idx,
            reinterpretAsUInt64(reverse(unhex(substring(data, ids_length_idx, 64)))) AS ids_length,
            (ids_length_idx + 64) + (ids_length * 64) AS amounts_length_idx,
            reinterpretAsUInt64(reverse(unhex(substring(data, amounts_length_idx, 64)))) AS amounts_length,
            amounts_length_idx + 64 AS amounts_values_idx
        FROM transfer_logs
        WHERE (topic_0 = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb') AND (length(topic_1) = 66) AND (length(topic_2) = 66) AND (length(topic_3) = 66) AND (length(data) != (258 + ((ids_length + amounts_length) * 64))) AND (ids_length = amounts_length)
    ),
    batch_transfer_logs AS
    (
        SELECT
            *,
            arrayMap(x -> substring(data, ids_values_idx + ((x - 1) * 64), 64), range(1, toInt32(ids_length) + 1)) AS ids_hex,
            arrayMap(x -> substring(data, amounts_values_idx + ((x - 1) * 64), 64), range(1, toInt32(amounts_length) + 1)) AS amounts_hex
        FROM batch_transfer_metadata
    )
SELECT
    chain_id,
    token_address,
    from_address,
    to_address,
    token_type,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    sign,
    insert_timestamp,
    multiIf(is_erc1155, reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))), is_erc721, reinterpretAsUInt256(reverse(unhex(substring(topic_3, 3, 64)))), toUInt256(0)) AS token_id,
    multiIf(is_erc20 AND (length(data) = 66), reinterpretAsUInt256(reverse(unhex(substring(data, 3)))), is_erc721, toUInt256(1), is_erc1155, if(length(data) = 130, reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64)))), toUInt256(1)), toUInt256(0)) AS amount
FROM transfer_logs
WHERE topic_0 IN ('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62')
UNION ALL
WITH
    transfer_logs AS
    (
        SELECT
            chain_id,
            address AS token_address,
            topic_0,
            topic_1,
            topic_2,
            topic_3,
            (topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') AND (topic_3 = '') AS is_erc20,
            (topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') AND (topic_3 != '') AS is_erc721,
            topic_0 IN ('0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62', '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb') AS is_erc1155,
            multiIf(is_erc20, 'erc20', is_erc721, 'erc721', 'erc1155') AS token_type,
            if(is_erc1155, concat('0x', substring(topic_2, 27, 40)), concat('0x', substring(topic_1, 27, 40))) AS from_address,
            if(is_erc1155, concat('0x', substring(topic_3, 27, 40)), concat('0x', substring(topic_2, 27, 40))) AS to_address,
            data,
            block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            sign,
            insert_timestamp
        FROM logs
        WHERE topic_0 IN ('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62', '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb')
    ),
    batch_transfer_metadata AS
    (
        SELECT
            *,
            3 + (2 * 64) AS ids_length_idx,
            ids_length_idx + 64 AS ids_values_idx,
            reinterpretAsUInt64(reverse(unhex(substring(data, ids_length_idx, 64)))) AS ids_length,
            (ids_length_idx + 64) + (ids_length * 64) AS amounts_length_idx,
            reinterpretAsUInt64(reverse(unhex(substring(data, amounts_length_idx, 64)))) AS amounts_length,
            amounts_length_idx + 64 AS amounts_values_idx
        FROM transfer_logs
        WHERE (topic_0 = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb') AND (length(topic_1) = 66) AND (length(topic_2) = 66) AND (length(topic_3) = 66) AND (length(data) != (258 + ((ids_length + amounts_length) * 64))) AND (ids_length = amounts_length)
    ),
    batch_transfer_logs AS
    (
        SELECT
            *,
            arrayMap(x -> substring(data, ids_values_idx + ((x - 1) * 64), 64), range(1, toInt32(ids_length) + 1)) AS ids_hex,
            arrayMap(x -> substring(data, amounts_values_idx + ((x - 1) * 64), 64), range(1, toInt32(amounts_length) + 1)) AS amounts_hex
        FROM batch_transfer_metadata
    )
SELECT
    chain_id,
    token_address,
    from_address,
    to_address,
    token_type,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    sign,
    insert_timestamp,
    reinterpretAsUInt256(reverse(unhex(substring(hex_id, 1, 64)))) AS token_id,
    reinterpretAsUInt256(reverse(unhex(substring(hex_amount, 1, 64)))) AS amount
FROM batch_transfer_logs
ARRAY JOIN
    ids_hex AS hex_id,
    amounts_hex AS hex_amount