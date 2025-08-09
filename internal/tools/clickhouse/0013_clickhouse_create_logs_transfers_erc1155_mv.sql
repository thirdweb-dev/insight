CREATE TABLE IF NOT EXISTS logs_transfers_erc1155
(
    `chain_id` UInt256,
    `token_address` FixedString(42),
    `token_id` UInt256,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `transaction_hash` FixedString(66),
    `amount` UInt256,
    `log_index` UInt64,
    `batch_index` UInt16 DEFAULT 0,
    `sign` Int8 DEFAULT 1,
    `insert_timestamp` DateTime DEFAULT now(),

    INDEX minmax_block_number block_number TYPE minmax GRANULARITY 4,
    INDEX minmax_block_timestamp block_timestamp TYPE minmax GRANULARITY 4,
    INDEX bloomfilter_token_address token_address TYPE bloom_filter GRANULARITY 4,
    INDEX bloomfilter_token_id token_id TYPE bloom_filter GRANULARITY 4,
    INDEX bloomfilter_from_address from_address TYPE bloom_filter GRANULARITY 4,
    INDEX bloomfilter_to_address to_address TYPE bloom_filter GRANULARITY 4,
    INDEX bloomfilter_transaction_hash transaction_hash TYPE bloom_filter GRANULARITY 4,
)
ENGINE = VersionedCollapsingMergeTree(sign, insert_timestamp)
PARTITION BY (chain_id, toStartOfYear(block_timestamp))
ORDER BY (chain_id, token_address, block_number, transaction_hash, log_index, batch_index)
SETTINGS index_granularity = 8192, lightweight_mutation_projection_mode = 'rebuild', deduplicate_merge_projection_mode = 'rebuild';


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_logs_to_erc1155_all
TO logs_transfers_erc1155
AS

SELECT
    chain_id,
    address AS token_address,
    reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS token_id,
    concat('0x', substring(topic_2, 27, 40)) AS from_address,
    concat('0x', substring(topic_3, 27, 40)) AS to_address,
    block_number,
    block_timestamp,
    transaction_hash AS transaction_hash,
    reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64)))) AS amount,
    log_index,
    toUInt16(0) AS batch_index,
    sign,
    insert_timestamp
FROM logs
WHERE topic_0 = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'   -- TransferSingle
  AND length(topic_2) = 66 AND length(topic_3) = 66
  AND length(data) = (2 + 2*64)    -- 0x + 2 words = 130

UNION ALL

WITH meta AS
(
    SELECT
        chain_id, address, topic_2, topic_3, data,
        block_number, block_timestamp, transaction_hash, log_index, sign, insert_timestamp,
        toUInt32(3 + 2*64)  AS ids_len_idx,     -- "0x" + 2*32B heads
        reinterpretAsUInt64(reverse(unhex(substring(data, ids_len_idx, 64)))) AS ids_len,
        (ids_len_idx + 64) AS ids_vals_idx,
        (ids_len_idx + 64) + ids_len * 64 AS amts_len_idx,
        reinterpretAsUInt64(reverse(unhex(substring(data, amts_len_idx, 64)))) AS amts_len,
        (amts_len_idx + 64) AS amts_vals_idx,
        (2 + (4 + ids_len + amts_len) * 64) AS expected_len
    FROM logs
    WHERE topic_0 = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb' -- TransferBatch
      AND length(topic_2) = 66 AND length(topic_3) = 66
),
expanded AS
(
    SELECT
        *,
        arrayMap(i -> substring(data, ids_vals_idx  + (i-1)*64, 64), range(1, toInt32(ids_len)  + 1)) AS ids_hex,
        arrayMap(i -> substring(data, amts_vals_idx + (i-1)*64, 64), range(1, toInt32(amts_len) + 1)) AS amts_hex
    FROM meta
    WHERE amts_len = ids_len
      AND length(data) = expected_len
)
SELECT
    chain_id,
    address AS token_address,
    reinterpretAsUInt256(reverse(unhex(id_hex))) AS token_id,
    concat('0x', substring(topic_2, 27, 40)) AS from_address,
    concat('0x', substring(topic_3, 27, 40)) AS to_address,
    block_number,
    block_timestamp,
    transaction_hash AS transaction_hash,
    reinterpretAsUInt256(reverse(unhex(amt_hex))) AS amount,
    log_index,
    toUInt16(idx - 1) AS batch_index, -- make it 0-based
    sign,
    insert_timestamp
FROM expanded
ARRAY JOIN
    ids_hex AS id_hex,
    amts_hex AS amt_hex,
    arrayEnumerate(ids_hex) AS idx;