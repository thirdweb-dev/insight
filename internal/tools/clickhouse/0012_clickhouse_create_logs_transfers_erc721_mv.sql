CREATE TABLE IF NOT EXISTS logs_transfers_erc721
(
    `chain_id` UInt256,
    `token_address` FixedString(42),
    `token_id` UInt256,
    `from_address` FixedString(42),
    `to_address` FixedString(42),
    `block_number` UInt256,
    `block_timestamp` DateTime CODEC(Delta(4), ZSTD(1)),
    `transaction_hash` FixedString(66),
    `amount` UInt8 DEFAULT 1,
    `log_index` UInt64,
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
ORDER BY (chain_id, token_address, block_number, transaction_hash, log_index)
SETTINGS index_granularity = 8192, lightweight_mutation_projection_mode = 'rebuild', deduplicate_merge_projection_mode = 'rebuild';


CREATE MATERIALIZED VIEW IF NOT EXISTS mv_logs_to_erc721
TO logs_transfers_erc721
AS
SELECT
  chain_id,
  address AS token_address,
  reinterpretAsUInt256(reverse(unhex(substring(topic_3, 3, 64)))) AS token_id,
  concat('0x', substring(topic_1, 27, 40)) AS from_address,
  concat('0x', substring(topic_2, 27, 40)) AS to_address,
  block_number,
  block_timestamp,
  transaction_hash,
  toUInt8(1) AS amount,
  log_index,
  sign,
  insert_timestamp
FROM logs
WHERE topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND topic_3 != ''
  AND length(topic_3) = 66
  AND length(data) = 2;