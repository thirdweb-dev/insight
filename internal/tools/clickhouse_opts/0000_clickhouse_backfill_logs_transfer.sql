CREATE TABLE IF NOT EXISTS backfill_logs
(
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

    `sign` Int8 DEFAULT 1,
    `insert_timestamp` DateTime DEFAULT now(),
) ENGINE = Null;


--- Materialize view running to the correct tables
-- ERC20
CREATE MATERIALIZED VIEW IF NOT EXISTS bf__token_transfers_erc20_mv
TO token_transfers
AS
SELECT
  chain_id,
  address AS token_address,
  'erc20' AS token_type,
  0 AS token_id,
  concat('0x', substring(topic_1, 27, 40)) AS from_address,
  concat('0x', substring(topic_2, 27, 40)) AS to_address,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS amount,
  log_index,
  CAST(NULL AS Nullable(UInt16)) AS batch_index,
  sign,
  insert_timestamp
FROM backfill_logs
WHERE topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND length(topic_1) = 66 AND startsWith(topic_1, '0x')
  AND length(topic_2) = 66 AND startsWith(topic_2, '0x')
  AND topic_3 = ''
  AND length(data) = 66;

-- ERC721
CREATE MATERIALIZED VIEW IF NOT EXISTS bf__token_transfers_erc721_mv
TO token_transfers
AS
SELECT
  chain_id,
  address AS token_address,
  'erc721' AS token_type,
  reinterpretAsUInt256(reverse(unhex(substring(topic_3, 3, 64)))) AS token_id,
  concat('0x', substring(topic_1, 27, 40)) AS from_address,
  concat('0x', substring(topic_2, 27, 40)) AS to_address,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  toUInt8(1) AS amount,
  log_index,
  CAST(NULL AS Nullable(UInt16)) AS batch_index,
  sign,
  insert_timestamp
FROM backfill_logs
WHERE topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND length(topic_1) = 66 AND startsWith(topic_1, '0x')
  AND length(topic_2) = 66 AND startsWith(topic_2, '0x')
  AND length(topic_3) = 66 AND startsWith(topic_3, '0x')
  AND length(data) = 2;

-- ERC1155 (single)
CREATE MATERIALIZED VIEW IF NOT EXISTS bf__token_transfers_erc1155_single_mv
TO token_transfers
AS
SELECT
    chain_id,
    address AS token_address,
    'erc1155' AS token_type,
    reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS token_id,
    concat('0x', substring(topic_2, 27, 40)) AS from_address,
    concat('0x', substring(topic_3, 27, 40)) AS to_address,
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64)))) AS amount,
    log_index,
    toNullable(toUInt16(0)) AS batch_index,
    sign,
    insert_timestamp
FROM backfill_logs
WHERE topic_0 = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'   -- TransferSingle
  AND length(topic_2) = 66 AND length(topic_3) = 66
  AND length(data) = (2 + 2*64);

-- ERC1155 (batch)
CREATE MATERIALIZED VIEW IF NOT EXISTS bf__token_transfers_erc1155_batch_mv
TO token_transfers
AS
SELECT
    chain_id,
    address AS token_address,
    'erc1155' AS token_type,
    reinterpretAsUInt256(reverse(unhex(id_hex))) AS token_id,
    concat('0x', substring(topic_2, 27, 40)) AS from_address,
    concat('0x', substring(topic_3, 27, 40)) AS to_address,
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    reinterpretAsUInt256(reverse(unhex(amount_hex))) AS amount,
    log_index,
    toNullable(toUInt16(array_index - 1)) AS batch_index,
    sign,
    insert_timestamp
FROM (
    SELECT 
        chain_id, address, topic_2, topic_3,
        block_number, block_timestamp, transaction_hash, transaction_index, log_index, sign, insert_timestamp,
        toUInt32(reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64))))) AS ids_offset,
        toUInt32(reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64))))) AS amounts_offset,
        toUInt32(reinterpretAsUInt256(reverse(unhex(substring(data, 3 + ids_offset * 2, 64))))) AS ids_length,
        toUInt32(reinterpretAsUInt256(reverse(unhex(substring(data, 3 + amounts_offset * 2, 64))))) AS amounts_length,
        arrayMap(i -> substring(data, 3 + ids_offset * 2 + 64 + (i-1)*64, 64), range(1, least(ids_length, 10000) + 1)) AS ids_array,
        arrayMap(i -> substring(data, 3 + amounts_offset * 2 + 64 + (i-1)*64, 64), range(1, least(amounts_length, 10000) + 1)) AS amounts_array
    FROM backfill_logs
    WHERE topic_0 = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb'
      AND length(topic_2) = 66 
      AND length(topic_3) = 66
      AND ids_length = amounts_length
)
ARRAY JOIN 
    ids_array AS id_hex,
    amounts_array AS amount_hex,
    arrayEnumerate(ids_array) AS array_index;


-- ERC6909
CREATE MATERIALIZED VIEW IF NOT EXISTS bf__token_transfers_erc6909_mv
TO token_transfers
AS
SELECT
  chain_id,
  lower(address) AS token_address,
  'erc6909' AS token_type,
  reinterpretAsUInt256(reverse(unhex(substring(topic_3, 3, 64)))) AS token_id,
  lower(concat('0x', substring(topic_1, 27, 40))) AS from_address,
  lower(concat('0x', substring(topic_2, 27, 40))) AS to_address,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64)))) AS amount,
  log_index,
  CAST(NULL AS Nullable(UInt16)) AS batch_index,
  sign,
  insert_timestamp
FROM backfill_logs
WHERE topic_0 = '0x1b3d7edb2e9c0b0e7c525b20aaaef0f5940d2ed71663c7d39266ecafac728859'
  AND length(topic_1) = 66
  AND length(topic_2) = 66
  AND length(data) == 2 + 128;

--- INITIATE BACKFILL BY RUNNING:
-- INSERT INTO backfill_logs
-- SELECT
-- chain_id,
-- block_number,
-- block_hash,
-- block_timestamp,
-- transaction_hash ,
-- transaction_index,
-- log_index,
-- address,
-- data,
-- topic_0,
-- topic_1,
-- topic_2,
-- topic_3,
-- sign,
-- insert_timestamp,
-- FROM logs
-- WHERE 1=1
--   AND chain_id = 1
--   AND block_number >= 0 AND block_number < 10000000
--   AND topic_0 IN (
--     '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', -- 20/721
--     '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62', -- 1155 single
--     '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb', -- 1155 batch
--     '0x1b3d7edb2e9c0b0e7c525b20aaaef0f5940d2ed71663c7d39266ecafac728859' -- 6909
--   );

-- DROP TABLE token_transfers, token_balance;
-- DROP TABLE bf__token_transfers_erc20_mv, bf__token_transfers_erc721_mv, bf__token_transfers_erc1155_mv, bf__token_transfers_erc6909_mv;
-- DROP TABLE token_transfers_erc20_mv, token_transfers_erc721_mv, token_transfers_erc1155_mv, token_transfers_erc6909_mv;
-- DROP TABLE token_balance_erc20_mv, token_balance_erc721_mv, token_balance_erc1155_mv, token_balance_erc6909_mv;