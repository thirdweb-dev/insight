-- ERC20
CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_erc20_mv
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
  insert_timestamp,
  is_deleted
FROM logs
WHERE topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- Transfer
  AND length(topic_1) = 66 AND startsWith(topic_1, '0x')
  AND length(topic_2) = 66 AND startsWith(topic_2, '0x')
  AND topic_3 = ''
  AND length(data) = 66;

-- ERC721
CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_erc721_mv
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
  insert_timestamp,
  is_deleted
FROM logs
WHERE topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  AND length(topic_1) = 66 AND startsWith(topic_1, '0x')
  AND length(topic_2) = 66 AND startsWith(topic_2, '0x')
  AND length(topic_3) = 66 AND startsWith(topic_3, '0x')
  AND length(data) = 2;

-- ERC1155 (single)
CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_erc1155_single_mv
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
    insert_timestamp,
    is_deleted
FROM logs
WHERE topic_0 = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'   -- TransferSingle
  AND length(topic_2) = 66 AND length(topic_3) = 66
  AND length(data) = (2 + 2*64);

-- ERC1155 (batch)
CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_erc1155_batch_mv
TO token_transfers
AS 
SELECT 
  chain_id, 
  address AS token_address, 
  'erc1155' AS token_type, 
  reinterpretAsUInt256(reverse(substring(bin, (ids_base + ((i - 1) * 32)) + 1, 32))) AS token_id, 
  concat('0x', substring(topic_2, 27, 40)) AS from_address, 
  concat('0x', substring(topic_3, 27, 40)) AS to_address, 
  block_number, 
  block_timestamp, 
  transaction_hash, 
  transaction_index, 
  reinterpretAsUInt256(reverse(substring(bin, (am_base + ((i - 1) * 32)) + 1, 32))) AS amount,
  log_index,
  toNullable(toUInt16(i - 1)) AS batch_index,
  insert_timestamp,
  is_deleted 
FROM (
  SELECT 
    chain_id, 
    address, 
    topic_2,
    topic_3,
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    is_deleted,
    insert_timestamp, 
    unhex(substring(data, 3)) AS bin,
    length(unhex(substring(data, 3))) AS bin_len, 
    toUInt32(reinterpretAsUInt256(reverse(substring(unhex(substring(data, 3)), 1, 32)))) AS ids_off,
    toUInt32(reinterpretAsUInt256(reverse(substring(unhex(substring(data, 3)), 33, 32)))) AS am_off,
    toUInt32(reinterpretAsUInt256(reverse(substring(unhex(substring(data, 3)), ids_off + 1, 32)))) AS ids_len,
    toUInt32(reinterpretAsUInt256(reverse(substring(unhex(substring(data, 3)), am_off + 1, 32)))) AS am_len,
    ids_off + 32 AS ids_base,
    am_off + 32 AS am_base
FROM default.logs
WHERE (topic_0 = '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb')
  AND (length(topic_2) = 66)
  AND (length(topic_3) = 66)
  AND (ids_len = am_len)
  AND (ids_len > 0)
  AND ((ids_off + 32) <= bin_len)
  AND ((am_off + 32) <= bin_len)
  AND ((ids_base + (ids_len * 32)) <= bin_len)
  AND ((am_base + (am_len * 32)) <= bin_len)
) ARRAY JOIN range(1, ids_len + 1) AS i;


-- ERC6909
CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_erc6909_mv
TO token_transfers
AS
SELECT
  chain_id,
  address AS token_address,
  'erc6909' AS token_type,
  reinterpretAsUInt256(reverse(unhex(substring(topic_3, 3, 64)))) AS token_id,
  concat('0x', substring(topic_1, 27, 40)) AS from_address,
  concat('0x', substring(topic_2, 27, 40)) AS to_address,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64)))) AS amount,
  log_index,
  CAST(NULL AS Nullable(UInt16)) AS batch_index,
  insert_timestamp,
  is_deleted
FROM logs
WHERE topic_0 = '0x1b3d7edb2e9c0b0e7c525b20aaaef0f5940d2ed71663c7d39266ecafac728859'
  AND length(topic_1) = 66
  AND length(topic_2) = 66
  AND length(data) == 2 + 128;