-- ERC20
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_erc20_mv
TO token_balances
AS
-- FROM side (outgoing, negative delta)
SELECT
  chain_id,
  token_type,
  token_address,
  from_address AS owner_address,
  token_id,
  -toInt256(amount) AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'from' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc20'
UNION ALL
-- TO side (incoming, positive delta)
SELECT
  chain_id,
  token_type,
  token_address,
  to_address AS owner_address,
  token_id,
  toInt256(amount) AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'to' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc20';

-- ERC721
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_erc721_mv
TO token_balances
AS
SELECT
  chain_id,
  token_type,
  token_address,
  from_address AS owner_address,
  token_id,
  -1 AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'from' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc721'
UNION ALL
SELECT
  chain_id,
  token_type,
  token_address,
  to_address AS owner_address,
  token_id,
  1 AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'to' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc721';

-- ERC1155
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_erc1155_mv
TO token_balances
AS
SELECT
  chain_id,
  token_type,
  token_address,
  from_address AS owner_address,
  token_id,
  -toInt256(amount) AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'from' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc1155'
UNION ALL
SELECT
  chain_id,
  token_type,
  token_address,
  to_address AS owner_address,
  token_id,
  toInt256(amount) AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'to' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc1155';

-- ERC6909
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_erc6909_mv
TO token_balances
AS
SELECT
  chain_id,
  token_type,
  token_address,
  from_address AS owner_address,
  token_id,
  -toInt256(amount) AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'from' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc6909'
UNION ALL
SELECT
  chain_id,
  token_type,
  token_address,
  to_address AS owner_address,
  token_id,
  toInt256(amount) AS balance_delta,
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  'to' AS direction,
  insert_timestamp,
  is_deleted
FROM token_transfers 
WHERE token_type = 'erc6909';