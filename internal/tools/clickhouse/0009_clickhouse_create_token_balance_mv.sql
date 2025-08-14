-- ERC20
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balance_erc20_mv
TO token_balance
AS
SELECT
  chain_id,
  token_type,
  token_address,
  owner_address,
  token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  -- FROM side (negative)
  SELECT
    chain_id,
    token_type,
    token_address,
    token_id,
    from_address AS owner_address,
    toInt256(amount) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc20'
  UNION ALL
  -- TO side (positive)
  SELECT
    chain_id,
    token_type,
    token_address,
    token_id,
    to_address AS owner_address,
    toInt256(amount) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc20'
)
GROUP BY chain_id, token_type, token_address, owner_address, token_id;

-- ERC721
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balance_erc721_mv
TO token_balance
AS
SELECT
  chain_id,
  token_type,
  token_address,
  owner_address,
  token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  SELECT
    chain_id,
    token_type,
    token_address,
    from_address AS owner_address,
    token_id,
    toInt256(1) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc721'
  UNION ALL
  SELECT
    chain_id,
    token_type,
    token_address,
    to_address AS owner_address,
    token_id,
    toInt256(1) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc721'
)
GROUP BY chain_id, token_type, token_address, owner_address, token_id;

-- ERC1155
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balance_erc1155_mv
TO token_balance
AS
SELECT
  chain_id,
  token_type,
  token_address,
  owner_address,
  token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  SELECT
    chain_id,
    token_type,
    token_address,
    from_address AS owner_address,
    token_id,
    toInt256(amount) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc1155'
  UNION ALL
  SELECT
    chain_id,
    token_type,
    token_address,
    to_address AS owner_address,
    token_id,
    toInt256(amount) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc1155'
)
GROUP BY chain_id, token_type, token_address, owner_address, token_id;

-- ERC6909
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balance_erc6909_mv
TO token_balance
AS
SELECT
  chain_id, 
  token_type,
  token_address,
  owner_address,
  token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  SELECT 
    chain_id, 
    token_type,
    token_address, 
    from_address AS owner_address,
    token_id,
    toInt256(amount) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc6909'
  UNION ALL
  SELECT
    chain_id,
    token_type,
    token_address,
    to_address AS owner_address,
    token_id,
    toInt256(amount) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfer WHERE token_type = 'erc6909'
)
GROUP BY chain_id, token_type, token_address, owner_address, token_id;