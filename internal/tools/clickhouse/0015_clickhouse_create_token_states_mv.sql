CREATE TABLE IF NOT EXISTS token_states
(
  `chain_id` UInt256,
  `token_address` FixedString(42),
  `owner_address` FixedString(42),

  `token_type` LowCardinality(String),
  `token_id` Nullable(UInt256), -- Nullable for fungible tokens

  `balance_state` AggregateFunction(sum, Int256),
  `last_block_number_state` AggregateFunction(max, UInt256),
  `last_block_timestamp_state` AggregateFunction(max, DateTime),

  INDEX bf_owner owner_address TYPE bloom_filter GRANULARITY 4,
  INDEX bf_token token_address TYPE bloom_filter GRANULARITY 4,

  PROJECTION owner_balances_projection
  (
    SELECT
      chain_id,
      owner_address,
      token_address,
      token_id,
      any(token_type) AS token_type,
      sumMerge(balance_state) AS balance,
      maxMerge(last_block_number_state) AS last_block_number,
      maxMerge(last_block_timestamp_state) AS last_block_timestamp
    GROUP BY chain_id, owner_address, token_address, token_id
  ),
  
  PROJECTION token_projection
  (
    SELECT
      chain_id,
      token_address,
      token_id,
      owner_address,
      token_type,
      balance_state,
      last_block_number_state,
      last_block_timestamp_state
    ORDER BY chain_id, token_address, token_id, owner_address
  )
)
ENGINE = AggregatingMergeTree
PARTITION BY chain_id
ORDER BY (chain_id, owner_address, token_address, ifNull(token_id, 0))
SETTINGS index_granularity = 8192;

------

-- ERC20
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_erc20_to_balances
TO token_states
AS
SELECT
  chain_id,
  token_address,
  owner_address,
  'erc20' AS token_type,
  CAST(NULL AS Nullable(UInt256)) AS token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  -- FROM side (negative)
  SELECT
    chain_id,
    token_address,
    from_address AS owner_address,
    toInt256(amount) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc20
  UNION ALL
  -- TO side (positive)
  SELECT
    chain_id,
    token_address,
    to_address AS owner_address,
    toInt256(amount) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc20
)
GROUP BY chain_id, token_address, owner_address, token_type, token_id;

-- ERC721
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_erc721_to_balances
TO token_states
AS
SELECT
  chain_id,
  token_address,
  owner_address,
  'erc721' AS token_type,
  token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  SELECT
    chain_id,
    token_address,
    from_address AS owner_address,
    token_id,
    toInt256(1) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc721
  UNION ALL
  SELECT
    chain_id,
    token_address,
    to_address AS owner_address,
    token_id,
    toInt256(1) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc721
)
GROUP BY chain_id, token_address, owner_address, token_type, token_id;

-- ERC1155
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_erc1155_to_balances
TO token_states
AS
SELECT
  chain_id,
  token_address,
  owner_address,
  'erc1155' AS token_type,
  token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  SELECT
    chain_id,
    token_address,
    from_address AS owner_address,
    token_id,
    toInt256(amount) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc1155
  UNION ALL
  SELECT
    chain_id,
    token_address,
    to_address AS owner_address,
    token_id,
    toInt256(amount) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc1155
)
GROUP BY chain_id, token_address, owner_address, token_type, token_id;

-- ERC6909
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_erc6909_to_balances
TO token_states
AS
SELECT
  chain_id, 
  token_address,
  owner_address,
  'erc6909' AS token_type,
  token_id,
  sumState(delta) AS balance_state,
  maxState(block_number) AS last_block_number_state,
  maxState(block_timestamp) AS last_block_timestamp_state
FROM
(
  SELECT 
    chain_id, 
    token_address, 
    from_address AS owner_address,
    token_id,
    toInt256(amount) * (-1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc6909
  UNION ALL
  SELECT
    chain_id,
    token_address,
    to_address AS owner_address,
    token_id,
    toInt256(amount) * (+1) * sign AS delta,
    block_number,
    block_timestamp
  FROM logs_transfers_erc6909
)
GROUP BY chain_id, token_address, owner_address, token_type, token_id;