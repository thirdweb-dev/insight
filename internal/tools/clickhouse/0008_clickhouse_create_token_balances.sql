CREATE TABLE IF NOT EXISTS token_balances
(
  `chain_id` UInt256,
  `token_type` LowCardinality(String),
  `token_address` FixedString(42),
  `owner_address` FixedString(42),
  `token_id` UInt256,
  
  -- Normalized delta: positive for incoming, negative for outgoing
  `balance_delta` Int256,
  
  -- Transaction details for ordering and deduplication
  `block_number` UInt256,
  `block_timestamp` DateTime,
  `transaction_hash` FixedString(66),
  `transaction_index` UInt64,
  `log_index` UInt64,
  `direction` Enum8('from' = 1, 'to' = 2),  -- To make each transfer create 2 unique rows
  
  `insert_timestamp` DateTime DEFAULT now(),
  `is_deleted` Int8 DEFAULT 0,

  INDEX idx_block_timestamp block_timestamp TYPE minmax GRANULARITY 1,
  INDEX idx_token_address token_address TYPE bloom_filter GRANULARITY 3,
  INDEX idx_owner_address owner_address TYPE bloom_filter GRANULARITY 3,

  -- Projection for efficient balance queries by owner
  PROJECTION owner_balances_projection
  (
    SELECT
      chain_id,
      owner_address,
      token_address,
      token_id,
      sumState(balance_delta * if(is_deleted = 0, 1, -1)) AS balance_state
      minState(block_number) AS min_block_number_state,
      minState(block_timestamp) AS min_block_timestamp_state,
      maxState(block_number) AS max_block_number_state,
      maxState(block_timestamp) AS max_block_timestamp_state
    GROUP BY chain_id, owner_address, token_address, token_id
    ORDER BY chain_id, owner_address, token_address, token_id
  ),
  
  -- Projection for efficient balance queries by token
  PROJECTION token_balances_projection
  (
    SELECT
      chain_id,
      token_address,
      token_id,
      owner_address,
      sumState(balance_delta * if(is_deleted = 0, 1, -1)) AS balance_state
      minState(block_number) AS min_block_number_state,
      minState(block_timestamp) AS min_block_timestamp_state,
      maxState(block_number) AS max_block_number_state,
      maxState(block_timestamp) AS max_block_timestamp_state
    GROUP BY chain_id, token_address, token_id, owner_address
    ORDER BY chain_id, token_address, token_id, owner_address
  )
)
ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)
PARTITION BY chain_id
ORDER BY (chain_id, owner_address, token_address, token_id, block_number, transaction_index, log_index, direction)
SETTINGS index_granularity = 8192, lightweight_mutation_projection_mode = 'rebuild', deduplicate_merge_projection_mode = 'rebuild';