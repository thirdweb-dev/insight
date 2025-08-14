CREATE TABLE IF NOT EXISTS token_balance
(
  `chain_id` UInt256,
  `token_type` LowCardinality(String),
  `token_address` FixedString(42),
  `owner_address` FixedString(42),
  `token_id` UInt256,

  `balance_state` AggregateFunction(sum, Int256),
  `last_block_number_state` AggregateFunction(max, UInt256),
  `last_block_timestamp_state` AggregateFunction(max, DateTime),

  INDEX idx_last_block_number (finalizeAggregation(last_block_number_state)) TYPE minmax GRANULARITY 1,
  INDEX idx_last_block_timestamp (finalizeAggregation(last_block_timestamp_state)) TYPE minmax GRANULARITY 1,

  PROJECTION owner_balances_projection
  (
    SELECT
      chain_id,
      owner_address,
      token_address,
      token_id,
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
      balance_state,
      last_block_number_state,
      last_block_timestamp_state
    ORDER BY chain_id, token_address, token_id, owner_address
  )
)
ENGINE = AggregatingMergeTree
PARTITION BY chain_id
ORDER BY (chain_id, owner_address, token_address, token_id)
SETTINGS index_granularity = 8192, lightweight_mutation_projection_mode = 'rebuild', deduplicate_merge_projection_mode = 'rebuild';