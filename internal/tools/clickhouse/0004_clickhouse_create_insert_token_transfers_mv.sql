CREATE MATERIALIZED VIEW default.insert_token_transfers_mv TO default.token_transfers
(
    chain_id UInt256,
    token_type LowCardinality(String),
    token_address FixedString(42),
    token_id UInt256,
    from_address FixedString(42),
    to_address FixedString(42),
    block_number UInt256,
    block_timestamp DateTime,
    transaction_hash FixedString(66),
    transaction_index UInt64,
    amount UInt256,
    log_index UInt64,
    batch_index Nullable(UInt16),
    insert_timestamp DateTime,
    is_deleted UInt8
)
AS SELECT
    chain_id,
    tt.2 AS token_type,
    tt.3 AS token_address,
    tt.4 AS token_id,
    tt.5 AS from_address,
    tt.6 AS to_address,
    tt.7 AS block_number,
    tt.8 AS block_timestamp,
    tt.9 AS transaction_hash,
    tt.10 AS transaction_index,
    tt.11 AS amount,
    tt.12 AS log_index,
    tt.13 AS batch_index,
    insert_timestamp,
    is_deleted
FROM default.insert_null_block_data
ARRAY JOIN token_transfers AS tt