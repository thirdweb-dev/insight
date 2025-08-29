CREATE MATERIALIZED VIEW IF NOT EXISTS insert_token_transfers_mv
TO token_transfers
AS SELECT
    chain_id,
    tt.1 AS token_type,
    tt.2 AS token_address,
    tt.3 AS token_id,
    tt.4 AS from_address,
    tt.5 AS to_address,
    tt.6 AS block_number,
    tt.7 AS block_timestamp,
    tt.8 AS transaction_hash,
    tt.9 AS transaction_index,
    tt.10 AS amount,
    tt.11 AS log_index,
    tt.12 AS batch_index,
    insert_timestamp,
    is_deleted
FROM insert_null_block_data
ARRAY JOIN token_transfers AS tt