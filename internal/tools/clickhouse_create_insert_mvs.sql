CREATE MATERIALIZED VIEW IF NOT EXISTS mv_blocks_inserts
TO blocks
AS
SELECT
    chain_id,
    block.1 AS block_number,
    block.2 AS block_timestamp,
    block.3 AS hash,
    block.4 AS parent_hash,
    block.5 AS sha3_uncles,
    block.6 AS nonce,
    block.7 AS mix_hash,
    block.8 AS miner,
    block.9 AS state_root,
    block.10 AS transactions_root,
    block.11 AS receipts_root,
    block.12 AS logs_bloom,
    block.13 AS size,
    block.14 AS extra_data,
    block.15 AS difficulty,
    block.16 AS total_difficulty,
    block.17 AS transaction_count,
    block.18 AS gas_limit,
    block.19 AS gas_used,
    block.20 AS withdrawals_root,
    block.21 AS base_fee_per_gas,
    insert_timestamp,
    sign
FROM inserts_null_table;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_transactions_inserts
TO transactions
AS
SELECT
    chain_id,
    t.1 AS hash,
    t.2 AS nonce,
    t.3 AS block_hash,
    t.4 AS block_number,
    t.5 AS block_timestamp,
    t.6 AS transaction_index,
    t.7 AS from_address,
    t.8 AS to_address,
    t.9 AS value,
    t.10 AS gas,
    t.11 AS gas_price,
    t.12 AS data,
    t.13 AS function_selector,
    t.14 AS max_fee_per_gas,
    t.15 AS max_priority_fee_per_gas,
    t.16 AS max_fee_per_blob_gas,
    t.17 AS blob_versioned_hashes,
    t.18 AS transaction_type,
    t.19 AS r,
    t.20 AS s,
    t.21 AS v,
    t.22 AS access_list,
    t.23 AS authorization_list,
    t.24 AS contract_address,
    t.25 AS gas_used,
    t.26 AS cumulative_gas_used,
    t.27 AS effective_gas_price,
    t.28 AS blob_gas_used,
    t.29 AS blob_gas_price,
    t.30 AS logs_bloom,
    t.31 AS status,
    insert_timestamp,
    sign
FROM inserts_null_table
ARRAY JOIN transactions AS t;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_logs_inserts
TO logs
AS
SELECT
    chain_id,
    l.1 AS block_number,
    l.2 AS block_hash,
    l.3 AS block_timestamp,
    l.4 AS transaction_hash,
    l.5 AS transaction_index,
    l.6 AS log_index,
    l.7 AS address,
    l.8 AS data,
    l.9 AS topic_0,
    l.10 AS topic_1,
    l.11 AS topic_2,
    l.12 AS topic_3,
    insert_timestamp,
    sign
FROM inserts_null_table
ARRAY JOIN logs AS l;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_traces_inserts
TO traces
AS
SELECT
    chain_id,
    tr.1 AS block_number,
    tr.2 AS block_hash,
    tr.3 AS block_timestamp,
    tr.4 AS transaction_hash,
    tr.5 AS transaction_index,
    tr.6 AS subtraces,
    tr.7 AS trace_address,
    tr.8 AS type,
    tr.9 AS call_type,
    tr.10 AS error,
    tr.11 AS from_address,
    tr.12 AS to_address,
    tr.13 AS gas,
    tr.14 AS gas_used,
    tr.15 AS input,
    tr.16 AS output,
    tr.17 AS value,
    tr.18 AS author,
    tr.19 AS reward_type,
    tr.20 AS refund_address,
    insert_timestamp,
    sign
FROM inserts_null_table
ARRAY JOIN traces AS tr;
