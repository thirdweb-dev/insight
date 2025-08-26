CREATE MATERIALIZED VIEW address_transfers_mv
TO address_transfers
AS
SELECT
    chain_id,
    token_type,
    token_address,
    token_id,
    address_tuple.1 AS address,
    address_tuple.2 AS address_type,
    from_address,
    to_address,
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    amount,
    log_index,
    batch_index,
    insert_timestamp,
    is_deleted
FROM token_transfers
ARRAY JOIN 
    arrayZip([from_address, to_address], ['from', 'to']) AS address_tuple;