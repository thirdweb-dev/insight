CREATE MATERIALIZED VIEW address_transactions_mv
TO address_transactions
AS
SELECT 
    chain_id,
    hash,
    nonce,
    block_hash,
    block_number,
    block_timestamp,
    transaction_index,
    address_tuple.1 AS address,
    address_tuple.2 AS address_type,
    value,
    gas,
    gas_price,
    data,
    function_selector,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    max_fee_per_blob_gas,
    blob_versioned_hashes,
    transaction_type,
    r,
    s,
    v,
    access_list,
    authorization_list,
    contract_address,
    gas_used,
    cumulative_gas_used,
    effective_gas_price,
    blob_gas_used,
    blob_gas_price,
    logs_bloom,
    status,
    
    insert_timestamp,
    is_deleted
FROM transactions
ARRAY JOIN 
    arrayZip([from_address, to_address], ['from', 'to']) AS address_tuple;