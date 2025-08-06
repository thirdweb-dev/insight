CREATE TABLE IF NOT EXISTS cursors (
    `chain_id` UInt256,
    `cursor_type` String,
    `cursor_value` String,
    `insert_timestamp` DateTime DEFAULT now(),
) ENGINE = ReplacingMergeTree(insert_timestamp)
ORDER BY (chain_id, cursor_type);
