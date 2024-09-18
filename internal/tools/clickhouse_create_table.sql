CREATE TABLE indexer_cache (
    `key` String,
    `value` String,
    `expires_at` DateTime
) ENGINE = MergeTree()
ORDER BY `key`
TTL expires_at
SETTINGS index_granularity = 8192;