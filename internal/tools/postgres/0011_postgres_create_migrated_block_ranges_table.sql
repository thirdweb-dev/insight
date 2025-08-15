CREATE TABLE IF NOT EXISTS migrated_block_ranges (
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
) WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.1, autovacuum_analyze_scale_factor = 0.05);

-- Create index for efficient querying by chain_id and block ranges
CREATE INDEX IF NOT EXISTS idx_migrated_block_ranges_chain_block ON migrated_block_ranges(chain_id, block_number DESC);

-- Create trigger to automatically update the updated_at timestamp
CREATE TRIGGER update_migrated_block_ranges_updated_at BEFORE UPDATE ON migrated_block_ranges 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
