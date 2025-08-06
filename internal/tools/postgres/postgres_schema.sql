CREATE TABLE IF NOT EXISTS block_failures (
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    last_error_timestamp BIGINT NOT NULL,
    failure_count INTEGER DEFAULT 1,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain_id, block_number)
) WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.1, autovacuum_analyze_scale_factor = 0.05);

CREATE INDEX IF NOT EXISTS idx_block_failures_block_number_ordered ON block_failures(chain_id, block_number DESC);

-- Cursors table for tracking various processing positions
CREATE TABLE IF NOT EXISTS cursors (
    chain_id BIGINT NOT NULL,
    cursor_type VARCHAR(30) NOT NULL,
    cursor_value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain_id, cursor_type)
) WITH (fillfactor = 80);


CREATE TABLE IF NOT EXISTS block_data (
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain_id, block_number)
) WITH (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.1, autovacuum_analyze_scale_factor = 0.05);


-- Function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at
CREATE TRIGGER update_block_failures_updated_at BEFORE UPDATE ON block_failures 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_cursors_updated_at BEFORE UPDATE ON cursors 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_block_data_updated_at BEFORE UPDATE ON block_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
