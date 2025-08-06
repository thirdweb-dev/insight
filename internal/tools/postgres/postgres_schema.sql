CREATE TABLE IF NOT EXISTS block_failures (
    chain_id NUMERIC NOT NULL,
    block_number NUMERIC NOT NULL,
    last_error_timestamp BIGINT NOT NULL,
    failure_count INTEGER DEFAULT 1,
    reason TEXT,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain_id, block_number)
) WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_block_failures_block_number ON block_failures USING BRIN (block_number) WITH (pages_per_range = 64) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_block_failures_chain_active ON block_failures(chain_id, block_number) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_block_failures_deleted_at ON block_failures(deleted_at) WHERE deleted_at IS NOT NULL;

-- Cursors table for tracking various processing positions
CREATE TABLE IF NOT EXISTS cursors (
    chain_id NUMERIC NOT NULL,
    cursor_type VARCHAR(50) NOT NULL,   -- Limited length for cursor types
    cursor_value TEXT NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain_id, cursor_type)
) WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_cursors_type ON cursors(cursor_type);
CREATE INDEX IF NOT EXISTS idx_cursors_reorg_updated ON cursors(chain_id, cursor_type, updated_at DESC) WHERE is_deleted = FALSE AND cursor_type = 'reorg';


CREATE TABLE IF NOT EXISTS block_data (
    chain_id NUMERIC NOT NULL,
    block_number NUMERIC NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain_id, block_number)
) WITH (fillfactor = 85, autovacuum_vacuum_scale_factor = 0.1);

CREATE INDEX IF NOT EXISTS idx_block_data_block_number ON block_data USING BRIN (block_number) WITH (pages_per_range = 64);
CREATE INDEX IF NOT EXISTS idx_block_data_json ON block_data USING GIN (data);

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
