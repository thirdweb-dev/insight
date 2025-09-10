# Committer Package

This package implements a committer that processes block data from S3 parquet files and publishes them to Kafka. It follows the requirements specified in the original comments.

## Features

- **ClickHouse Integration**: Gets the maximum block number from ClickHouse for the chain
- **S3 File Discovery**: Lists parquet files from S3 with chain-specific prefixes
- **Block Range Parsing**: Extracts start and end block numbers from S3 filenames
- **File Filtering**: Skips files where end block is less than max block number from ClickHouse
- **Sequential Processing**: Processes files in ascending order by start block number
- **Memory-Efficient Streaming**: Streams parquet files row-by-row to minimize memory usage
- **Kafka Publishing**: Publishes processed block data to Kafka
- **Error Handling**: Comprehensive error handling with detailed logging

## Usage

### Basic Usage

```go
package main

import (
    "context"
    "math/big"
    "log"
    
    "github.com/thirdweb-dev/indexer/internal/committer"
    "github.com/thirdweb-dev/indexer/configs"
)

func main() {
    // Load configuration
    if err := configs.LoadConfig("config.yml"); err != nil {
        log.Fatal("Failed to load config:", err)
    }
    
    // Create committer for chain ID 1 (Ethereum mainnet)
    chainId := big.NewInt(1)
    committer, err := committer.NewCommitterFromConfig(chainId)
    if err != nil {
        log.Fatal("Failed to create committer:", err)
    }
    defer committer.Close()
    
    // Process blocks
    ctx := context.Background()
    if err := committer.ProcessBlocks(ctx); err != nil {
        log.Fatal("Failed to process blocks:", err)
    }
}
```

### Advanced Usage with Custom Configuration

```go
package main

import (
    "context"
    "math/big"
    "log"
    
    "github.com/thirdweb-dev/indexer/internal/committer"
    "github.com/thirdweb-dev/indexer/configs"
)

func main() {
    // Custom configuration
    chainId := big.NewInt(137) // Polygon
    
    clickhouseConfig := &configs.ClickhouseConfig{
        Host:     "localhost",
        Port:     9000,
        Username: "default",
        Password: "",
        Database: "insight",
    }
    
    s3Config := &configs.S3Config{
        Bucket:          "thirdweb-insight-production",
        Region:          "us-east-1",
        AccessKeyID:     "your-access-key",
        SecretAccessKey: "your-secret-key",
    }
    
    kafkaConfig := &configs.KafkaConfig{
        Brokers: "localhost:9092",
    }
    
    // Create committer
    committer, err := committer.NewCommitter(chainId, clickhouseConfig, s3Config, kafkaConfig)
    if err != nil {
        log.Fatal("Failed to create committer:", err)
    }
    defer committer.Close()
    
    // Process blocks
    ctx := context.Background()
    if err := committer.ProcessBlocks(ctx); err != nil {
        log.Fatal("Failed to process blocks:", err)
    }
}
```

## Configuration Requirements

The committer requires the following configuration:

### ClickHouse Configuration
- Host, Port, Username, Password, Database
- Used to query the maximum block number for the chain

### S3 Configuration
- Bucket name (e.g., "thirdweb-insight-production")
- Region, Access Key ID, Secret Access Key
- Used to list and download parquet files

### Kafka Configuration
- Brokers list
- Used to publish processed block data

## S3 File Structure

The committer expects S3 files to follow this naming pattern:
```
chain_${chainId}/year=2024/blocks_1000_2000.parquet
```

Where:
- `chain_${chainId}` is the prefix for the chain
- `year=2024` is the partitioning by year
- `blocks_1000_2000.parquet` contains blocks from 1000 to 2000

## Parquet File Structure

The parquet files should contain the following columns:
- `chain_id` (uint64): Chain identifier
- `block_number` (uint64): Block number
- `block_hash` (string): Block hash
- `block_timestamp` (int64): Block timestamp
- `block_json` (bytes): Serialized block data
- `transactions_json` (bytes): Serialized transactions data
- `logs_json` (bytes): Serialized logs data
- `traces_json` (bytes): Serialized traces data

## Processing Flow

1. **Query ClickHouse**: Get the maximum block number for the chain
2. **List S3 Files**: Find all parquet files with the chain prefix
3. **Filter Files**: Skip files where end block ≤ max block number
4. **Sort Files**: Order by start block number (ascending)
5. **Process Sequentially**: For each file:
   - Download from S3 to local storage
   - Stream parquet file row-by-row
   - Skip blocks < next commit block number
   - Error if block > next commit block number (missing data)
   - Publish found blocks to Kafka
   - Increment commit block number
   - Clean up local file

## Error Handling

The committer includes comprehensive error handling:
- Missing configuration validation
- S3 connection and download errors
- Parquet file parsing errors
- Kafka publishing errors
- Block sequence validation errors

All errors are logged with detailed context for debugging.

## Memory Management

The committer is designed to be memory-efficient:
- Downloads files directly to disk (no in-memory buffering)
- Streams parquet files row-by-row
- Processes one file at a time
- Cleans up local files after processing
- Uses semaphores to limit concurrent operations
