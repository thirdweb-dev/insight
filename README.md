# Insight

**Insight** is a high-performance, modular blockchain indexer and data API for EVM chains. It fetches, processes, and stores on-chain data—making it easy to query blocks, transactions, logs, token balances, and more via a robust HTTP API.

## 🚀 Getting Started

**Quickstart (Local Development):**

```bash
# 1. Clone the repo
git clone https://github.com/thirdweb-dev/insight.git
cd insight

# 2. Copy example configs and secrets
cp configs/config.example.yml configs/config.yml
cp configs/secrets.example.yml configs/secrets.yml

# 3. (Optional) Start dependencies with Docker Compose
docker-compose up -d clickhouse

# 4. Apply ClickHouse migrations
cat internal/tools/clickhouse/*.sql | docker exec -i <clickhouse-container> clickhouse-client --user admin --password password

# 4b. Apply Postgres migrations (if using Postgres for orchestration and staging)
psql -h localhost -U postgres -d postgres -f internal/tools/postgres/postgres_schema.sql

# 5. Build and run Insight
go build -o main -tags=production
./main orchestrator   # Starts the indexer
./main api           # Starts the API server

# 6. Access the API
# Default: http://localhost:3000
```

---

## 🏗 How It Works

Insight's architecture consists of five main components that work together to continuously index blockchain data:

### 1. **Poller** 
The Poller continuously fetches new blocks from the configured RPC endpoint. It uses multiple worker goroutines to concurrently retrieve block data, handles successful and failed results, and stores the processed block data and any failures in staging storage.

### 2. **Worker** 
The Worker processes batches of block numbers, fetching block data, logs, and traces (if supported) from the configured RPC. It divides the work into chunks, processes them concurrently, and returns the results as a collection of WorkerResult structures containing block data, transactions, logs, and traces for each processed block.

### 3. **Committer** 
The Committer periodically moves data from staging storage to main storage. It ensures blocks are committed sequentially, handling any gaps in the data, and updates various metrics while performing concurrent inserts of blocks, logs, transactions, and traces into the main storage.

### 4. **Failure Recoverer** 
The FailureRecoverer recovers from block processing failures. It periodically checks for failed blocks, attempts to reprocess them using a worker, and either removes successfully processed blocks from the failure list or updates the failure count for blocks that continue to fail.

### 5. **Orchestrator** 
The Orchestrator coordinates and manages the poller, failure recoverer, and committer. It initializes these components based on configuration settings and starts them concurrently, ensuring they run independently while waiting for all of them to complete their tasks.

### Data Flow
1. **Polling**: The Poller continuously checks for new blocks on the blockchain
2. **Processing**: Workers fetch and process block data, transactions, logs, and traces
3. **Staging**: Processed data is stored in staging storage for validation
4. **Commitment**: The Committer moves validated data to main storage
5. **Recovery**: Failed blocks are retried by the Failure Recoverer
6. **API**: The HTTP API serves queries from the main storage

### Work Modes
Insight operates in two distinct work modes that automatically adapt based on how far behind the chain head the indexer is:

**Backfill Mode** (Catching Up):
- Used when the indexer is significantly behind the latest block
- Processes blocks in large batches for maximum throughput
- Optimized for an error-free indexing process over speed
- Automatically switches to live mode when caught up

**Live Mode** (Real-time):
- Used when the indexer is close to the chain head (within ~500 blocks by default)
- Processes blocks as they arrive with minimal latency
- Optimized for real-time data availability
- Switches back to backfill mode if falling behind

The work mode threshold and check interval are configurable via `workMode.liveModeThreshold` and `workMode.checkIntervalMinutes` settings.

This modular architecture allows for adaptation to various EVM chains and use cases, with configurable batch sizes, delays, and processing strategies.

---

## ⚙️ Installation / Setup

### Prerequisites

- **Go** 1.23+
- **ClickHouse** database (Docker Compose included)
- (Optional) **Redis**, **Kafka**, **Prometheus**, **Grafana** for advanced features

### Environment Variables & Secrets

Insight supports configuration via environment variables, which is especially useful for containerized deployments and CI/CD pipelines.

**Environment Variable Naming Convention:**
- Use uppercase letters and underscores
- Nested YAML paths become underscore-separated variables
- Example: `rpc.url` becomes `RPC_URL`
- Example: `storage.main.clickhouse.host` becomes `STORAGE_MAIN_CLICKHOUSE_HOST`

**Common Environment Variables:**

**RPC Configuration:**
```bash
RPC_URL=https://1.rpc.thirdweb.com/your-client-id
RPC_CHAIN_ID=1
RPC_BLOCKS_BLOCKS_PER_REQUEST=500
RPC_BLOCKS_BATCH_DELAY=100
RPC_LOGS_BLOCKS_PER_REQUEST=250
RPC_LOGS_BATCH_DELAY=100
RPC_TRACES_ENABLED=false
```

**Storage Configuration:**
```bash
STORAGE_MAIN_CLICKHOUSE_HOST=localhost
STORAGE_MAIN_CLICKHOUSE_PORT=9000
STORAGE_MAIN_CLICKHOUSE_USERNAME=admin
STORAGE_MAIN_CLICKHOUSE_PASSWORD=your-password
STORAGE_MAIN_CLICKHOUSE_DATABASE=main
STORAGE_MAIN_CLICKHOUSE_DISABLE_TLS=false
```

**API Configuration:**
```bash
API_HOST=0.0.0.0
API_PORT=3000
API_THIRDWEB_CLIENT_ID=your-client-id
API_BASIC_AUTH_USERNAME=admin
API_BASIC_AUTH_PASSWORD=your-api-password
```

**Logging Configuration:**
```bash
LOG_LEVEL=info
LOG_PRETTIFY=false
```

**Poller Configuration:**
```bash
POLLER_ENABLED=true
POLLER_INTERVAL=1000
POLLER_BLOCKS_PER_POLL=500
POLLER_FROM_BLOCK=0
```

**Complete Example:**
```bash
# Set all configuration via environment variables
export RPC_URL="https://1.rpc.thirdweb.com/your-client-id"
export RPC_CHAIN_ID=1
export STORAGE_MAIN_CLICKHOUSE_HOST="your-clickhouse-host"
export STORAGE_MAIN_CLICKHOUSE_PASSWORD="your-password"
export API_BASIC_AUTH_USERNAME="admin"
export API_BASIC_AUTH_PASSWORD="your-api-password"
export LOG_LEVEL="info"

# Run without config files
./main orchestrator
./main api
```

**Secrets Management:**
- For sensitive credentials, you can use environment variables instead of `configs/secrets.yml`
- Environment variables take precedence over config files
- See `configs/secrets.example.yml` for the complete structure

### Docker

- `docker-compose.yml` provides ClickHouse, Redis, Prometheus, and Grafana for local development.
- Exposes:
  - ClickHouse: `localhost:8123` (web UI), `localhost:9440` (native)
  - Prometheus: `localhost:9090`
  - Grafana: `localhost:4000`
  - Redis: `localhost:6379`

### Database Migrations

- SQL migration scripts are in `internal/tools/`.
- Apply them to your ClickHouse instance before running the indexer.

---

## 💡 Usage

### CLI Commands

- **Indexer (Orchestrator):**  
  `./main orchestrator`  
  Starts the block poller, committer, and failure recovery.

- **API Server:**  
  `./main api`  
  Serves the HTTP API at `http://localhost:3000`.

- **Validation & Utilities:**  
  Additional commands: `validate`, `validate-and-fix`, `migrate-valid` (see `cmd/`).

### API Endpoints

All endpoints require HTTP Basic Auth (see `configs/secrets.yml`).

- **Blocks:**  
  `GET /{chainId}/blocks`  
  Query blocks with filters, sorting, pagination, and aggregation.

- **Transactions:**  
  `GET /{chainId}/transactions`  
  `GET /{chainId}/transactions/{to}`  
  `GET /{chainId}/transactions/{to}/{signature}`  
  `GET /{chainId}/wallet-transactions/{wallet_address}`

- **Logs/Events:**  
  `GET /{chainId}/events`  
  `GET /{chainId}/events/{contract}`  
  `GET /{chainId}/events/{contract}/{signature}`

- **Token Balances & Holders:**  
  `GET /{chainId}/balances/{owner}/{type}`  
  `GET /{chainId}/holders/{address}`  
  `GET /{chainId}/tokens/{address}`

- **Token Transfers:**  
  `GET /{chainId}/transfers`

- **Search:**  
  `GET /{chainId}/search/{input}`  
  Search by block number, hash, address, or function signature.

- **Health:**  
  `GET /health`

- **Swagger/OpenAPI:**  
  `GET /swagger/index.html`  
  `GET /openapi.json`

See the [OpenAPI spec](docs/swagger.yaml) for full details.

---

## 🛠 Configuration

Insight supports configuration via multiple methods with the following priority order:

1. **Command-line flags** (highest priority)
2. **Environment variables** 
3. **YAML config files** (`configs/config.yml`)

### Configuration Methods

**1. YAML Config Files (Recommended for Development):**
```yaml
# configs/config.yml
rpc:
  url: https://1.rpc.thirdweb.com/your-thirdweb-client-id
  blocks:
    blocksPerRequest: 1000
    batchDelay: 0
  logs:
    blocksPerRequest: 400
    batchDelay: 100
  traces:
    enabled: true
    blocksPerRequest: 200
    batchDelay: 100

log:
  level: debug
  pretty: true

storage:
  main:
    clickhouse:
      host: localhost
      port: 9440
      database: "default"
      username: admin
      password: password
      disableTLS: true
```

**2. Environment Variables (Recommended for Production):**
```bash
# Set configuration via environment variables
export RPC_URL="https://1.rpc.thirdweb.com/your-client-id"
export RPC_BLOCKS_BLOCKS_PER_REQUEST=1000
export RPC_BLOCKS_BATCH_DELAY=0
export LOG_LEVEL="debug"
export LOG_PRETTIFY=true
export STORAGE_MAIN_CLICKHOUSE_HOST="localhost"
export STORAGE_MAIN_CLICKHOUSE_PASSWORD="your-password"
```

**3. Command-line Flags:**
```bash
# Override specific settings via CLI flags
./main orchestrator --rpc-url="https://1.rpc.thirdweb.com/your-client-id" --log-level=info
./main api --api-host=0.0.0.0 --api-port=8080
```

### Environment Variable Reference

**RPC Configuration:**
| YAML Path | Environment Variable | Description | Default |
|-----------|---------------------|-------------|---------|
| `rpc.url` | `RPC_URL` | RPC endpoint URL | - |
| `rpc.chainId` | `RPC_CHAIN_ID` | Blockchain network ID | 1 |
| `rpc.blocks.blocksPerRequest` | `RPC_BLOCKS_BLOCKS_PER_REQUEST` | Blocks per RPC request | 500 |
| `rpc.blocks.batchDelay` | `RPC_BLOCKS_BATCH_DELAY` | Delay between batches (ms) | 100 |
| `rpc.logs.blocksPerRequest` | `RPC_LOGS_BLOCKS_PER_REQUEST` | Logs per RPC request | 250 |
| `rpc.logs.batchDelay` | `RPC_LOGS_BATCH_DELAY` | Log batch delay (ms) | 100 |
| `rpc.traces.enabled` | `RPC_TRACES_ENABLED` | Enable trace fetching | false |
| `rpc.traces.blocksPerRequest` | `RPC_TRACES_BLOCKS_PER_REQUEST` | Traces per RPC request | 500 |

**Storage Configuration:**
| YAML Path | Environment Variable | Description | Default |
|-----------|---------------------|-------------|---------|
| `storage.main.clickhouse.host` | `STORAGE_MAIN_CLICKHOUSE_HOST` | ClickHouse host | localhost |
| `storage.main.clickhouse.port` | `STORAGE_MAIN_CLICKHOUSE_PORT` | ClickHouse port | 9000 |
| `storage.main.clickhouse.username` | `STORAGE_MAIN_CLICKHOUSE_USERNAME` | Database username | admin |
| `storage.main.clickhouse.password` | `STORAGE_MAIN_CLICKHOUSE_PASSWORD` | Database password | password |
| `storage.main.clickhouse.database` | `STORAGE_MAIN_CLICKHOUSE_DATABASE` | Database name | main |
| `storage.main.clickhouse.disableTLS` | `STORAGE_MAIN_CLICKHOUSE_DISABLE_TLS` | Disable TLS | false |

**API Configuration:**
| YAML Path | Environment Variable | Description | Default |
|-----------|---------------------|-------------|---------|
| `api.host` | `API_HOST` | API server host | localhost |
| `api.port` | `API_PORT` | API server port | 3000 |
| `api.thirdweb.clientId` | `API_THIRDWEB_CLIENT_ID` | ThirdWeb client ID | - |
| `api.basicAuth.username` | `API_BASIC_AUTH_USERNAME` | API username | admin |
| `api.basicAuth.password` | `API_BASIC_AUTH_PASSWORD` | API password | admin |

**Logging Configuration:**
| YAML Path | Environment Variable | Description | Default |
|-----------|---------------------|-------------|---------|
| `log.level` | `LOG_LEVEL` | Log level (debug, info, warn, error) | debug |
| `log.prettify` | `LOG_PRETTIFY` | Pretty print logs | true |

**Poller Configuration:**
| YAML Path | Environment Variable | Description | Default |
|-----------|---------------------|-------------|---------|
| `poller.enabled` | `POLLER_ENABLED` | Enable block polling | true |
| `poller.interval` | `POLLER_INTERVAL` | Polling interval (ms) | 1000 |
| `poller.blocksPerPoll` | `POLLER_BLOCKS_PER_POLL` | Blocks per poll | 500 |
| `poller.fromBlock` | `POLLER_FROM_BLOCK` | Starting block number | 0 |

### Configuration Best Practices

**Development:**
- Use YAML config files for easy configuration management
- Keep sensitive data in `configs/secrets.yml` (gitignored)

**Production:**
- Use environment variables for security and flexibility
- Set sensitive credentials via environment variables
- Use container orchestration secrets management

**Docker/Kubernetes:**
```yaml
# docker-compose.yml example
environment:
  - RPC_URL=https://1.rpc.thirdweb.com/your-client-id
  - STORAGE_MAIN_CLICKHOUSE_HOST=clickhouse
  - STORAGE_MAIN_CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
  - API_BASIC_AUTH_PASSWORD=${API_PASSWORD}
```

- See `configs/config.example.yml` and `configs/secrets.example.yml` for complete configuration options.
- All config options can be overridden by environment variables or CLI flags.

---

## 📁 Project Structure

```
insight/
  api/           # API layer
  cmd/           # CLI commands (orchestrator, api, validation, etc.)
  configs/       # Config and secrets templates
  docs/          # Swagger/OpenAPI docs
  internal/
    common/      # Core blockchain models/utilities
    handlers/    # HTTP API handlers
    log/         # Logging setup
    metrics/     # Prometheus metrics
    middleware/  # API middleware (auth, CORS, logging)
    orchestrator/# Indexer orchestration logic
    publisher/   # Kafka publisher (optional)
    rpc/         # RPC client logic
    storage/     # ClickHouse connectors
    tools/       # SQL migration scripts
    validation/  # Data validation logic
    worker/      # Block processing workers
  test/          # Mocks and test helpers
  main.go        # Entrypoint
  Dockerfile     # Container build
  docker-compose.yml
```

---

## 🤝 Contributing

1. **Fork & clone** the repo.
2. **Install dependencies:**  
   `go mod download`
3. **Set up local ClickHouse:**  
   `docker-compose up -d clickhouse`
4. **Apply migrations** (see above).
5. **Run tests:**  
   `go test ./...`
6. **Open a PR** with your changes!

---

## 🧪 Testing

- All core logic is covered by unit tests (see `test/` and `internal/handlers/*_test.go`).
- Run the full suite:
  ```bash
  go test ./...
  ```

---

## 📚 Documentation

- **API Reference:**  
  - [Swagger UI](http://localhost:3000/swagger/index.html) (when running)
  - [OpenAPI Spec](docs/swagger.yaml)
- **Metrics:**  
  - Prometheus metrics at [http://localhost:2112/metrics](http://localhost:2112/metrics)
  - See `internal/metrics/metrics.go` for all exposed metrics.
- **Architecture & Design:**  
  - See the top of this README and code comments for architectural details.

---

**License:** Apache 2.0
