# indexer

## Configuration

You can configure the application in 3 ways.
The order of priority is
1. Command line arguments
2. Environment variables
3. Configuration files

### Configuration using command line arguments
You can configure the application using command line arguments.
For example to configure the `rpc.url` configuration, you can use the `--rpc-url` command line argument.
Only select arguments are implemented. More on this below.

### Configuration using environment variables
You can also configure the application using environment variables. You can configure any configuration in the `config.yml` file using environment variables by replacing the `.` in the configuration with `_` and making the variable name uppercase.  
For example to configure the `rpc.url` configuration to `https://my-rpc.com`, you can set the `RPC_URL` environment variable to `https://my-rpc.com`.  

### Configuration using configuration files
The default configuration should live in `configs/config.yml`. Copy `configs/config.example.yml` to get started.  
Or you can use the `--config` flag to specify a different configuration file.  
If you want to add secrets to the configuration file, you can copy `configs/secrets.example.yml` to `configs/secrets.yml` and add the secrets. They won't be commited to the repository or the built image.

### Supported configurations:

#### RPC URL
URL to use as the RPC client.

cmd: `--rpc-url`
env: `RPC_URL`
yaml:
```yaml
rpc:
  url: https://rpc.com
```

#### RPC Blocks Per Request
How many blocks at a time to fetch from the RPC. Default is 1000.

cmd: `--rpc-blocks-blocksPerRequest`
env: `RPC_BLOCKS_BLOCKSPERREQUEST`
yaml:
```yaml
rpc:
  blocks:
    blocksPerRequest: 1000
```

#### RPC Blocks Batch Delay
Milliseconds to wait between batches of blocks when fetching from the RPC. Default is 0.

cmd: `--rpc-blocks-batchDelay`
env: `RPC_BLOCKS_BATCHDELAY`
yaml:
```yaml
rpc:
  blocks:
    batchDelay: 100
```

#### RPC Logs Blocks Per Request
How many blocks at a time to query logs for from the RPC. Default is 100.
Has no effect if it's larger than RPC blocks per request.

cmd: `--rpc-logs-blocksPerRequest`
env: `RPC_LOGS_BLOCKSPERREQUEST`
yaml:
```yaml
rpc:
  logs:
    blocksPerRequest: 100
```

#### RPC Logs Batch Delay
Milliseconds to wait between batches of logs when fetching from the RPC. Default is 0.

cmd: `--rpc-logs-batchDelay`
env: `RPC_LOGS_BATCHDELAY`
yaml:
```yaml
rpc:
  logs:
    batchDelay: 100
```

#### RPC Traces Enabled
Whether to enable fetching traces from the RPC. Default is `true`, but it will try to detect if the RPC supports traces automatically.

cmd: `--rpc-traces-enabled`
env: `RPC_TRACES_ENABLED`
yaml:
```yaml
rpc:
  traces:
    enabled: true
```

#### RPC Traces Blocks Per Request
How many blocks at a time to fetch traces for from the RPC. Default is 100.
Has no effect if it's larger than RPC blocks per request.

cmd: `--rpc-traces-blocksPerRequest`
env: `RPC_TRACES_BLOCKSPERREQUEST`
yaml:
```yaml
rpc:
  traces:
    blocksPerRequest: 100
```

#### RPC Traces Batch Delay
Milliseconds to wait between batches of traces when fetching from the RPC. Default is 0.

cmd: `--rpc-traces-batchDelay`
env: `RPC_TRACES_BATCHDELAY`
yaml:
```yaml
rpc:
  traces:
    batchDelay: 100
```

#### Log Level
Log level for the logger. Default is `warn`.

cmd: `--log-level`
env: `LOG_LEVEL`
yaml:
```yaml
log:
  level: debug
```

#### Prettify logs
Whether to print logs in a prettified format. Affects performance. Default is `false`.

cmd: `--log-prettify`
env: `LOG_PRETTIFY`
yaml:
```yaml
log:
  prettify: true
```

#### Poller
Whether to enable the poller. Default is `true`.

cmd: `--poller-enabled`
env: `POLLER_ENABLED`
yaml:
```yaml
poller:
  enabled: true
```

#### Poller Interval
Poller trigger interval in milliseconds. Default is `1000`.

cmd: `--poller-interval`
env: `POLLER_INTERVAL`
yaml:
```yaml
poller:
  interval: 3000
```

#### Poller Blocks Per Poll
How many blocks to poll each interval. Default is `10`.

cmd: `--poller-blocks-per-poll`
env: `POLLER_BLOCKSPERPOLL`
yaml:
```yaml
poller:
  blocksPerPoll: 3
```

#### Poller From Block
From which block to start polling. Default is `0`.

cmd: `--poller-from-block`
env: `POLLER_FROMBLOCK`
yaml:
```yaml
poller:
  fromBlock: 20000000
```

#### Poller Until Block
Until which block to poll. If not set, it will poll until the latest block.

cmd: `--poller-until-block`
env: `POLLER_UNTILBLOCK`
yaml:
```yaml
poller:
  untilBlock: 20000010
```

#### Committer
Whether to enable the committer. Default is `true`.

cmd: `--committer-enabled`
env: `COMMITTER_ENABLED`
yaml:
```yaml
committer:
  enabled: true
```

#### Committer Interval
Committer trigger interval in milliseconds. Default is `250`.

cmd: `--committer-interval`
env: `COMMITTER_INTERVAL`
yaml:
```yaml
committer:
  interval: 3000
```

#### Committer Blocks Per Commit
How many blocks to commit each interval. Default is `10`.

cmd: `--committer-blocks-per-commit`
env: `COMMITTER_BLOCKSPERCOMMIT`
yaml:
```yaml
committer:
  blocksPerCommit: 1000
```

#### Failure Recoverer
Whether to enable the failure recoverer. Default is `true`.

cmd: `--failure-recoverer-enabled`
env: `FAILURERECOVERER_ENABLED`
yaml:
```yaml
failureRecoverer:
  enabled: true
```

#### Failure Recoverer Interval
Failure recoverer trigger interval in milliseconds. Default is `1000`.

cmd: `--failure-recoverer-interval`
env: `FAILURERECOVERER_INTERVAL`
yaml:
```yaml
failureRecoverer:
  interval: 3000
```

#### Failure Recoverer Blocks Per Run
How many blocks to recover each interval. Default is `10`.

cmd: `--failure-recoverer-blocks-per-run`
env: `FAILURERECOVERER_BLOCKSPERRUN`
yaml:
```yaml
failureRecoverer:
  blocksPerRun: 100
```

#### Storage
This application has 3 kinds of strorage. Main, Staging and Orchestrator.
Each of them takes similar configuration depending on the driver you want to use.

There are no defaults, so this needs to be configured.

```yaml
storage:
  main:
    driver: "clickhouse"
    clickhouse:
      host: "localhost"
      port: 3000
      user: "admin"
      password: "admin"
      database: "base"
  staging:
    driver: "memory"
    memory:
      maxItems: 1000000
  orchestrator:
    driver: "memory"
```