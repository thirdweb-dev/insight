# Readonly ClickHouse for API Endpoints

This document explains how to configure and use a readonly ClickHouse instance for user-facing API endpoints while keeping the main orchestration flow unchanged.

## Overview

The readonly ClickHouse feature allows you to:
- Use a separate, read-only ClickHouse instance for API queries
- Keep the main ClickHouse instance for orchestration (indexing, committing, etc.)
- Scale read and write operations independently
- Improve API performance by using dedicated read replicas

## Configuration

### Environment Variables

Set the following environment variables to enable readonly ClickHouse:

```bash
export CLICKHOUSE_HOST_READONLY="readonly-clickhouse.example.com"
export CLICKHOUSE_PORT_READONLY=9000
export CLICKHOUSE_USER_READONLY="readonly_user"
export CLICKHOUSE_PASSWORD_READONLY="readonly_password"
export CLICKHOUSE_DATABASE_READONLY="insight_readonly"
```

### Configuration File

You can also set these values in your configuration file:

```yaml
storage:
  main:
    clickhouse:
      # Main ClickHouse configuration (for orchestration)
      host: "localhost"
      port: 9000
      username: "default"
      password: "password"
      database: "insight"
      
      # Readonly ClickHouse configuration (for API endpoints)
      readonlyHost: "readonly-clickhouse.example.com"
      readonlyPort: 9000
      readonlyUsername: "readonly_user"
      readonlyPassword: "readonly_password"
      readonlyDatabase: "insight_readonly"
```

## How It Works

### Automatic Detection

The system automatically detects if readonly configuration is available:

1. **With readonly config**: API endpoints use the readonly ClickHouse instance
2. **Without readonly config**: API endpoints fall back to the main ClickHouse instance

### Affected Endpoints

The following API endpoints will use the readonly ClickHouse instance when configured:

- `GET /{chainId}/blocks` - Block queries
- `GET /{chainId}/transactions` - Transaction queries
- `GET /{chainId}/events` - Event/log queries
- `GET /{chainId}/transfers` - Token transfer queries
- `GET /{chainId}/balances/{owner}` - Token balance queries
- `GET /{chainId}/holders/{address}` - Token holder queries
- `GET /{chainId}/search/{input}` - Search queries

### Unaffected Operations

The following operations continue to use the main ClickHouse instance:

- Block indexing and polling
- Transaction processing
- Event/log processing
- Staging data operations
- Orchestration flow (committer, failure recovery, etc.)

## Implementation Details

### Readonly Connector

The `ClickHouseReadonlyConnector` implements the same interface as the main connector but:

- Only allows read operations
- Panics on write operations (ensuring readonly behavior)
- Uses readonly connection parameters
- Falls back to main config if readonly config is incomplete

### Storage Factory

The `NewReadonlyConnector` function creates readonly connectors:

```go
// For API endpoints (readonly if configured)
storage.NewReadonlyConnector[storage.IMainStorage](&config.Cfg.Storage.Main)

// For orchestration (always main connector)
storage.NewConnector[storage.IMainStorage](&config.Cfg.Storage.Main)
```

## Setup Instructions

### 1. Create Readonly ClickHouse Instance

Set up a ClickHouse replica or read-only instance:

```sql
-- On your readonly ClickHouse instance
CREATE DATABASE insight_readonly;
-- Grant readonly permissions to readonly_user
GRANT SELECT ON insight_readonly.* TO readonly_user;
```

### 2. Set Environment Variables

```bash
export CLICKHOUSE_HOST_READONLY="your-readonly-host"
export CLICKHOUSE_PORT_READONLY=9000
export CLICKHOUSE_USER_READONLY="readonly_user"
export CLICKHOUSE_PASSWORD_READONLY="readonly_password"
export CLICKHOUSE_DATABASE_READONLY="insight_readonly"
```

### 3. Restart API Server

Restart your API server to pick up the new configuration:

```bash
./insight api
```

### 4. Verify Configuration

Check the logs to confirm which connector is being used:

```
INFO Using readonly ClickHouse connector for API endpoints
```

## Monitoring and Troubleshooting

### Log Messages

- **"Using readonly ClickHouse connector for API endpoints"** - Readonly mode active
- **"Using regular ClickHouse connector for API endpoints"** - Fallback to main connector

### Common Issues

1. **Connection refused**: Check readonly ClickHouse host/port
2. **Authentication failed**: Verify readonly username/password
3. **Database not found**: Ensure readonly database exists
4. **Permission denied**: Grant SELECT permissions to readonly user

### Testing

Test the readonly connection:

```bash
# Test readonly connection
clickhouse-client --host=readonly-host --port=9000 --user=readonly_user --password=readonly_password --database=insight_readonly -q "SELECT 1"
```

## Performance Considerations

### Read Replicas

- Use ClickHouse read replicas for better performance
- Consider geographic distribution for global users
- Monitor replica lag to ensure data consistency

### Connection Pooling

The readonly connector uses the same connection pool settings as the main connector:

```yaml
maxOpenConns: 100
maxIdleConns: 10
```

### Query Optimization

- Readonly instances can be optimized for query performance
- Consider different ClickHouse settings for readonly vs. write instances
- Use materialized views on readonly instances for complex queries

## Security

### Network Security

- Restrict readonly ClickHouse to internal networks
- Use VPN or private subnets for readonly access
- Consider ClickHouse's built-in network security features

### User Permissions

- Create dedicated readonly user with minimal permissions
- Grant only SELECT permissions on required tables
- Regularly rotate readonly user passwords

### Data Access

- Readonly users cannot modify data
- No risk of accidental data corruption
- Audit logs show readonly access patterns

## Migration

### From Single Instance

1. Set up readonly ClickHouse instance
2. Configure environment variables
3. Restart API server
4. Monitor performance and errors
5. Gradually migrate more endpoints if needed

### Rollback

To rollback to main ClickHouse:

1. Remove readonly environment variables
2. Restart API server
3. API endpoints will automatically use main connector

## Support

For issues or questions about the readonly ClickHouse feature:

1. Check the logs for error messages
2. Verify ClickHouse connectivity
3. Review configuration parameters
4. Check ClickHouse server logs
5. Contact the development team
