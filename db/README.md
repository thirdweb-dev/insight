# Insight DB migrations

## Clickhouse

Migrations are managed using [golang-migrate](https://github.com/golang-migrate/migrate) and the migration files are located in the `ch_migrations` directory.

Each storage type (orchestrator, staging and main) has its own migrations.

To add a new migration, run
```
migrate create -ext sql -dir db/ch_migrations/<storage_type> <migration_name>
```

Migrations are run when the indexer is started.
