package tools

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
)

func Connect() (clickhouse.Conn, error) {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		return nil, fmt.Errorf("error loading .env file: %w", err)
	}

	// Parse CLICKHOUSE_PORT as int
	port, err := strconv.Atoi(os.Getenv("CLICKHOUSE_PORT"))
	if err != nil {
		return nil, fmt.Errorf("invalid CLICKHOUSE_PORT: %w", err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", os.Getenv("CLICKHOUSE_HOST"), port)},
		Auth: clickhouse.Auth{
			Database: os.Getenv("CLICKHOUSE_DATABASE"),
			Username: os.Getenv("CLICKHOUSE_USERNAME"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func BatchInsert(conn clickhouse.Conn, query string, data [][]interface{}) error {
	batch, err := conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	for _, row := range data {
		if err := batch.Append(row...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func InsertRow(conn clickhouse.Conn, query string, data []interface{}) error {
	return BatchInsert(conn, query, [][]interface{}{data})
}
