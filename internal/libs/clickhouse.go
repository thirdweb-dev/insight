package libs

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"

	"github.com/rs/zerolog/log"

	"github.com/ClickHouse/clickhouse-go/v2"
	config "github.com/thirdweb-dev/indexer/configs"
)

// only use this for backfill or getting old data.
var ClickhouseConnV1 clickhouse.Conn

// use this for new current states and query
var ClickhouseConnV2 clickhouse.Conn

func InitOldClickHouseV1() {
	ClickhouseConnV1 = initClickhouse(
		config.Cfg.OldClickhouseHostV1,
		config.Cfg.OldClickhousePortV1,
		config.Cfg.OldClickhouseUsernameV1,
		config.Cfg.OldClickhousePasswordV1,
		config.Cfg.OldClickhouseDatabaseV1,
		config.Cfg.OldClickhouseEnableTLSV1,
	)
}

// This is a new clickhouse where data will be inserted into.
// All user queries will be done against this clickhouse.
func InitNewClickHouseV2() {
	ClickhouseConnV2 = initClickhouse(
		config.Cfg.CommitterClickhouseHost,
		config.Cfg.CommitterClickhousePort,
		config.Cfg.CommitterClickhouseUsername,
		config.Cfg.CommitterClickhousePassword,
		config.Cfg.CommitterClickhouseDatabase,
		config.Cfg.CommitterClickhouseEnableTLS,
	)
}

func initClickhouse(host string, port int, username string, password string, database string, enableTLS bool) clickhouse.Conn {
	clickhouseConn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", host, port)},
		Protocol: clickhouse.Native,
		TLS: func() *tls.Config {
			if enableTLS {
				return &tls.Config{}
			}
			return nil
		}(),
		Auth: clickhouse.Auth{
			Username: username,
			Password: password,
			Database: database,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
	}

	return clickhouseConn
}

func GetMaxBlockNumberFromClickHouseV2(chainId *big.Int) (*big.Int, error) {
	// Use toString() to force ClickHouse to return a string instead of UInt256
	query := fmt.Sprintf("SELECT toString(max(block_number)) FROM blocks WHERE chain_id = %d", chainId.Uint64())
	rows, err := ClickhouseConnV2.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return big.NewInt(0), nil
	}

	var maxBlockNumberStr string
	if err := rows.Scan(&maxBlockNumberStr); err != nil {
		return nil, err
	}

	// Convert string to big.Int to handle UInt256 values
	maxBlockNumber, ok := new(big.Int).SetString(maxBlockNumberStr, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", maxBlockNumberStr)
	}

	return maxBlockNumber, nil
}
