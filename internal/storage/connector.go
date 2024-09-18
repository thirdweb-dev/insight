package storage

import (
	"fmt"
)

type ConnectorConfig struct {
	Driver string
	Memory *MemoryConnectorConfig
	Clickhouse *ClickhouseConnectorConfig
}

type IStorage struct {
	IStorageBase
	Cache 	IStorageCache
	DB 		IStorageDB
}

type IStorageBase interface {
	connect() error
	close() error
}

type IStorageCache interface {
	queryCache(index, partitionKey, rangeKey string) (string, error)
	setCache(partitionKey, rangeKey, value string) error
	purgeCache(index, partitionKey, rangeKey string) error
}

type IStorageDB interface {
	query(query string) (string, error)
	insert(query string) error
	delete(query string) error
}


func NewStorageConnector(
	cfg *ConnectorConfig,
) (IStorage, error) {
	switch cfg.Driver {
	case "memory":
		connector, err := NewMemoryConnector(cfg.Memory)
		return IStorage{Cache: connector}, err
	case "clickhouse":
		connector, err := NewClickHouseConnector(cfg.Clickhouse)
		return IStorage{DB: connector, Cache: connector}, err
	}

	return IStorage{}, fmt.Errorf("invalid connector driver: %s", cfg.Driver)
}
