package handlers

import (
	"sync"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// package-level variables for shared storage
var (
	mainStorage storage.IMainStorage
	storageOnce sync.Once
	storageErr  error
)

// getMainStorage returns a storage connector, using readonly if configured
// This function is shared across all handlers to ensure consistent storage access
func getMainStorage() (storage.IMainStorage, error) {
	storageOnce.Do(func() {
		var err error
		// Use readonly connector for API endpoints if readonly configuration is available
		if config.Cfg.Storage.Main.Clickhouse != nil &&
			(config.Cfg.Storage.Main.Clickhouse.ReadonlyHost != "" ||
				config.Cfg.Storage.Main.Clickhouse.ReadonlyPort != 0 ||
				config.Cfg.Storage.Main.Clickhouse.ReadonlyUsername != "" ||
				config.Cfg.Storage.Main.Clickhouse.ReadonlyPassword != "" ||
				config.Cfg.Storage.Main.Clickhouse.ReadonlyDatabase != "") {
			// Use readonly connector for API endpoints
			log.Info().Msg("Using readonly ClickHouse connector for API endpoints")
			mainStorage, err = storage.NewReadonlyConnector[storage.IMainStorage](&config.Cfg.Storage.Main)
		} else {
			// Use regular connector for orchestration flow
			log.Info().Msg("Using regular ClickHouse connector for API endpoints")
			mainStorage, err = storage.NewConnector[storage.IMainStorage](&config.Cfg.Storage.Main)
		}
		if err != nil {
			storageErr = err
			log.Error().Err(err).Msg("Error creating storage connector")
		}
	})
	return mainStorage, storageErr
}
