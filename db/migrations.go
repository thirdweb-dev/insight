package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/clickhouse"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
)

func RunMigrations() error {
	storageConfigs := []struct {
		Type   string
		Config *config.ClickhouseConfig
	}{
		{Type: "orchestrator", Config: config.Cfg.Storage.Orchestrator.Clickhouse},
		{Type: "staging", Config: config.Cfg.Storage.Staging.Clickhouse},
		{Type: "main", Config: config.Cfg.Storage.Main.Clickhouse},
	}

	groupedConfigs := make(map[string][]struct {
		Type   string
		Config *config.ClickhouseConfig
	})
	for _, cfg := range storageConfigs {
		key := fmt.Sprintf("%s:%d:%s", cfg.Config.Host, cfg.Config.Port, cfg.Config.Database)
		groupedConfigs[key] = append(groupedConfigs[key], cfg)
	}

	removeTmpMigrations() // just in case
	for _, cfgs := range groupedConfigs {
		var types []string
		for _, cfg := range cfgs {
			copyMigrationsToTmp([]string{"db/ch_migrations/" + cfg.Type})
			types = append(types, cfg.Type)
		}
		log.Info().Msgf("Running Clickhouse migrations for %s", strings.Join(types, ", "))
		runClickhouseMigrations(cfgs[0].Config)
		removeTmpMigrations()
		log.Info().Msgf("Clickhouse migration completed for %s", strings.Join(types, ", "))
	}

	log.Info().Msg("All Clickhouse migrations completed")

	return nil
}

func runClickhouseMigrations(cfg *config.ClickhouseConfig) error {
	if cfg.Host == "" {
		return nil
	}

	secureParam := "&secure=true"
	if cfg.DisableTLS {
		secureParam = "&secure=false"
	}

	url := fmt.Sprintf("clickhouse://%s:%d/%s?username=%s&password=%s%s&x-multi-statement=true&x-migrations-table-engine=MergeTree",
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.Username,
		cfg.Password,
		secureParam,
	)

	m, err := migrate.New("file://db/ch_migrations/tmp", url)
	if err != nil {
		return err
	}
	m.Up()

	m.Close()

	return nil
}

func copyMigrationsToTmp(sources []string) error {
	destination := "db/ch_migrations/tmp"
	err := os.MkdirAll(destination, os.ModePerm)
	if err != nil {
		return err
	}

	for _, source := range sources {
		filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			// skip directories
			if info.IsDir() {
				return nil
			}
			// determine destination path
			relPath, err := filepath.Rel(source, path)
			if err != nil {
				return err
			}
			destPath := filepath.Join(destination, relPath)
			if err := os.MkdirAll(filepath.Dir(destPath), os.ModePerm); err != nil {
				return err
			}

			return copyFile(path, destPath)
		})
	}
	return nil
}

func copyFile(src string, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func removeTmpMigrations() error {
	err := os.RemoveAll("db/ch_migrations/tmp")
	if err != nil {
		return fmt.Errorf("error removing directory: %w", err)
	}
	return nil
}
