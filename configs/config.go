package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type LogConfig struct {
	Level    string `mapstructure:"level"`
	Prettify bool   `mapstructure:"prettify"`
}

type PollerConfig struct {
	Enabled         bool            `mapstructure:"enabled"`
	ParallelPollers int             `mapstructure:"parallelPollers"`
	S3              *S3SourceConfig `mapstructure:"s3"`
}

type CommitterConfig struct {
	Enabled         bool `mapstructure:"enabled"`
	BlocksPerCommit int  `mapstructure:"blocksPerCommit"`
	FromBlock       int  `mapstructure:"fromBlock"`
	ToBlock         int  `mapstructure:"toBlock"`
}

type ReorgHandlerConfig struct {
	Enabled        bool `mapstructure:"enabled"`
	Interval       int  `mapstructure:"interval"`
	BlocksPerScan  int  `mapstructure:"blocksPerScan"`
	FromBlock      int  `mapstructure:"fromBlock"`
	ForceFromBlock bool `mapstructure:"forceFromBlock"`
}

type StorageConfig struct {
	Orchestrator StorageOrchestratorConfig `mapstructure:"orchestrator"`
	Staging      StorageStagingConfig      `mapstructure:"staging"`
	Main         StorageMainConfig         `mapstructure:"main"`
}

type StorageOrchestratorConfig struct {
	Type       string            `mapstructure:"type"`
	Clickhouse *ClickhouseConfig `mapstructure:"clickhouse"`
	Postgres   *PostgresConfig   `mapstructure:"postgres"`
	Redis      *RedisConfig      `mapstructure:"redis"`
	Badger     *BadgerConfig     `mapstructure:"badger"`
	Pebble     *PebbleConfig     `mapstructure:"pebble"`
}

type StorageStagingConfig struct {
	Type       string            `mapstructure:"type"`
	Clickhouse *ClickhouseConfig `mapstructure:"clickhouse"`
	Postgres   *PostgresConfig   `mapstructure:"postgres"`
	Badger     *BadgerConfig     `mapstructure:"badger"`
	Pebble     *PebbleConfig     `mapstructure:"pebble"`
}

type StorageMainConfig struct {
	Type       string            `mapstructure:"type"`
	Clickhouse *ClickhouseConfig `mapstructure:"clickhouse"`
	Postgres   *PostgresConfig   `mapstructure:"postgres"`
	Kafka      *KafkaConfig      `mapstructure:"kafka"`
	S3         *S3StorageConfig  `mapstructure:"s3"`
}

type BadgerConfig struct {
	Path string `mapstructure:"path"`
}

type PebbleConfig struct {
	Path string `mapstructure:"path"`
}

type S3Config struct {
	Bucket          string `mapstructure:"bucket"`
	Region          string `mapstructure:"region"`
	Prefix          string `mapstructure:"prefix"`
	AccessKeyID     string `mapstructure:"accessKeyId"`
	SecretAccessKey string `mapstructure:"secretAccessKey"`
	Endpoint        string `mapstructure:"endpoint"`
}

type S3StorageConfig struct {
	S3Config `mapstructure:",squash"`
	Format   string         `mapstructure:"format"`
	Parquet  *ParquetConfig `mapstructure:"parquet"`
	// Buffering configuration
	BufferSize       int64 `mapstructure:"bufferSizeMB"`         // Target buffer size in MB before flush
	BufferTimeout    int   `mapstructure:"bufferTimeoutSeconds"` // Max time in seconds before flush
	MaxBlocksPerFile int   `mapstructure:"maxBlocksPerFile"`     // Max blocks per parquet file (0 = no limit, only size/timeout triggers)
}

type ParquetConfig struct {
	Compression  string `mapstructure:"compression"`
	RowGroupSize int64  `mapstructure:"rowGroupSize"`
	PageSize     int64  `mapstructure:"pageSize"`
}

type TableConfig struct {
	DefaultSelectFields []string `mapstructure:"defaultSelectFields"`
	TableName           string   `mapstructure:"tableName"`
}

type TableOverrideConfig map[string]TableConfig

type ClickhouseConfig struct {
	Host                         string                         `mapstructure:"host"`
	Port                         int                            `mapstructure:"port"`
	Username                     string                         `mapstructure:"username"`
	Password                     string                         `mapstructure:"password"`
	Database                     string                         `mapstructure:"database"`
	DisableTLS                   bool                           `mapstructure:"disableTLS"`
	AsyncInsert                  bool                           `mapstructure:"asyncInsert"`
	MaxRowsPerInsert             int                            `mapstructure:"maxRowsPerInsert"`
	MaxOpenConns                 int                            `mapstructure:"maxOpenConns"`
	MaxIdleConns                 int                            `mapstructure:"maxIdleConns"`
	ChainBasedConfig             map[string]TableOverrideConfig `mapstructure:"chainBasedConfig"`
	EnableParallelViewProcessing bool                           `mapstructure:"enableParallelViewProcessing"`
	MaxQueryTime                 int                            `mapstructure:"maxQueryTime"`
	MaxMemoryUsage               int                            `mapstructure:"maxMemoryUsage"`
	EnableCompression            bool                           `mapstructure:"enableCompression"`
}

type PostgresConfig struct {
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	Username        string `mapstructure:"username"`
	Password        string `mapstructure:"password"`
	Database        string `mapstructure:"database"`
	SSLMode         string `mapstructure:"sslMode"`
	MaxOpenConns    int    `mapstructure:"maxOpenConns"`
	MaxIdleConns    int    `mapstructure:"maxIdleConns"`
	MaxConnLifetime int    `mapstructure:"maxConnLifetime"`
	ConnectTimeout  int    `mapstructure:"connectTimeout"`
}

type RedisConfig struct {
	Host      string `mapstructure:"host"`
	Port      int    `mapstructure:"port"`
	Password  string `mapstructure:"password"`
	DB        int    `mapstructure:"db"`
	EnableTLS bool   `mapstructure:"enableTLS"`
}

type KafkaConfig struct {
	Brokers   string `mapstructure:"brokers"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
	EnableTLS bool   `mapstructure:"enableTLS"`
}

type RPCBatchRequestConfig struct {
	BlocksPerRequest int `mapstructure:"blocksPerRequest"`
	BatchDelay       int `mapstructure:"batchDelay"`
}

type ToggleableRPCBatchRequestConfig struct {
	Enabled bool `mapstructure:"enabled"`
	RPCBatchRequestConfig
}

type RPCConfig struct {
	URL           string                          `mapstructure:"url"`
	Blocks        RPCBatchRequestConfig           `mapstructure:"blocks"`
	Logs          RPCBatchRequestConfig           `mapstructure:"logs"`
	BlockReceipts ToggleableRPCBatchRequestConfig `mapstructure:"blockReceipts"`
	Traces        ToggleableRPCBatchRequestConfig `mapstructure:"traces"`
	ChainID       string                          `mapstructure:"chainId"`
}

type BasicAuthConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type ThirdwebConfig struct {
	ClientId string `mapstructure:"clientId"`
}

type ContractApiRequestConfig struct {
	MaxIdleConns        int  `mapstructure:"maxIdleConns"`
	MaxIdleConnsPerHost int  `mapstructure:"maxIdleConnsPerHost"`
	MaxConnsPerHost     int  `mapstructure:"maxConnsPerHost"`
	IdleConnTimeout     int  `mapstructure:"idleConnTimeout"`
	DisableCompression  bool `mapstructure:"disableCompression"`
	Timeout             int  `mapstructure:"timeout"`
}

type APIConfig struct {
	Host                string                   `mapstructure:"host"`
	BasicAuth           BasicAuthConfig          `mapstructure:"basicAuth"`
	ThirdwebContractApi string                   `mapstructure:"thirdwebContractApi"`
	ContractApiRequest  ContractApiRequestConfig `mapstructure:"contractApiRequest"`
	AbiDecodingEnabled  bool                     `mapstructure:"abiDecodingEnabled"`
	Thirdweb            ThirdwebConfig           `mapstructure:"thirdweb"`
}

type BlockPublisherConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	TopicName string `mapstructure:"topicName"`
}

type TransactionPublisherConfig struct {
	Enabled    bool     `mapstructure:"enabled"`
	TopicName  string   `mapstructure:"topicName"`
	ToFilter   []string `mapstructure:"toFilter"`
	FromFilter []string `mapstructure:"fromFilter"`
}

type TracePublisherConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	TopicName string `mapstructure:"topicName"`
}

type EventPublisherConfig struct {
	Enabled       bool     `mapstructure:"enabled"`
	TopicName     string   `mapstructure:"topicName"`
	AddressFilter []string `mapstructure:"addressFilter"`
	Topic0Filter  []string `mapstructure:"topic0Filter"`
}

type PublisherConfig struct {
	Enabled      bool                       `mapstructure:"enabled"`
	Mode         string                     `mapstructure:"mode"`
	Brokers      string                     `mapstructure:"brokers"`
	Username     string                     `mapstructure:"username"`
	Password     string                     `mapstructure:"password"`
	EnableTLS    bool                       `mapstructure:"enableTLS"`
	Blocks       BlockPublisherConfig       `mapstructure:"blocks"`
	Transactions TransactionPublisherConfig `mapstructure:"transactions"`
	Traces       TracePublisherConfig       `mapstructure:"traces"`
	Events       EventPublisherConfig       `mapstructure:"events"`
}

type S3SourceConfig struct {
	S3Config               `mapstructure:",squash"`
	CacheDir               string        `mapstructure:"cacheDir"`
	MetadataTTL            time.Duration `mapstructure:"metadataTTL"`
	FileCacheTTL           time.Duration `mapstructure:"fileCacheTTL"`
	MaxCacheSize           int64         `mapstructure:"maxCacheSize"`
	CleanupInterval        time.Duration `mapstructure:"cleanupInterval"`
	MaxConcurrentDownloads int           `mapstructure:"maxConcurrentDownloads"`
}

type ValidationConfig struct {
	Mode string `mapstructure:"mode"` // "disabled", "minimal", "strict"
}

type MigratorConfig struct {
	Destination StorageMainConfig `mapstructure:"destination"`
	StartBlock  uint              `mapstructure:"startBlock"`
	EndBlock    uint              `mapstructure:"endBlock"`
	BatchSize   uint              `mapstructure:"batchSize"`
	WorkerCount uint              `mapstructure:"workerCount"`
}

type Config struct {
	RPC          RPCConfig          `mapstructure:"rpc"`
	Log          LogConfig          `mapstructure:"log"`
	Poller       PollerConfig       `mapstructure:"poller"`
	Committer    CommitterConfig    `mapstructure:"committer"`
	ReorgHandler ReorgHandlerConfig `mapstructure:"reorgHandler"`
	Storage      StorageConfig      `mapstructure:"storage"`
	API          APIConfig          `mapstructure:"api"`
	Publisher    PublisherConfig    `mapstructure:"publisher"`
	Validation   ValidationConfig   `mapstructure:"validation"`
	Migrator     MigratorConfig     `mapstructure:"migrator"`

	CommitterClickhouseDatabase  string `env:"COMMITTER_CLICKHOUSE_DATABASE"`
	CommitterClickhouseHost      string `env:"COMMITTER_CLICKHOUSE_HOST"`
	CommitterClickhousePort      int    `env:"COMMITTER_CLICKHOUSE_PORT"`
	CommitterClickhouseUsername  string `env:"COMMITTER_CLICKHOUSE_USERNAME"`
	CommitterClickhousePassword  string `env:"COMMITTER_CLICKHOUSE_PASSWORD"`
	CommitterClickhouseEnableTLS bool   `env:"COMMITTER_CLICKHOUSE_ENABLE_TLS" envDefault:"true"`
	CommitterKafkaBrokers        string `env:"COMMITTER_KAFKA_BROKERS"`
	CommitterKafkaUsername       string `env:"COMMITTER_KAFKA_USERNAME"`
	CommitterKafkaPassword       string `env:"COMMITTER_KAFKA_PASSWORD"`
	CommitterKafkaEnableTLS      bool   `env:"COMMITTER_KAFKA_ENABLE_TLS" envDefault:"true"`

	StagingS3Bucket           string `env:"STAGING_S3_BUCKET" envDefault:"thirdweb-insight-production"`
	StagingS3Region           string `env:"STAGING_S3_REGION" envDefault:"us-west-2"`
	StagingS3AccessKeyID      string `env:"STAGING_S3_ACCESS_KEY_ID"`
	StagingS3SecretAccessKey  string `env:"STAGING_S3_SECRET_ACCESS_KEY"`
	S3MaxParallelFileDownload int    `env:"S3_MAX_PARALLEL_FILE_DOWNLOAD" envDefault:"2"`
}

var Cfg Config

func LoadConfig(cfgFile string) error {
	err := godotenv.Load()
	if err != nil {
		log.Info().Msg("No .env file found")
	}
	err = env.Parse(&Cfg)
	if err != nil {
		panic(err)
	}

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("error reading config file, %s", err)
		}
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("./configs")

		if err := viper.ReadInConfig(); err != nil {
			log.Warn().Msgf("error reading config file, %s", err)
		}

		viper.SetConfigName("secrets")
		err := viper.MergeInConfig()
		if err != nil {
			log.Warn().Msgf("error loading secrets file: %v", err)
		}
	}

	// sets e.g. RPC_URL to rpc.url
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	viper.AutomaticEnv()

	err = viper.Unmarshal(&Cfg)
	if err != nil {
		return fmt.Errorf("error unmarshalling config: %v", err)
	}

	err = setCustomJSONConfigs()
	if err != nil {
		return fmt.Errorf("error setting custom JSON configs: %v", err)
	}

	// Add debug logging
	if clickhouse := Cfg.Storage.Main.Clickhouse; clickhouse != nil {
		log.Debug().
			Interface("chainConfig", clickhouse.ChainBasedConfig).
			Msgf("Loaded chain config %v", clickhouse.ChainBasedConfig)
	}

	return nil
}

func setCustomJSONConfigs() error {
	if chainConfigJSON := os.Getenv("STORAGE_MAIN_CLICKHOUSE_CHAINBASEDCONFIG"); chainConfigJSON != "" {
		var mainChainConfig map[string]TableOverrideConfig
		if err := json.Unmarshal([]byte(chainConfigJSON), &mainChainConfig); err != nil {
			return fmt.Errorf("error parsing main chainBasedConfig JSON: %v", err)
		}
		if Cfg.Storage.Main.Clickhouse != nil {
			Cfg.Storage.Main.Clickhouse.ChainBasedConfig = mainChainConfig
		}
	}
	if chainConfigJSON := os.Getenv("STORAGE_STAGING_CLICKHOUSE_CHAINBASEDCONFIG"); chainConfigJSON != "" {
		var stagingChainConfig map[string]TableOverrideConfig
		if err := json.Unmarshal([]byte(chainConfigJSON), &stagingChainConfig); err != nil {
			return fmt.Errorf("error parsing staging chainBasedConfig JSON: %v", err)
		}
		if Cfg.Storage.Staging.Clickhouse != nil {
			Cfg.Storage.Staging.Clickhouse.ChainBasedConfig = stagingChainConfig
		}
	}
	if chainConfigJSON := os.Getenv("STORAGE_ORCHESTRATOR_CLICKHOUSE_CHAINBASEDCONFIG"); chainConfigJSON != "" {
		var orchestratorChainConfig map[string]TableOverrideConfig
		if err := json.Unmarshal([]byte(chainConfigJSON), &orchestratorChainConfig); err != nil {
			return fmt.Errorf("error parsing orchestrator chainBasedConfig JSON: %v", err)
		}
		if Cfg.Storage.Main.Clickhouse != nil {
			Cfg.Storage.Main.Clickhouse.ChainBasedConfig = orchestratorChainConfig
		}
	}
	return nil
}
