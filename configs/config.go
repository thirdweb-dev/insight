package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type LogConfig struct {
	Level    string `mapstructure:"level"`
	Prettify bool   `mapstructure:"prettify"`
}

type PollerConfig struct {
	Enabled         bool `mapstructure:"enabled"`
	Interval        int  `mapstructure:"interval"`
	BlocksPerPoll   int  `mapstructure:"blocksPerPoll"`
	FromBlock       int  `mapstructure:"fromBlock"`
	ForceFromBlock  bool `mapstructure:"forceFromBlock"`
	UntilBlock      int  `mapstructure:"untilBlock"`
	ParallelPollers int  `mapstructure:"parallelPollers"`
}

type CommitterConfig struct {
	Enabled         bool `mapstructure:"enabled"`
	Interval        int  `mapstructure:"interval"`
	BlocksPerCommit int  `mapstructure:"blocksPerCommit"`
	FromBlock       int  `mapstructure:"fromBlock"`
	UntilBlock      int  `mapstructure:"untilBlock"`
}

type ReorgHandlerConfig struct {
	Enabled        bool `mapstructure:"enabled"`
	Interval       int  `mapstructure:"interval"`
	BlocksPerScan  int  `mapstructure:"blocksPerScan"`
	FromBlock      int  `mapstructure:"fromBlock"`
	ForceFromBlock bool `mapstructure:"forceFromBlock"`
}

type FailureRecovererConfig struct {
	Enabled      bool `mapstructure:"enabled"`
	Interval     int  `mapstructure:"interval"`
	BlocksPerRun int  `mapstructure:"blocksPerRun"`
}

type StorageConfig struct {
	Staging      StorageConnectionConfig `mapstructure:"staging"`
	Main         StorageConnectionConfig `mapstructure:"main"`
	Orchestrator StorageConnectionConfig `mapstructure:"orchestrator"`
}

type StorageConnectionConfig struct {
	Type       string            `mapstructure:"type"` // "auto", "clickhouse", "postgres", "kafka", "badger", "s3"
	Clickhouse *ClickhouseConfig `mapstructure:"clickhouse"`
	Postgres   *PostgresConfig   `mapstructure:"postgres"`
	Kafka      *KafkaConfig      `mapstructure:"kafka"`
	Badger     *BadgerConfig     `mapstructure:"badger"`
	S3         *S3Config         `mapstructure:"s3"`
}

type BadgerConfig struct {
	Path string `mapstructure:"path"`
}

type S3Config struct {
	Bucket          string         `mapstructure:"bucket"`
	Region          string         `mapstructure:"region"`
	Prefix          string         `mapstructure:"prefix"`
	AccessKeyID     string         `mapstructure:"accessKeyId"`
	SecretAccessKey string         `mapstructure:"secretAccessKey"`
	Endpoint        string         `mapstructure:"endpoint"`
	Format          string         `mapstructure:"format"`
	Parquet         *ParquetConfig `mapstructure:"parquet"`
	// Buffering configuration
	BufferSize       int64 `mapstructure:"bufferSizeMB"`         // Target buffer size in MB before flush (default 1024 MB = 1GB)
	BufferTimeout    int   `mapstructure:"bufferTimeoutSeconds"` // Max time in seconds before flush (default 300 = 5 min)
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
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type KafkaConfig struct {
	Brokers   string       `mapstructure:"brokers"`
	Username  string       `mapstructure:"username"`
	Password  string       `mapstructure:"password"`
	EnableTLS bool         `mapstructure:"enableTLS"`
	Redis     *RedisConfig `mapstructure:"redis"`
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

type WorkModeConfig struct {
	CheckIntervalMinutes int   `mapstructure:"checkIntervalMinutes"`
	LiveModeThreshold    int64 `mapstructure:"liveModeThreshold"`
}

type ValidationConfig struct {
	Mode string `mapstructure:"mode"` // "disabled", "minimal", "strict"
}

type MigratorConfig struct {
	Destination StorageConnectionConfig `mapstructure:"destination"`
	StartBlock  uint                    `mapstructure:"startBlock"`
	EndBlock    uint                    `mapstructure:"endBlock"`
	BatchSize   uint                    `mapstructure:"batchSize"`
}

type Config struct {
	RPC              RPCConfig              `mapstructure:"rpc"`
	Log              LogConfig              `mapstructure:"log"`
	Poller           PollerConfig           `mapstructure:"poller"`
	Committer        CommitterConfig        `mapstructure:"committer"`
	FailureRecoverer FailureRecovererConfig `mapstructure:"failureRecoverer"`
	ReorgHandler     ReorgHandlerConfig     `mapstructure:"reorgHandler"`
	Storage          StorageConfig          `mapstructure:"storage"`
	API              APIConfig              `mapstructure:"api"`
	Publisher        PublisherConfig        `mapstructure:"publisher"`
	WorkMode         WorkModeConfig         `mapstructure:"workMode"`
	Validation       ValidationConfig       `mapstructure:"validation"`
	Migrator         MigratorConfig         `mapstructure:"migrator"`
}

var Cfg Config

func LoadConfig(cfgFile string) error {
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

	err := viper.Unmarshal(&Cfg)
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
