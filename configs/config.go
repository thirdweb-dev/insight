package config

import (
	"fmt"
	"strings"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type LogConfig struct {
	Level    string `mapstructure:"level"`
	Prettify bool   `mapstructure:"prettify"`
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

type Config struct {
	RPC                              RPCConfig `mapstructure:"rpc"`
	Log                              LogConfig `mapstructure:"log"`
	ZeetProjectName                  string    `env:"ZEET_PROJECT_NAME" envDefault:"insight-indexer"`
	ZeetDeploymentId                 string    `env:"ZEET_DEPLOYMENT_ID"`
	ZeetClusterId                    string    `env:"ZEET_CLUSTER_ID"`
	CommitterClickhouseDatabase      string    `env:"COMMITTER_CLICKHOUSE_DATABASE"`
	CommitterClickhouseHost          string    `env:"COMMITTER_CLICKHOUSE_HOST"`
	CommitterClickhousePort          int       `env:"COMMITTER_CLICKHOUSE_PORT"`
	CommitterClickhouseUsername      string    `env:"COMMITTER_CLICKHOUSE_USERNAME"`
	CommitterClickhousePassword      string    `env:"COMMITTER_CLICKHOUSE_PASSWORD"`
	CommitterClickhouseEnableTLS     bool      `env:"COMMITTER_CLICKHOUSE_ENABLE_TLS" envDefault:"true"`
	CommitterKafkaBrokers            string    `env:"COMMITTER_KAFKA_BROKERS"`
	CommitterKafkaUsername           string    `env:"COMMITTER_KAFKA_USERNAME"`
	CommitterKafkaPassword           string    `env:"COMMITTER_KAFKA_PASSWORD"`
	CommitterKafkaEnableTLS          bool      `env:"COMMITTER_KAFKA_ENABLE_TLS" envDefault:"true"`
	CommitterMaxMemoryMB             int       `env:"COMMITTER_MAX_MEMORY_MB" envDefault:"512"`
	CommitterCompressionThresholdMB  int       `env:"COMMITTER_COMPRESSION_THRESHOLD_MB" envDefault:"50"`
	CommitterKafkaBatchSize          int       `env:"COMMITTER_KAFKA_BATCH_SIZE" envDefault:"500"`
	CommitterIsLive                  bool      `env:"COMMITTER_IS_LIVE" envDefault:"false"`
	StagingS3Bucket                  string    `env:"STAGING_S3_BUCKET" envDefault:"thirdweb-insight-production"`
	StagingS3Region                  string    `env:"STAGING_S3_REGION" envDefault:"us-west-2"`
	StagingS3AccessKeyID             string    `env:"STAGING_S3_ACCESS_KEY_ID"`
	StagingS3SecretAccessKey         string    `env:"STAGING_S3_SECRET_ACCESS_KEY"`
	StagingS3MaxParallelFileDownload int       `env:"STAGING_S3_MAX_PARALLEL_FILE_DOWNLOAD" envDefault:"2"`
	BackfillStartBlock               uint64    `env:"BACKFILL_START_BLOCK"`
	BackfillEndBlock                 uint64    `env:"BACKFILL_END_BLOCK"`
	RPCNumParallelCalls              uint64    `env:"RPC_NUM_PARALLEL_CALLS" envDefault:"20"`
	RPCBatchSize                     uint64    `env:"RPC_BATCH_SIZE" envDefault:"10"`
	RPCBatchMaxMemoryUsageMB         uint64    `env:"RPC_BATCH_MAX_MEMORY_USAGE_MB" envDefault:"32"`
	ParquetMaxFileSizeMB             int64     `env:"PARQUET_MAX_FILE_SIZE_MB" envDefault:"512"`
	InsightServiceUrl                string    `env:"INSIGHT_SERVICE_URL" envDefault:"https://insight.thirdweb.com"`
	InsightServiceApiKey             string    `env:"INSIGHT_SERVICE_API_KEY"`
	RedisAddr                        string    `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	RedisUsername                    string    `env:"REDIS_USERNAME"`
	RedisPassword                    string    `env:"REDIS_PASSWORD"`
	RedisDB                          int       `env:"REDIS_DB" envDefault:"0"`
	ValidationMode                   string    `env:"VALIDATION_MODE" envDefault:"minimal"`
	EnableReorgValidation            bool      `env:"ENABLE_REORG_VALIDATION" envDefault:"true"`
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

	// Set default values for viper-managed configs
	viper.SetDefault("rpc.blockReceipts.enabled", true)

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

	return nil
}
