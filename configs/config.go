package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Pretty bool   `mapstructure:"pretty"`
}

type PollerConfig struct {
	Enabled       bool `mapstructure:"enabled"`
	Interval      int  `mapstructure:"interval"`
	BlocksPerPoll int  `mapstructure:"blocksPerPoll"`
	FromBlock     int  `mapstructure:"fromBlock"`
	UntilBlock    int  `mapstructure:"untilBlock"`
}

type CommitterConfig struct {
	Enabled         bool `mapstructure:"enabled"`
	Interval        int  `mapstructure:"interval"`
	BlocksPerCommit int  `mapstructure:"blocksPerCommit"`
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
type StorageType string

const (
	StorageTypeMain         StorageType = "main"
	StorageTypeStaging      StorageType = "staging"
	StorageTypeOrchestrator StorageType = "orchestrator"
)

type StorageConnectionConfig struct {
	Clickhouse *ClickhouseConfig `mapstructure:"clickhouse"`
	Memory     *MemoryConfig     `mapstructure:"memory"`
}

type ClickhouseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
}

type MemoryConfig struct {
	MaxItems int `mapstructure:"maxItems"`
}

type RPCBatchSizeConfig struct {
	BlocksPerRequest int `mapstructure:"blocksPerRequest"`
}

type RPCTracesConfig struct {
	Enabled          bool `mapstructure:"enabled"`
	BlocksPerRequest int  `mapstructure:"blocksPerRequest"`
}

type RPCConfig struct {
	URL    string             `mapstructure:"url"`
	Blocks RPCBatchSizeConfig `mapstructure:"blocks"`
	Logs   RPCBatchSizeConfig `mapstructure:"logs"`
	Traces RPCTracesConfig    `mapstructure:"traces"`
}

type Config struct {
	RPC              RPCConfig              `mapstructure:"rpc"`
	Log              LogConfig              `mapstructure:"log"`
	Poller           PollerConfig           `mapstructure:"poller"`
	Committer        CommitterConfig        `mapstructure:"committer"`
	FailureRecoverer FailureRecovererConfig `mapstructure:"failureRecoverer"`
	Storage          StorageConfig          `mapstructure:"storage"`
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
			return fmt.Errorf("error reading config file, %s", err)
		}

		viper.SetConfigName("secrets")
		err := viper.MergeInConfig()
		if err != nil {
			return fmt.Errorf("error loading secrets file: %v", err)
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

	return nil
}
