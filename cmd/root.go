package cmd

import (
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	configs "github.com/thirdweb-dev/indexer/configs"
	customLogger "github.com/thirdweb-dev/indexer/internal/log"
)

var (
	// Used for flags.
	cfgFile string

	rootCmd = &cobra.Command{
		Use:   "indexer",
		Short: "TBD",
		Long:  "TBD",
		Run: func(cmd *cobra.Command, args []string) {
			log.Info().Msg("TODO: Starting indexer & api both")
		},
	}
)

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./configs/config.yml)")
	rootCmd.PersistentFlags().String("rpc-url", "", "RPC Url to use for the indexer")
	rootCmd.PersistentFlags().String("log-level", "", "Log level to use for the application")
	rootCmd.PersistentFlags().Bool("poller", true, "Toggle poller")
	rootCmd.PersistentFlags().Bool("commiter", true, "Toggle commiter")
	rootCmd.PersistentFlags().Bool("failure-recoverer", true, "Toggle failure recoverer")
	viper.BindPFlag("rpc.url", rootCmd.PersistentFlags().Lookup("rpc-url"))
	viper.BindPFlag("log.level", rootCmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("poller.enabled", rootCmd.PersistentFlags().Lookup("poller"))
	viper.BindPFlag("commiter.enabled", rootCmd.PersistentFlags().Lookup("commiter"))
	viper.BindPFlag("failure-recoverer.enabled", rootCmd.PersistentFlags().Lookup("failure-recoverer"))

	rootCmd.AddCommand(orchestratorCmd)
	rootCmd.AddCommand(apiCmd)
}

func initConfig() {
	configs.LoadConfig(cfgFile)
	customLogger.InitLogger()
}
