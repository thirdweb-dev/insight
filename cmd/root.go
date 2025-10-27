package cmd

import (
	"os"

	"github.com/spf13/cobra"
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

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "optional config file path (defaults to env-only when unset)")
	rootCmd.AddCommand(committerCmd)
	rootCmd.AddCommand(backfillCmd)
}

func initConfig() {
	configs.LoadConfig(cfgFile)
	customLogger.InitLogger()
}
