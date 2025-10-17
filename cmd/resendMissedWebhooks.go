package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

var (
	resendMissedWebhooksCmd = &cobra.Command{
		Use:   "resend-missed-webhooks",
		Short: "TBD",
		Long:  "TBD",
		Run: func(cmd *cobra.Command, args []string) {
			RunResendMissedWebhooks(cmd, args)
		},
	}
)

func RunResendMissedWebhooks(cmd *cobra.Command, args []string) {
	log.Info().Msg("RunResendMissedWebhooks")

	startTimestamp := "2025-10-15T20:28:36Z" // utc timestamp
	endTimestamp := "2025-10-16T21:37:17Z"   // utc timestamp
	chainIds := config.Cfg.ResubmitWebhooksChainIds

	kp := getKafkaPublisher()

	for _, chainId := range chainIds {
		startBlockNumber, endBlockNumber, err := libs.GetBlockRangeForTimestampClickHouseV2(chainId, startTimestamp, endTimestamp)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get block range")
			continue
		}

		for blockNumber := startBlockNumber; blockNumber <= endBlockNumber; blockNumber += 100 {
			blockData, err := libs.GetBlockDataFromClickHouseV2(chainId, blockNumber, min(blockNumber+99, endBlockNumber))
			if err != nil {
				log.Error().Err(err).Msg("Failed to get block data")
				continue
			}
			log.Info().Msgf("Block data: %+v", blockData)

			kp.PublishBlockData(blockData)
		}
	}
}

func getKafkaPublisher() *storage.KafkaPublisher {
	kp, err := storage.NewKafkaPublisher(&config.KafkaConfig{
		Brokers:   config.Cfg.ResubmitWebhooksKafkaBrokers,
		Username:  config.Cfg.ResubmitWebhooksKafkaUsername,
		Password:  config.Cfg.ResubmitWebhooksKafkaPassword,
		EnableTLS: config.Cfg.ResubmitWebhooksKafkaEnableTLS,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka publisher")
	}
	return kp
}
