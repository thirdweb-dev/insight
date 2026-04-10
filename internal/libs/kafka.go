package libs

import (
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

var KafkaPublisherV2 *storage.KafkaPublisher

func InitKafkaV2() {
	InitKafkaV2ForRole("committer")
}

func InitKafkaV2ForRole(role string) {
	var err error
	KafkaPublisherV2, err = storage.NewKafkaPublisher(&config.KafkaConfig{
		Brokers:      config.Cfg.CommitterKafkaBrokers,
		Username:     config.Cfg.CommitterKafkaUsername,
		Password:     config.Cfg.CommitterKafkaPassword,
		EnableTLS:    config.Cfg.CommitterKafkaEnableTLS,
		ProducerRole: role,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka publisher")
	}
}
