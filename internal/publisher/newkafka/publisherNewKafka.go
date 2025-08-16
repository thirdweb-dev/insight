package newkafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type NewKafka struct {
	client *kgo.Client
	mu     sync.RWMutex
}

var (
	instance *NewKafka
	once     sync.Once
)

type PublishableMessage[T common.BlockModel | common.TransactionModel | common.LogModel | common.TraceModel] struct {
	Data   T      `json:"data"`
	Status string `json:"status"`
}

// GetInstance returns the singleton Publisher instance
func GetInstance() *NewKafka {
	once.Do(func() {
		instance = &NewKafka{}
		if err := instance.initialize(); err != nil {
			log.Error().Err(err).Msg("Failed to initialize publisher")
		}
	})
	return instance
}

func (p *NewKafka) initialize() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if config.Cfg.NewKafka.Brokers == "" {
		log.Info().Msg("No Kafka brokers configured, skipping publisher initialization")
		return nil
	}

	brokers := strings.Split(config.Cfg.NewKafka.Brokers, ",")
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ClientID(fmt.Sprintf("insight-indexer-%s", config.Cfg.RPC.ChainID)),
		kgo.MaxBufferedRecords(1_000_000),
		kgo.ProducerBatchMaxBytes(16_000_000),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(1_000_000, false, false, nil)),
		kgo.MetadataMaxAge(60 * time.Second),
		kgo.DialTimeout(10 * time.Second),
	}

	if config.Cfg.NewKafka.Username != "" && config.Cfg.NewKafka.Password != "" {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: config.Cfg.NewKafka.Username,
			Pass: config.Cfg.NewKafka.Password,
		}.AsMechanism()))
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		client.Close()
		return fmt.Errorf("failed to connect to Kafka: %v", err)
	}
	p.client = client
	return nil
}

func (p *NewKafka) PublishBlockData(blockData []common.BlockData) error {
	return p.publishBlockData(blockData, false)
}

func (p *NewKafka) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		p.client.Close()
		log.Debug().Msg("Publisher client closed")
	}
	return nil
}

func (p *NewKafka) publishBlockData(blockData []common.BlockData, isReorg bool) error {
	if p.client == nil || len(blockData) == 0 {
		return nil
	}

	publishStart := time.Now()

	// Prepare messages for blocks, events, transactions and traces
	blockdataMessages := make([]*kgo.Record, len(blockData))

	status := "new"
	if isReorg {
		status = "reverted"
	}

	for i, data := range blockData {
		msgJson, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal block data: %v", err)
		}
		blockdataMessages[i] = &kgo.Record{
			Topic: "block_data",
			Key:   []byte(fmt.Sprintf("block-%s-%s-%s", status, data.Block.ChainId.String(), data.Block.Hash)),
			Value: msgJson,
		}
	}

	if err := p.publishMessages(context.Background(), blockdataMessages); err != nil {
		return fmt.Errorf("failed to publish block messages: %v", err)
	}
	log.Debug().
		Str("metric", "publish_duration").
		Str("publisher", "newkafka").
		Msgf("Publisher.PublishBlockData duration: %f", time.Since(publishStart).Seconds())
	metrics.PublishDuration.Observe(time.Since(publishStart).Seconds())
	metrics.PublisherBlockCounter.Add(float64(len(blockData)))
	metrics.LastPublishedBlock.Set(float64(blockData[len(blockData)-1].Block.Number.Int64()))
	if isReorg {
		metrics.PublisherReorgedBlockCounter.Add(float64(len(blockData)))
	}
	return nil
}

func (p *NewKafka) publishMessages(ctx context.Context, messages []*kgo.Record) error {
	if len(messages) == 0 {
		return nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return nil // Skip if no client configured
	}

	var wg sync.WaitGroup
	wg.Add(len(messages))
	// Publish to all configured producers
	for _, msg := range messages {
		p.client.Produce(ctx, msg, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				log.Error().Err(err).Msg("Failed to publish message to Kafka")
			}
		})
	}
	wg.Wait()

	return nil
}
