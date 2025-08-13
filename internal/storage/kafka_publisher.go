package storage

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
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type KafkaPublisher struct {
	client *kgo.Client
	mu     sync.RWMutex
}

type PublishableMessage[T common.BlockData] struct {
	Data   T      `json:"data"`
	Status string `json:"status"`
}

// NewKafkaPublisher method for storage connector (public)
func NewKafkaPublisher(cfg *config.KafkaConfig) (*KafkaPublisher, error) {
	brokers := strings.Split(cfg.Brokers, ",")
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.ClientID(fmt.Sprintf("insight-indexer-kafka-storage-%s", config.Cfg.RPC.ChainID)),
		kgo.MaxBufferedRecords(1_000_000),
		kgo.ProducerBatchMaxBytes(16_000_000),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(1_000_000, false, false, nil)),
		kgo.MetadataMaxAge(60 * time.Second),
		kgo.DialTimeout(10 * time.Second),
	}

	if cfg.Username != "" && cfg.Password != "" {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsMechanism()))
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Kafka: %v", err)
	}

	publisher := &KafkaPublisher{
		client: client,
	}
	return publisher, nil
}

func (p *KafkaPublisher) PublishBlockData(blockData []common.BlockData) error {
	return p.publishBlockData(blockData, false)
}

func (p *KafkaPublisher) PublishReorg(oldData []common.BlockData, newData []common.BlockData) error {
	// TODO: need to revisit how reorg blocks get published to downstream
	if err := p.publishBlockData(oldData, true); err != nil {
		return fmt.Errorf("failed to publish old block data: %v", err)
	}

	if err := p.publishBlockData(newData, false); err != nil {
		return fmt.Errorf("failed to publish new block data: %v", err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		p.client.Close()
		log.Debug().Msg("Publisher client closed")
	}
	return nil
}

func (p *KafkaPublisher) publishMessages(ctx context.Context, messages []*kgo.Record) error {
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

func (p *KafkaPublisher) publishBlockData(blockData []common.BlockData, isReorg bool) error {
	if len(blockData) == 0 {
		return nil
	}

	publishStart := time.Now()

	// Prepare messages for blocks, events, transactions and traces
	blockMessages := make([]*kgo.Record, len(blockData))

	// TODO: handle reorg
	status := "new"
	if isReorg {
		status = "reverted"
	}

	for i, data := range blockData {
		// Block message
		if blockMsg, err := p.createBlockDataMessage(data, status); err == nil {
			blockMessages[i] = blockMsg
		} else {
			return fmt.Errorf("failed to create block message: %v", err)
		}
	}

	if err := p.publishMessages(context.Background(), blockMessages); err != nil {
		return fmt.Errorf("failed to publish block messages: %v", err)
	}

	log.Debug().Str("metric", "publish_duration").Msgf("Publisher.PublishBlockData duration: %f", time.Since(publishStart).Seconds())
	return nil
}

func (p *KafkaPublisher) createBlockDataMessage(data common.BlockData, status string) (*kgo.Record, error) {
	msg := PublishableMessage[common.BlockData]{
		Data:   data,
		Status: status,
	}
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %v", err)
	}
	return &kgo.Record{
		Topic: p.getTopicName("commit", data.ChainId),
		Key:   []byte(fmt.Sprintf("block-%s-%d-%s", status, data.ChainId, data.Block.Hash)),
		Value: msgJson,
	}, nil
}

func (p *KafkaPublisher) getTopicName(entity string, chainId uint64) string {
	switch entity {
	case "commit":
		return fmt.Sprintf("insight.commit.blocks.%d", chainId)
	default:
		panic(fmt.Errorf("unknown topic entity: %s", entity))
	}
}
