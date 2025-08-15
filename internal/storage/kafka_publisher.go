package storage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
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
	client  *kgo.Client
	mu      sync.RWMutex
	chainID string
}

type PublishableBlockMessage struct {
	common.BlockData
	Sign            int8      `json:"sign"`
	InsertTimestamp time.Time `json:"insert_timestamp"`
}

// NewKafkaPublisher method for storage connector (public)
func NewKafkaPublisher(cfg *config.KafkaConfig) (*KafkaPublisher, error) {
	brokers := strings.Split(cfg.Brokers, ",")
	chainID := config.Cfg.RPC.ChainID

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ClientID(fmt.Sprintf("insight-indexer-kafka-storage-%s", chainID)),
		kgo.TransactionalID(fmt.Sprintf("insight-producer-%s", chainID)),
		kgo.MaxBufferedBytes(2 * 1024 * 1024 * 1024), // 2GB
		kgo.MaxBufferedRecords(1_000_000),
		kgo.ProducerBatchMaxBytes(16_000_000),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProduceRequestTimeout(30 * time.Second),
		kgo.MetadataMaxAge(60 * time.Second),
		kgo.DialTimeout(10 * time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RequestRetries(5),
	}

	if cfg.Username != "" && cfg.Password != "" {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsMechanism()))
	}

	if cfg.EnableTLS {
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
		client:  client,
		chainID: chainID,
	}

	return publisher, nil
}

func (p *KafkaPublisher) PublishBlockData(blockData []common.BlockData) error {
	return p.publishBlockData(blockData, false)
}

func (p *KafkaPublisher) PublishReorg(oldData []common.BlockData, newData []common.BlockData) error {
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

	// Lock for the entire transaction lifecycle to ensure thread safety
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return fmt.Errorf("no kafka client configured")
	}

	// Start a new transaction
	if err := p.client.BeginTransaction(); err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Produce all messages in the transaction
	for _, msg := range messages {
		p.client.Produce(ctx, msg, nil)
	}

	// Flush all messages
	if err := p.client.Flush(ctx); err != nil {
		p.client.EndTransaction(ctx, kgo.TryAbort)
		return fmt.Errorf("failed to flush messages: %v", err)
	}

	// Commit the transaction
	if err := p.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func (p *KafkaPublisher) publishBlockData(blockData []common.BlockData, isDeleted bool) error {
	if len(blockData) == 0 {
		return nil
	}

	publishStart := time.Now()

	// Prepare messages for blocks, events, transactions and traces
	blockMessages := make([]*kgo.Record, len(blockData))

	for i, data := range blockData {
		// Block message
		if blockMsg, err := p.createBlockDataMessage(data, isDeleted); err == nil {
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

func (p *KafkaPublisher) createBlockDataMessage(data common.BlockData, isDeleted bool) (*kgo.Record, error) {
	insertTimestamp := time.Now()
	msg := PublishableBlockMessage{
		BlockData:       data.Serialize(),
		Sign:            1,
		InsertTimestamp: insertTimestamp,
	}
	if isDeleted {
		msg.Sign = -1 // Indicate deletion with a negative sign
	}
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %v", err)
	}

	// Determine partition based on chainID
	var partition int32
	if data.ChainId <= math.MaxInt32 {
		// Direct assignment for chain IDs that fit in int32
		partition = int32(data.ChainId)
	} else {
		// Hash for larger chain IDs to avoid overflow
		h := fnv.New32a()
		fmt.Fprintf(h, "%d", data.ChainId)
		partition = int32(h.Sum32() & 0x7FFFFFFF) // Ensure positive
	}

	// Create headers with metadata
	headers := []kgo.RecordHeader{
		{Key: "chain_id", Value: []byte(fmt.Sprintf("%d", data.ChainId))},
		{Key: "block_number", Value: []byte(fmt.Sprintf("%d", data.Block.Number))},
		{Key: "sign", Value: []byte(fmt.Sprintf("%d", msg.Sign))},
		{Key: "insert_timestamp", Value: []byte(insertTimestamp.Format(time.RFC3339Nano))},
		{Key: "schema_version", Value: []byte("1")},
	}

	return &kgo.Record{
		Topic:     "insight.commit.blocks",
		Key:       []byte(fmt.Sprintf("blockdata-%d-%d-%s-%d", data.ChainId, data.Block.Number, data.Block.Hash, msg.Sign)),
		Value:     msgJson,
		Headers:   headers,
		Partition: partition,
	}, nil
}
