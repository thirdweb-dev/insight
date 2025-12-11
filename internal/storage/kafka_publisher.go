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

	"github.com/klauspost/compress/zstd"
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

type MessageType string

type PublishableData interface {
	GetType() MessageType
}

type PublishableMessagePayload struct {
	Data      PublishableData `json:"data"`
	Type      MessageType     `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
}

type PublishableMessageBlockData struct {
	*common.BlockData
	ChainId         uint64    `json:"chain_id"`
	IsDeleted       int8      `json:"is_deleted"`
	InsertTimestamp time.Time `json:"insert_timestamp"`
}

type PublishableMessageRevert struct {
	ChainId         uint64    `json:"chain_id"`
	BlockNumber     uint64    `json:"block_number"`
	IsDeleted       int8      `json:"is_deleted"`
	InsertTimestamp time.Time `json:"insert_timestamp"`
}

func (b PublishableMessageBlockData) GetType() MessageType {
	return "block_data"
}

func (b PublishableMessageRevert) GetType() MessageType {
	return "revert"
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
		kgo.ProducerBatchMaxBytes(100 * 1024 * 1024), // 100MB
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
		client: client,
	}

	return publisher, nil
}

func (p *KafkaPublisher) PublishBlockData(blockData []*common.BlockData) error {
	return p.publishBlockData(blockData, false, false)
}

func (p *KafkaPublisher) PublishBlockDataReorg(newBlockData []*common.BlockData, oldBlockData []*common.BlockData) error {
	log.Debug().Int("new_block_data_count", len(newBlockData)).Int("old_block_data_count", len(oldBlockData)).Msg("PublishBlockDataReorg: Publishing block data reorg")
	if err := p.publishBlockData(oldBlockData, true, true); err != nil {
		return fmt.Errorf("failed to publish old block data: %v", err)
	}

	if err := p.publishBlockData(newBlockData, false, true); err != nil {
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

	// Track if any produce errors occur
	var produceErrors []error
	var produceErrorsMu sync.Mutex
	var wg sync.WaitGroup

	// Produce all messages in the transaction
	for _, msg := range messages {
		wg.Add(1)
		p.client.Produce(ctx, msg, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				log.Error().Err(err).Any("headers", r.Headers).Msg("KAFKA PUBLISHER::publishMessages::err")
				produceErrorsMu.Lock()
				produceErrors = append(produceErrors, err)
				produceErrorsMu.Unlock()
			}
		})
	}

	// Flush all messages
	if err := p.client.Flush(ctx); err != nil {
		p.client.EndTransaction(ctx, kgo.TryAbort)
		return fmt.Errorf("failed to flush messages: %v", err)
	}

	// Wait for all callbacks to complete
	wg.Wait()

	// Check if any produce errors occurred
	hasErrors := len(produceErrors) > 0

	if hasErrors {
		// Abort the transaction if any produce errors occurred
		p.client.EndTransaction(ctx, kgo.TryAbort)
		return fmt.Errorf("transaction aborted due to produce errors: %v", produceErrors)
	}

	// Commit the transaction
	if err := p.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func (p *KafkaPublisher) publishBlockData(blockData []*common.BlockData, isDeleted bool, isReorg bool) error {
	if len(blockData) == 0 {
		return nil
	}

	publishStart := time.Now()

	// Filter out nil blocks and prepare messages
	blockMessages := make([]*kgo.Record, 0, len(blockData))

	for _, data := range blockData {
		// Skip nil blocks
		if data == nil {
			log.Warn().Msg("Skipping nil block in publishBlockData")
			continue
		}

		// Block message
		if blockMsg, err := p.createBlockDataMessage(data, isDeleted, isReorg); err == nil {
			blockMessages = append(blockMessages, blockMsg)
		} else {
			return fmt.Errorf("failed to create block message: %v", err)
		}
	}

	if len(blockMessages) == 0 {
		log.Warn().Msg("No valid blocks to publish after filtering")
		return nil
	}

	if err := p.publishMessages(context.Background(), blockMessages); err != nil {
		return fmt.Errorf("failed to publish block messages: %v", err)
	}

	log.Debug().Str("metric", "publish_duration").Msgf("Publisher.PublishBlockData duration: %f", time.Since(publishStart).Seconds())
	return nil
}

func (p *KafkaPublisher) createBlockDataMessage(block *common.BlockData, isDeleted bool, isReorg bool) (*kgo.Record, error) {
	if block == nil {
		return nil, fmt.Errorf("block is nil")
	}

	timestamp := time.Now()

	data := PublishableMessageBlockData{
		BlockData:       block,
		ChainId:         block.Block.ChainId.Uint64(),
		IsDeleted:       0,
		InsertTimestamp: timestamp,
	}
	if isDeleted {
		data.IsDeleted = 1
	}

	msg := PublishableMessagePayload{
		Data:      data,
		Type:      data.GetType(),
		Timestamp: timestamp,
	}

	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %v", err)
	}

	log.Debug().
		Uint64("chain_id", data.ChainId).
		Uint64("block_number", block.Block.Number.Uint64()).
		Int("tx_count", len(block.Transactions)).
		Int("log_count", len(block.Logs)).
		Int("trace_count", len(block.Traces)).
		Bool("is_deleted", isDeleted).
		Bool("is_reorg", isReorg).
		Msg("KafkaPublisher Message: Block metadata")

	return p.createRecord(data.GetType(), data.ChainId, block.Block.Number.Uint64(), timestamp, isDeleted, isReorg, msgJson)
}

func (p *KafkaPublisher) createBlockRevertMessage(chainId uint64, blockNumber uint64) (*kgo.Record, error) {
	timestamp := time.Now()

	data := PublishableMessageRevert{
		ChainId:         chainId,
		BlockNumber:     blockNumber,
		IsDeleted:       0,
		InsertTimestamp: timestamp,
	}

	msg := PublishableMessagePayload{
		Data:      data,
		Type:      data.GetType(),
		Timestamp: timestamp,
	}

	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %v", err)
	}

	return p.createRecord(data.GetType(), chainId, blockNumber, timestamp, false, false, msgJson)
}

func (p *KafkaPublisher) createRecord(msgType MessageType, chainId uint64, blockNumber uint64, timestamp time.Time, isDeleted bool, isReorg bool, msgJson []byte) (*kgo.Record, error) {
	compressionThreshold := config.Cfg.CommitterCompressionThresholdMB * 1024 * 1024

	var value []byte
	var contentType string

	if len(msgJson) >= compressionThreshold {
		encoder, err := zstd.NewWriter(nil)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create zstd encoder")
		}
		defer encoder.Close()

		value = encoder.EncodeAll([]byte(msgJson), nil)
		contentType = "zstd"
	} else {
		value = msgJson
		contentType = "json"
	}

	// Create headers with metadata
	headers := []kgo.RecordHeader{
		{Key: "chain_id", Value: []byte(fmt.Sprintf("%d", chainId))},         // order is important. always 0
		{Key: "block_number", Value: []byte(fmt.Sprintf("%d", blockNumber))}, // order is important. always 1
		{Key: "is_reorg", Value: []byte(fmt.Sprintf("%t", isReorg))},         // order is important. always 2
		{Key: "is_deleted", Value: []byte(fmt.Sprintf("%t", isDeleted))},     // order is important. always 3
		{Key: "type", Value: []byte(fmt.Sprintf("%s", msgType))},
		{Key: "timestamp", Value: []byte(timestamp.Format(time.RFC3339Nano))},
		{Key: "schema_version", Value: []byte("1")},
		{Key: "content-type", Value: []byte(contentType)},
	}

	return &kgo.Record{
		Topic:     fmt.Sprintf("insight.commit.blocks.%d", chainId),
		Key:       []byte(fmt.Sprintf("%d:%s:%d", chainId, msgType, blockNumber)),
		Value:     value,
		Headers:   headers,
		Partition: 0,
	}, nil
}
