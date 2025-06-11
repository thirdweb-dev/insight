package publisher

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

type Publisher struct {
	client *kgo.Client
	mu     sync.RWMutex
}

var (
	instance *Publisher
	once     sync.Once
)

type PublishableMessage[T common.BlockModel | common.TransactionModel | common.LogModel | common.TraceModel] struct {
	Data   T      `json:"data"`
	Status string `json:"status"`
}

// GetInstance returns the singleton Publisher instance
func GetInstance() *Publisher {
	once.Do(func() {
		instance = &Publisher{}
		if err := instance.initialize(); err != nil {
			log.Error().Err(err).Msg("Failed to initialize publisher")
		}
	})
	return instance
}

func (p *Publisher) initialize() error {
	if !config.Cfg.Publisher.Enabled {
		log.Debug().Msg("Publisher is disabled, skipping initialization")
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if config.Cfg.Publisher.Brokers == "" {
		log.Info().Msg("No Kafka brokers configured, skipping publisher initialization")
		return nil
	}

	brokers := strings.Split(config.Cfg.Publisher.Brokers, ",")
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

	if config.Cfg.Publisher.Username != "" && config.Cfg.Publisher.Password != "" {
		opts = append(opts, kgo.SASL(plain.Auth{
			User: config.Cfg.Publisher.Username,
			Pass: config.Cfg.Publisher.Password,
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

func (p *Publisher) PublishBlockData(blockData []common.BlockData) error {
	return p.publishBlockData(blockData, false)
}

func (p *Publisher) PublishReorg(oldData []common.BlockData, newData []common.BlockData) error {
	if err := p.publishBlockData(oldData, true); err != nil {
		return fmt.Errorf("failed to publish old block data: %v", err)
	}

	if err := p.publishBlockData(newData, false); err != nil {
		return fmt.Errorf("failed to publish new block data: %v", err)
	}
	return nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		p.client.Close()
		log.Debug().Msg("Publisher client closed")
	}
	return nil
}

func (p *Publisher) publishMessages(ctx context.Context, messages []*kgo.Record) error {
	if len(messages) == 0 {
		return nil
	}

	if !config.Cfg.Publisher.Enabled {
		log.Debug().Msg("Publisher is disabled, skipping publish")
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

func (p *Publisher) publishBlockData(blockData []common.BlockData, isReorg bool) error {
	if p.client == nil || len(blockData) == 0 {
		return nil
	}

	publishStart := time.Now()

	// Prepare messages for blocks, events, transactions and traces
	blockMessages := make([]*kgo.Record, len(blockData))
	var eventMessages []*kgo.Record
	var txMessages []*kgo.Record
	var traceMessages []*kgo.Record

	status := "new"
	if isReorg {
		status = "reverted"
	}

	for i, data := range blockData {
		// Block message
		if config.Cfg.Publisher.Blocks.Enabled {
			if blockMsg, err := p.createBlockMessage(data.Block, status); err == nil {
				blockMessages[i] = blockMsg
			} else {
				return fmt.Errorf("failed to create block message: %v", err)
			}
		}

		// Event messages
		if config.Cfg.Publisher.Events.Enabled {
			for _, event := range data.Logs {
				if p.shouldPublishEvent(event) {
					if eventMsg, err := p.createEventMessage(event, status); err == nil {
						eventMessages = append(eventMessages, eventMsg)
					} else {
						return fmt.Errorf("failed to create event message: %v", err)
					}
				}
			}
		}

		// Transaction messages
		if config.Cfg.Publisher.Transactions.Enabled {
			for _, tx := range data.Transactions {
				if p.shouldPublishTransaction(tx) {
					if txMsg, err := p.createTransactionMessage(tx, status); err == nil {
						txMessages = append(txMessages, txMsg)
					} else {
						return fmt.Errorf("failed to create transaction message: %v", err)
					}
				}
			}
		}

		// Trace messages
		if config.Cfg.Publisher.Traces.Enabled {
			for _, trace := range data.Traces {
				if traceMsg, err := p.createTraceMessage(trace, status); err == nil {
					traceMessages = append(traceMessages, traceMsg)
				} else {
					return fmt.Errorf("failed to create trace message: %v", err)
				}
			}
		}
	}

	if config.Cfg.Publisher.Blocks.Enabled {
		if err := p.publishMessages(context.Background(), blockMessages); err != nil {
			return fmt.Errorf("failed to publish block messages: %v", err)
		}
	}

	if config.Cfg.Publisher.Events.Enabled {
		if err := p.publishMessages(context.Background(), eventMessages); err != nil {
			return fmt.Errorf("failed to publish event messages: %v", err)
		}
	}

	if config.Cfg.Publisher.Transactions.Enabled {
		if err := p.publishMessages(context.Background(), txMessages); err != nil {
			return fmt.Errorf("failed to publish transaction messages: %v", err)
		}
	}

	if config.Cfg.Publisher.Traces.Enabled {
		if err := p.publishMessages(context.Background(), traceMessages); err != nil {
			return fmt.Errorf("failed to publish trace messages: %v", err)
		}
	}

	log.Debug().Str("metric", "publish_duration").Msgf("Publisher.PublishBlockData duration: %f", time.Since(publishStart).Seconds())
	metrics.PublishDuration.Observe(time.Since(publishStart).Seconds())
	metrics.PublisherBlockCounter.Add(float64(len(blockData)))
	metrics.LastPublishedBlock.Set(float64(blockData[len(blockData)-1].Block.Number.Int64()))
	if isReorg {
		metrics.PublisherReorgedBlockCounter.Add(float64(len(blockData)))
	}
	return nil
}

func (p *Publisher) createBlockMessage(block common.Block, status string) (*kgo.Record, error) {
	msg := PublishableMessage[common.BlockModel]{
		Data:   block.Serialize(),
		Status: status,
	}
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal block data: %v", err)
	}
	return &kgo.Record{
		Topic: p.getTopicName("blocks"),
		Key:   []byte(fmt.Sprintf("block-%s-%s-%s", status, block.ChainId.String(), block.Hash)),
		Value: msgJson,
	}, nil
}

func (p *Publisher) createTransactionMessage(tx common.Transaction, status string) (*kgo.Record, error) {
	msg := PublishableMessage[common.TransactionModel]{
		Data:   tx.Serialize(),
		Status: status,
	}
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transaction data: %v", err)
	}
	return &kgo.Record{
		Topic: p.getTopicName("transactions"),
		Key:   []byte(fmt.Sprintf("transaction-%s-%s-%s", status, tx.ChainId.String(), tx.Hash)),
		Value: msgJson,
	}, nil
}

func (p *Publisher) createTraceMessage(trace common.Trace, status string) (*kgo.Record, error) {
	msg := PublishableMessage[common.TraceModel]{
		Data:   trace.Serialize(),
		Status: status,
	}
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal trace data: %v", err)
	}
	traceAddressStr := make([]string, len(trace.TraceAddress))
	for i, addr := range trace.TraceAddress {
		traceAddressStr[i] = fmt.Sprint(addr)
	}
	return &kgo.Record{
		Topic: p.getTopicName("traces"),
		Key:   []byte(fmt.Sprintf("trace-%s-%s-%s-%v", status, trace.ChainID.String(), trace.TransactionHash, strings.Join(traceAddressStr, ","))),
		Value: msgJson,
	}, nil
}

func (p *Publisher) createEventMessage(event common.Log, status string) (*kgo.Record, error) {
	msg := PublishableMessage[common.LogModel]{
		Data:   event.Serialize(),
		Status: status,
	}
	msgJson, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %v", err)
	}
	return &kgo.Record{
		Topic: p.getTopicName("events"),
		Key:   []byte(fmt.Sprintf("event-%s-%s-%s-%d", status, event.ChainId.String(), event.TransactionHash, event.LogIndex)),
		Value: msgJson,
	}, nil
}

func (p *Publisher) shouldPublishEvent(event common.Log) bool {
	if len(config.Cfg.Publisher.Events.AddressFilter) > 0 {
		for _, addr := range config.Cfg.Publisher.Events.AddressFilter {
			if addr == event.Address {
				return true
			}
		}
		return false
	}

	if len(config.Cfg.Publisher.Events.Topic0Filter) > 0 {
		for _, topic0 := range config.Cfg.Publisher.Events.Topic0Filter {
			if topic0 == event.Topic0 {
				return true
			}
		}
		return false
	}
	return true
}

func (p *Publisher) shouldPublishTransaction(tx common.Transaction) bool {
	if len(config.Cfg.Publisher.Transactions.ToFilter) > 0 {
		for _, addr := range config.Cfg.Publisher.Transactions.ToFilter {
			if addr == tx.ToAddress {
				return true
			}
		}
		return false
	}

	if len(config.Cfg.Publisher.Transactions.FromFilter) > 0 {
		for _, addr := range config.Cfg.Publisher.Transactions.FromFilter {
			if addr == tx.FromAddress {
				return true
			}
		}
		return false
	}
	return true
}

func (p *Publisher) getTopicName(entity string) string {
	chainIdSuffix := ""
	if config.Cfg.RPC.ChainID != "" {
		chainIdSuffix = fmt.Sprintf(".%s", config.Cfg.RPC.ChainID)
	}
	switch entity {
	case "blocks":
		if config.Cfg.Publisher.Blocks.TopicName != "" {
			return config.Cfg.Publisher.Blocks.TopicName
		}
		return fmt.Sprintf("insight.blocks%s", chainIdSuffix)
	case "transactions":
		if config.Cfg.Publisher.Transactions.TopicName != "" {
			return config.Cfg.Publisher.Transactions.TopicName
		}
		return fmt.Sprintf("insight.transactions%s", chainIdSuffix)
	case "traces":
		if config.Cfg.Publisher.Traces.TopicName != "" {
			return config.Cfg.Publisher.Traces.TopicName
		}
		return fmt.Sprintf("insight.traces%s", chainIdSuffix)
	case "events":
		if config.Cfg.Publisher.Events.TopicName != "" {
			return config.Cfg.Publisher.Events.TopicName
		}
		return fmt.Sprintf("insight.events%s", chainIdSuffix)
	default:
		return ""
	}
}
