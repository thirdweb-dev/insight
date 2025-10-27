package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Backfill Metrics
var (
	BackfillStartBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_start_block",
		Help: "The starting block number for backfill",
	}, []string{"project_name", "chain_id"})

	BackfillEndBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_end_block",
		Help: "The ending block number for backfill",
	}, []string{"project_name", "chain_id"})

	BackfillComputedBatchSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_computed_batch_size",
		Help: "The computed batch size for backfill processing",
	}, []string{"project_name", "chain_id"})

	BackfillCurrentStartBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_current_start_block",
		Help: "The current start block number being processed",
	}, []string{"project_name", "chain_id"})

	BackfillCurrentEndBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_current_end_block",
		Help: "The current end block number being processed",
	}, []string{"project_name", "chain_id"})

	BackfillAvgMemoryPerBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_avg_memory_per_block_bytes",
		Help: "The average memory usage per block in bytes",
	}, []string{"project_name", "chain_id"})

	BackfillParquetBytesWritten = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_parquet_bytes_written_total",
		Help: "The total number of bytes written to parquet files during backfill",
	}, []string{"project_name", "chain_id"})

	BackfillFlushEndBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_flush_end_block",
		Help: "The end block number of the last flush operation",
	}, []string{"project_name", "chain_id"})

	BackfillBlockdataChannelLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_blockdata_channel_length",
		Help: "The current length of the blockdata channel",
	}, []string{"project_name", "chain_id"})
)

// Committer Streaming Metrics
var (
	CommitterNextBlockNumber = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_next_block_number",
		Help: "The next block number to be processed",
	}, []string{"project_name", "chain_id"})

	CommitterLatestBlockNumber = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_latest_block_number",
		Help: "The latest block number from RPC",
	}, []string{"project_name", "chain_id"})

	CommitterBlockDataChannelLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_block_data_channel_length",
		Help: "The current length of the block data channel",
	}, []string{"project_name", "chain_id"})

	CommitterMemoryPermitBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_memory_permit_bytes",
		Help: "The current memory permit value (bytes in block data channel)",
	}, []string{"project_name", "chain_id"})

	CommitterLastPublishedBlockNumber = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_last_published_block_number",
		Help: "The last published block number to Kafka",
	}, []string{"project_name", "chain_id"})

	CommitterLastPublishedReorgBlockNumber = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_last_published_reorg_block_number",
		Help: "The last published reorg block number to Kafka",
	}, []string{"project_name", "chain_id"})

	CommitterRPCRowsToFetch = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_rpc_rows_to_fetch",
		Help: "The total number of rows to fetch from RPC",
	}, []string{"project_name", "chain_id"})

	CommitterRPCRetries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_rpc_retries_total",
		Help: "The total number of RPC retries",
	}, []string{"project_name", "chain_id"})
)
