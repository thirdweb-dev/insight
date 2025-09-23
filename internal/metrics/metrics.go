package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Committer Metrics
var (
	SuccessfulCommits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "committer_successful_commits_total",
		Help: "The total number of successful block commits",
	})

	LastCommittedBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "committer_last_committed_block",
		Help: "The last successfully committed block number",
	})

	CommitterLagInSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "committer_lag_seconds",
		Help: "The lag in seconds between the last committed block and the current timestamp",
	})

	GapCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "committer_gap_counter",
		Help: "The number of gaps detected during commits",
	})

	MissedBlockNumbers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "committer_first_missed_block_number",
		Help: "The first blocknumber detected in a commit gap",
	})
)

// Worker Metrics
var LastFetchedBlock = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "worker_last_fetched_block_from_rpc",
	Help: "The last block number fetched by the worker from the RPC",
})

// ChainTracker Metrics
var (
	ChainHead = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chain_tracker_chain_head",
		Help: "The latest block number in the current chain",
	})
)

// Poller metrics
var (
	PolledBatchSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "polled_batch_size",
		Help: "The number of blocks polled in a single batch",
	})
)

var (
	PollerLastTriggeredBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "poller_last_triggered_block",
		Help: "The last block number that the poller was triggered for",
	})
)

// Failure Recoverer Metrics
var (
	FailureRecovererLastTriggeredBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "failure_recoverer_last_triggered_block",
		Help: "The last block number that the failure recoverer was triggered for",
	})

	FirstBlocknumberInFailureRecovererBatch = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "failure_recoverer_first_block_in_batch",
		Help: "The first block number in the failure recoverer batch",
	})
)

// Reorg Handler Metrics
var (
	ReorgHandlerLastCheckedBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "reorg_handler_last_checked_block",
		Help: "The last block number that the reorg handler checked",
	})

	ReorgCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reorg_handler_reorg_counter",
		Help: "The number of reorgs detected",
	})
)

// Publisher Metrics
var (
	PublisherBlockCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "publisher_block_counter",
		Help: "The number of blocks published",
	})

	PublisherReorgedBlockCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "publisher_reorged_block_counter",
		Help: "The number of reorged blocks published",
	})

	LastPublishedBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "last_published_block",
		Help: "The last block number that was published",
	})
)

// Operation Duration Metrics
var (
	StagingInsertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "staging_insert_duration_seconds",
		Help:    "Time taken to insert data into staging storage",
		Buckets: prometheus.DefBuckets,
	})

	MainStorageInsertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "main_storage_insert_duration_seconds",
		Help:    "Time taken to insert data into main storage",
		Buckets: prometheus.DefBuckets,
	})

	PublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "publish_duration_seconds",
		Help:    "Time taken to publish block data to Kafka",
		Buckets: prometheus.DefBuckets,
	})

	StagingDeleteDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "staging_delete_duration_seconds",
		Help:    "Time taken to delete data from staging storage",
		Buckets: prometheus.DefBuckets,
	})

	GetBlockNumbersToCommitDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "get_block_numbers_to_commit_duration_seconds",
		Help:    "Time taken to get block numbers to commit from storage",
		Buckets: prometheus.DefBuckets,
	})

	GetStagingDataDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "get_staging_data_duration_seconds",
		Help:    "Time taken to get data from staging storage",
		Buckets: prometheus.DefBuckets,
	})
)

// Work Mode Metrics
var (
	CurrentWorkMode = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "current_work_mode",
		Help: "The current work mode (0 = backfill, 1 = live)",
	})
)

// ClickHouse Insert Row Count Metrics
var (
	ClickHouseMainStorageInsertOperations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_main_storage_insert_operations",
		Help: "The total number of insert operations into ClickHouse main storage",
	})

	ClickHouseMainStorageRowsInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_main_storage_rows_inserted_total",
		Help: "The total number of rows inserted into ClickHouse main storage",
	})

	ClickHouseTransactionsInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_transactions_inserted_total",
		Help: "The total number of transactions inserted into ClickHouse",
	})

	ClickHouseLogsInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_logs_inserted_total",
		Help: "The total number of logs inserted into ClickHouse",
	})

	ClickHouseTracesInserted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_traces_inserted_total",
		Help: "The total number of traces inserted into ClickHouse",
	})
)

// Backfill Metrics
var (
	BackfillIndexerName = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_indexer_name",
		Help: "The name of the indexer running backfill",
	}, []string{"project_name", "chain_id", "indexer_name"})

	BackfillChainId = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_chain_id",
		Help: "The chain ID being backfilled",
	}, []string{"project_name", "chain_id"})

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

	BackfillClickHouseRowsFetched = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "backfill_clickhouse_rows_fetched_total",
		Help: "The total number of rows fetched from ClickHouse during backfill",
	}, []string{"project_name", "chain_id"})

	BackfillRPCRowsFetched = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "backfill_rpc_rows_fetched_total",
		Help: "The total number of rows fetched from RPC during backfill",
	}, []string{"project_name", "chain_id"})

	BackfillRPCRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "backfill_rpc_retries_total",
		Help: "The total number of RPC retries with block numbers during backfill",
	}, []string{"project_name", "chain_id"})

	BackfillParquetBytesWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "backfill_parquet_bytes_written_total",
		Help: "The total number of bytes written to parquet files during backfill",
	}, []string{"project_name", "chain_id"})

	BackfillFlushStartBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_flush_start_block",
		Help: "The start block number of the last flush operation",
	}, []string{"project_name", "chain_id"})

	BackfillFlushEndBlock = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_flush_end_block",
		Help: "The end block number of the last flush operation",
	}, []string{"project_name", "chain_id"})

	BackfillFlushBlockTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_flush_block_timestamp",
		Help: "The block timestamp of the last flush operation",
	}, []string{"project_name", "chain_id"})

	BackfillFlushCurrentTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_flush_current_time",
		Help: "The current time when the last flush operation occurred",
	}, []string{"project_name", "chain_id"})

	BackfillBlockdataChannelLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "backfill_blockdata_channel_length",
		Help: "The current length of the blockdata channel",
	}, []string{"project_name", "chain_id"})
)

// Committer Streaming Metrics
var (
	CommitterIndexerName = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_indexer_name",
		Help: "The name of the indexer running committer",
	}, []string{"project_name", "chain_id", "indexer_name"})

	CommitterChainId = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_chain_id",
		Help: "The chain ID being processed by committer",
	}, []string{"project_name", "chain_id"})

	CommitterNextBlockNumber = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_next_block_number",
		Help: "The next block number to be processed",
	}, []string{"project_name", "chain_id"})

	CommitterLatestBlockNumber = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_latest_block_number",
		Help: "The latest block number from RPC",
	}, []string{"project_name", "chain_id"})

	CommitterDownloadedFilePathChannelLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "committer_downloaded_file_path_channel_length",
		Help: "The current length of the downloaded file path channel",
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

	CommitterBlockDataParseDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "committer_block_data_parse_duration_seconds",
		Help:    "Average time taken to parse individual block data rows from parquet files",
		Buckets: prometheus.DefBuckets,
	}, []string{"project_name", "chain_id"})

	CommitterKafkaPublishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "committer_kafka_publish_duration_seconds",
		Help:    "Time taken to publish Kafka batch",
		Buckets: prometheus.DefBuckets,
	}, []string{"project_name", "chain_id"})

	CommitterS3DownloadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "committer_s3_download_duration_seconds",
		Help:    "Time taken to download S3 file",
		Buckets: prometheus.DefBuckets,
	}, []string{"project_name", "chain_id"})

	CommitterRPCRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "committer_rpc_retries_total",
		Help: "The total number of RPC retries",
	}, []string{"project_name", "chain_id"})

	CommitterRPCDownloadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "committer_rpc_download_duration_seconds",
		Help:    "Time taken to download from RPC",
		Buckets: prometheus.DefBuckets,
	}, []string{"project_name", "chain_id"})
)
