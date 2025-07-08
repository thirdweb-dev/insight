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
