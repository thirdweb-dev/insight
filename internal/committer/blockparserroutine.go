package committer

import (
	"io"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/metrics"
)

func blockParserRoutine(blockParserDone chan struct{}) {
	defer close(blockParserDone)
	if err := channelParseBlocksFromFile(); err != nil {
		log.Error().Err(err).Msg("Error in parquet parsing goroutine")
	}
}

func channelParseBlocksFromFile() error {
	// Initialize metrics labels
	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName

	for filePath := range downloadedFilePathChannel {
		log.Debug().Str("file", filePath).Msg("Starting to parse parquet file")

		// Open parquet file
		file, err := os.Open(filePath)
		if err != nil {
			log.Error().Err(err).Str("file", filePath).Msg("Failed to open parquet file")
			continue
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			log.Error().Err(err).Str("file", filePath).Msg("Failed to get file stats")
			continue
		}

		pFile, err := parquet.OpenFile(file, stat.Size())
		if err != nil {
			file.Close()
			log.Error().Err(err).Str("file", filePath).Msg("Failed to open parquet file")
			continue
		}

		log.Debug().
			Str("file", filePath).
			Int("row_groups", len(pFile.RowGroups())).
			Msg("Starting streaming parquet file parsing")

		// Track parsing metrics
		var totalParseTime time.Duration
		var parsedRowCount int

		// Stream through each row group
		for _, rg := range pFile.RowGroups() {
			reader := parquet.NewRowGroupReader(rg)

			for {
				row := make([]parquet.Row, 1)
				n, err := reader.ReadRows(row)

				// Process the row if we successfully read it, even if EOF occurred
				if n > 0 {
					if len(row[0]) < 8 {
						log.Debug().
							Str("file", filePath).
							Int("columns", len(row[0])).
							Msg("Row has insufficient columns, skipping")
						if err == io.EOF {
							break // EOF and no valid row, we're done
						}
						continue // Not enough columns, try again
					}

					// Track individual row parsing time
					parseStart := time.Now()
					byteSize, blockData, err := ParseParquetRow(row[0])
					parseDuration := time.Since(parseStart)

					// Only count parsing time for rows that will be processed
					if blockData.Block.Number.Uint64() >= nextBlockNumber {
						totalParseTime += parseDuration
						parsedRowCount++
					}

					// skip to nextBlockNumber. happens on the first run.
					if blockData.Block.Number.Uint64() < nextBlockNumber {
						continue
					}

					if err != nil {
						log.Panic().Err(err).Msg("Failed to parse block data. Should never happen.")
					}

					// Check if we need to acquire semaphore based on channel state
					acquired, err := acquireMemoryPermit(byteSize)
					if err != nil {
						log.Panic().Err(err).Msg("Failed to acquire memory permit")
					}

					metrics.CommitterBlockDataChannelLength.WithLabelValues(indexerName, chainIdStr).Set(float64(len(blockDataChannel)))
					metrics.CommitterMemoryPermitBytes.WithLabelValues(indexerName, chainIdStr).Set(float64(memorySemaphore.held))

					blockDataChannel <- &BlockDataWithSize{
						BlockData: &blockData,
						ByteSize:  byteSize,
						Acquired:  acquired,
					}
				}

				// Handle EOF and other errors
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Panic().Err(err).Str("file", filePath).Msg("Failed to read row")
				}
				if n == 0 {
					// No rows read in this call, try again
					continue
				}
			}
		}

		log.Debug().
			Str("file", filePath).
			Int("parsed_rows", parsedRowCount).
			Dur("total_parse_time", totalParseTime).
			Dur("avg_parse_time_per_row", totalParseTime/time.Duration(maxInt(parsedRowCount, 1))).
			Msg("Completed streaming parquet file parsing")

		file.Close()

		// Clean up local file
		if err := os.Remove(filePath); err != nil {
			log.Warn().
				Err(err).
				Str("file", filePath).
				Msg("Failed to clean up local file")
		} else {
			log.Debug().Str("file", filePath).Msg("Cleaned up local file")
		}
	}

	return nil
}

// Helper function to get maximum of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
