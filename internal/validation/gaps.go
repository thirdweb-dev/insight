package validation

import (
	"context"
	"math/big"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

func FindAndFixGaps(rpc rpc.IRPCClient, s storage.IStorage, conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) error {
	missingBlockNumbers, err := findMissingBlocksInRange(conn, chainId, startBlock, endBlock)
	if err != nil {
		return err
	}
	if len(missingBlockNumbers) == 0 {
		log.Debug().Msg("No missing blocks found")
		return nil
	}
	log.Debug().Msgf("Found %d missing blocks: %v", len(missingBlockNumbers), missingBlockNumbers)

	// query missing blocks
	poller := orchestrator.NewBoundlessPoller(rpc, s)
	poller.Poll(context.Background(), missingBlockNumbers)
	log.Debug().Msg("Missing blocks polled")

	blocksData, err := s.StagingStorage.GetStagingData(storage.QueryFilter{BlockNumbers: missingBlockNumbers, ChainId: rpc.GetChainID()})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get staging data")
	}
	if len(blocksData) == 0 {
		log.Fatal().Msg("Failed to get staging data")
	}

	err = s.MainStorage.InsertBlockData(blocksData)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to commit blocks: %v", blocksData)
	}

	if err := s.StagingStorage.DeleteStagingData(blocksData); err != nil {
		log.Error().Err(err).Msgf("Failed to delete staging data: %v", blocksData)
	}
	return nil
}

func findMissingBlocksInRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]*big.Int, error) {
	query := `
		WITH sequence AS (
			SELECT 
					{startBlock:UInt256} + number AS expected_block_number
			FROM 
					numbers(toUInt64({endBlock:UInt256} - {startBlock:UInt256} + 1))
		),
		existing_blocks AS (
				SELECT DISTINCT 
						block_number
				FROM 
						blocks FINAL
				WHERE 
						chain_id = {chainId:UInt256} 
						AND block_number >= {startBlock:UInt256}
						AND block_number <= {endBlock:UInt256}
		)
		SELECT 
				s.expected_block_number AS missing_block_number
		FROM 
				sequence s
		LEFT JOIN 
				existing_blocks e ON s.expected_block_number = e.block_number
		WHERE 
				e.block_number = 0
		ORDER BY 
				missing_block_number
	`
	rows, err := conn.Query(context.Background(), query, clickhouse.Named("chainId", chainId.String()), clickhouse.Named("startBlock", startBlock.String()), clickhouse.Named("endBlock", endBlock.String()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blockNumbers := make([]*big.Int, 0)
	for rows.Next() {
		var blockNumber *big.Int
		err := rows.Scan(&blockNumber)
		if err != nil {
			return nil, err
		}
		blockNumbers = append(blockNumbers, blockNumber)
	}
	return blockNumbers, nil
}
