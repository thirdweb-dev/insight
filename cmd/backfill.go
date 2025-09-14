package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/backfill"
)

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "run backfill",
	Long:  "backfill data from old clickhouse + rpc to s3. if cannot get all blocks for a range, it will panic",
	Run:   RunBackfill,
}

func RunBackfill(cmd *cobra.Command, args []string) {
	fmt.Println("running backfill")
	backfill.Init()
	backfill.RunBackfill()
}
