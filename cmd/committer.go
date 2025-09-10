package cmd

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/committer"
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

var committerCmd = &cobra.Command{
	Use:   "committer",
	Short: "run committer",
	Long:  "published data from s3 to kafka. if block is not found in s3, it will panic",
	Run:   RunCommitter,
}

func RunCommitter(cmd *cobra.Command, args []string) {
	fmt.Println("running committer")

	rpc, err := rpc.Initialize()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RPC")
	}
	chainId := rpc.GetChainID()

	committer.Commit(chainId)
}
