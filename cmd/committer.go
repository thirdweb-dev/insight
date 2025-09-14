package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/committer"
)

var committerCmd = &cobra.Command{
	Use:   "committer",
	Short: "run committer",
	Long:  "published data from s3 to kafka. if block is not found in s3, it will panic",
	Run:   RunCommitter,
}

func RunCommitter(cmd *cobra.Command, args []string) {
	fmt.Println("running committer")
	committer.Init()
	committer.Commit()
}
