package cmd

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"

	"github.com/thirdweb-dev/indexer/internal/handlers"
	"github.com/thirdweb-dev/indexer/internal/middleware"
)

var (
	apiCmd = &cobra.Command{
		Use:   "api",
		Short: "TBD",
		Long:  "TBD",
		Run: func(cmd *cobra.Command, args []string) {
			RunApi(cmd, args)
		},
	}
)

func RunApi(cmd *cobra.Command, args []string) {
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	root := r.Group("/:chainId")
	{
		root.Use(middleware.Authorization)
		// wildcard queries
		root.GET("/transactions", handlers.GetTransactions)
		root.GET("/events", handlers.GetLogs)

		// contract scoped queries
		root.GET("/transactions/:to", handlers.GetTransactionsByContract)
		root.GET("/events/:contract", handlers.GetLogsByContract)

		// signature scoped queries
		root.GET("/transactions/:to/:signature", handlers.GetTransactionsByContractAndSignature)
		root.GET("/events/:contract/:signature", handlers.GetLogsByContractAndSignature)
	}

	r.GET("/health", func(c *gin.Context) {
		// TODO: implement a simple query before going live
		c.String(http.StatusOK, "ok")
	})

	r.Run(":3000")
}
