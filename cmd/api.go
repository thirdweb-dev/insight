package cmd

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/swag"

	"github.com/thirdweb-dev/indexer/internal/handlers"
	"github.com/thirdweb-dev/indexer/internal/middleware"

	// Import the generated Swagger docs
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/docs"
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

// @title Thirdweb Insight
// @version v0.0.1-beta
// @description API for querying blockchain transactions and events
// @license.name Apache 2.0
// @license.url https://github.com/thirdweb-dev/indexer/blob/main/LICENSE
// @BasePath /
// @Security BasicAuth
// @securityDefinitions.basic BasicAuth
func RunApi(cmd *cobra.Command, args []string) {
	docs.SwaggerInfo.Host = config.Cfg.API.Host

	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	// Add Swagger route
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	// Add Swagger JSON endpoint
	r.GET("/openapi.json", func(c *gin.Context) {
		doc, err := swag.ReadDoc()
		if err != nil {
			log.Error().Err(err).Msg("Failed to read Swagger documentation")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to provide Swagger documentation"})
			return
		}
		c.Header("Content-Type", "application/json")
		c.String(http.StatusOK, doc)
	})

	root := r.Group("/:chainId")
	{
		root.Use(middleware.Authorization)
		root.Use(middleware.Cors)
		// wildcard queries
		root.GET("/transactions", handlers.GetTransactions)
		root.GET("/events", handlers.GetLogs)

		// contract scoped queries
		root.GET("/transactions/:to", handlers.GetTransactionsByContract)
		root.GET("/events/:contract", handlers.GetLogsByContract)

		// signature scoped queries
		root.GET("/transactions/:to/:signature", handlers.GetTransactionsByContractAndSignature)
		root.GET("/events/:contract/:signature", handlers.GetLogsByContractAndSignature)

		// blocks table queries
		root.GET("/blocks", handlers.GetBlocks)
	}

	r.GET("/health", func(c *gin.Context) {
		// TODO: implement a simple query before going live
		c.String(http.StatusOK, "ok")
	})

	r.Run(":3000")
}
