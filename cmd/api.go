package cmd

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/swag"

	"github.com/thirdweb-dev/indexer/internal/handlers"
	"github.com/thirdweb-dev/indexer/internal/middleware"
	"github.com/thirdweb-dev/indexer/internal/storage"

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	r := gin.New()
	r.Use(middleware.Logger())
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
		root.GET("/wallet-transactions/:wallet_address", handlers.GetWalletTransactions)

		// contract scoped queries
		root.GET("/transactions/:to", handlers.GetTransactionsByContract)
		root.GET("/events/:contract", handlers.GetLogsByContract)

		// signature scoped queries
		root.GET("/transactions/:to/:signature", handlers.GetTransactionsByContractAndSignature)
		root.GET("/events/:contract/:signature", handlers.GetLogsByContractAndSignature)

		// blocks table queries
		root.GET("/blocks", handlers.GetBlocks)

		// token balance queries
		root.GET("/balances/:owner/:type", handlers.GetTokenBalancesByType)

		root.GET("/balances/:owner", handlers.GetTokenBalancesByType)

		// token holder queries
		root.GET("/holders/:address", handlers.GetTokenHoldersByType)

		// token transfers queries
		root.GET("/transfers", handlers.GetTokenTransfers)
		// token ID queries
		root.GET("/tokens/:address", handlers.GetTokenIdsByType)

		// search
		root.GET("/search/:input", handlers.Search)
	}

	r.GET("/health", func(c *gin.Context) {
		// TODO: implement a simple query before going live
		c.String(http.StatusOK, "ok")
	})

	// Database health check endpoint
	r.GET("/health/db", func(c *gin.Context) {
		health := checkDatabaseHealth()
		c.JSON(http.StatusOK, health)
	})

	srv := &http.Server{
		Addr:    ":3000",
		Handler: r,
	}

	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("listen: %s\n")
		}
	}()

	// Listen for the interrupt signal.
	<-ctx.Done()

	// Restore default behavior on the interrupt signal and notify user of shutdown.
	stop()
	log.Info().Msg("shutting down API gracefully")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal().Err(err).Msg("API server forced to shutdown")
	}

	log.Info().Msg("API server exiting")
}

type DatabaseHealth struct {
	PostgreSQL struct {
		Status  string `json:"status"`
		Error   string `json:"error,omitempty"`
		Details string `json:"details,omitempty"`
	} `json:"postgresql"`
	ClickHouse struct {
		Status  string `json:"status"`
		Error   string `json:"error,omitempty"`
		Details string `json:"details,omitempty"`
	} `json:"clickhouse"`
	Overall struct {
		Status string `json:"status"`
		Time   string `json:"time"`
	} `json:"overall"`
}

func checkDatabaseHealth() DatabaseHealth {
	health := DatabaseHealth{}
	overallHealthy := true

	// Check PostgreSQL
	postgresHealthy := checkPostgreSQLHealth(&health.PostgreSQL)
	if !postgresHealthy {
		overallHealthy = false
	}

	// Check ClickHouse
	clickhouseHealthy := checkClickHouseHealth(&health.ClickHouse)
	if !clickhouseHealthy {
		overallHealthy = false
	}

	// Set overall status
	if overallHealthy {
		health.Overall.Status = "healthy"
	} else {
		health.Overall.Status = "unhealthy"
	}
	health.Overall.Time = time.Now().Format(time.RFC3339)

	return health
}

func checkPostgreSQLHealth(postgres *struct {
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
	Details string `json:"details,omitempty"`
}) bool {
	// Try to create a PostgreSQL connector
	postgresConfig := config.Cfg.Storage.Orchestrator.Postgres
	connector, err := storage.NewPostgresConnector(postgresConfig)
	if err != nil {
		postgres.Status = "unhealthy"
		postgres.Error = err.Error()
		postgres.Details = "Failed to create PostgreSQL connector"
		return false
	}
	defer connector.Close()

	// Test the connection with a simple query
	_, err = connector.GetBlockFailures(storage.QueryFilter{})
	if err != nil {
		postgres.Status = "unhealthy"
		postgres.Error = err.Error()
		postgres.Details = "Failed to execute test query"
		return false
	}

	postgres.Status = "healthy"
	postgres.Details = "Connection and query test successful"
	return true
}

func checkClickHouseHealth(clickhouse *struct {
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
	Details string `json:"details,omitempty"`
}) bool {
	// Try to create a ClickHouse connector
	clickhouseConfig := config.Cfg.Storage.Main.Clickhouse
	connector, err := storage.NewClickHouseConnector(clickhouseConfig)
	if err != nil {
		clickhouse.Status = "unhealthy"
		clickhouse.Error = err.Error()
		clickhouse.Details = "Failed to create ClickHouse connector"
		return false
	}

	// Test the connection with a simple query
	err = connector.TestConnection()
	if err != nil {
		clickhouse.Status = "unhealthy"
		clickhouse.Error = err.Error()
		clickhouse.Details = "Failed to execute test query"
		return false
	}

	clickhouse.Status = "healthy"
	clickhouse.Details = "Connection and query test successful"
	return true
}
