package backfill

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
)

type DeployS3CommitterRequest struct {
	ZeetDeploymentId string `json:"zeetDeploymentId"`
}

func DisableIndexerMaybeStartCommitter() {
	serviceURL := config.Cfg.InsightServiceUrl
	apiKey := config.Cfg.InsightServiceApiKey
	zeetDeploymentId := config.Cfg.ZeetDeploymentId

	// Prepare request payload
	requestBody := DeployS3CommitterRequest{
		ZeetDeploymentId: zeetDeploymentId,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal request body")
		return
	}

	// Create HTTP request
	url := fmt.Sprintf("%s/service/chains/%s/deploy-s3-committer", serviceURL, libs.ChainIdStr)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Error().Err(err).Msg("Failed to create HTTP request")
		return
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-service-api-key", apiKey)

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Send request
	log.Info().
		Str("url", url).
		Str("zeetDeploymentId", zeetDeploymentId).
		Msg("Sending deploy-s3-committer request to disable indexer")

	resp, err := client.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send HTTP request")
		return
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Info().
			Int("statusCode", resp.StatusCode).
			Msg("Successfully sent deploy-s3-committer request. Indexer disabled")
	} else {
		log.Error().
			Int("statusCode", resp.StatusCode).
			Msg("Deploy-s3-committer request failed. Could not disable indexer")
	}
}
