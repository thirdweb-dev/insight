package libs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
)

type DeployS3CommitterRequest struct {
	ZeetDeploymentId string `json:"zeetDeploymentId"`
}

func DisableIndexerMaybeStartCommitter() {
	makeS3CommitterRequest("deploy-s3-committer")
}

func RightsizeS3Committer() {
	makeS3CommitterRequest("rightsize-s3-committer")
}

// makeS3CommitterRequest is a common function to make HTTP requests to the insight service
func makeS3CommitterRequest(endpoint string) {
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
	url := fmt.Sprintf("%s/service/chains/%s/%s", serviceURL, ChainIdStr, endpoint)
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
		Str("endpoint", endpoint).
		Str("zeetDeploymentId", zeetDeploymentId).
		Msgf("Sending %s request", endpoint)

	resp, err := client.Do(req)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to send %s request", endpoint)
		return
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Info().
			Int("statusCode", resp.StatusCode).
			Msgf("Successfully sent %s request", endpoint)
	} else {
		log.Error().
			Int("statusCode", resp.StatusCode).
			Msgf("%s request failed", endpoint)
	}
}
