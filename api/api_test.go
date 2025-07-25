package api

import (
	"strconv"
	"testing"
	"time"

	config "github.com/thirdweb-dev/indexer/configs"
)

func TestApplyDefaultTimeRange(t *testing.T) {
	// Save original config
	originalConfig := config.Cfg

	// Test with different default values
	testCases := []struct {
		name           string
		defaultDays    int
		inputParams    map[string]string
		expectedParams map[string]string
		shouldApply    bool
	}{
		{
			name:        "no timestamp filters - should apply defaults (7 days)",
			defaultDays: 7,
			inputParams: map[string]string{"other_filter": "value"},
			shouldApply: true,
		},
		{
			name:        "no timestamp filters - should apply defaults (3 days)",
			defaultDays: 3,
			inputParams: map[string]string{"other_filter": "value"},
			shouldApply: true,
		},
		{
			name:        "has block_timestamp_gte - should not apply defaults",
			defaultDays: 7,
			inputParams: map[string]string{"block_timestamp_gte": "1234567890"},
			shouldApply: false,
		},
		{
			name:        "has block_timestamp_lte - should not apply defaults",
			defaultDays: 7,
			inputParams: map[string]string{"block_timestamp_lte": "1234567890"},
			shouldApply: false,
		},
		{
			name:        "has other block_timestamp filter - should not apply defaults",
			defaultDays: 7,
			inputParams: map[string]string{"block_timestamp_other": "1234567890"},
			shouldApply: false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			// Set test config
			config.Cfg.API.DefaultTimeRangeDays = tt.defaultDays

			// Create a copy of input params
			params := make(map[string]string)
			for k, v := range tt.inputParams {
				params[k] = v
			}

			// Apply default time range
			ApplyDefaultTimeRange(params)

			// Check if defaults were applied
			hadGteBefore := false
			hadLteBefore := false
			hasGteAfter := false
			hasLteAfter := false

			// Check what was in input
			for key := range tt.inputParams {
				if key == "block_timestamp_gte" {
					hadGteBefore = true
				}
				if key == "block_timestamp_lte" {
					hadLteBefore = true
				}
			}

			// Check what's in output
			for key := range params {
				if key == "block_timestamp_gte" {
					hasGteAfter = true
				}
				if key == "block_timestamp_lte" {
					hasLteAfter = true
				}
			}

			if tt.shouldApply {
				// Should have added both gte and lte
				if !hasGteAfter || !hasLteAfter {
					t.Errorf("Expected default time range to be applied, but gte=%v, lte=%v", hasGteAfter, hasLteAfter)
				}
			} else {
				// Should not have added any new defaults
				if (!hadGteBefore && hasGteAfter) || (!hadLteBefore && hasLteAfter) {
					t.Errorf("Expected no new defaults to be applied, but added gte=%v, lte=%v",
						!hadGteBefore && hasGteAfter, !hadLteBefore && hasLteAfter)
				}
			}

			// Verify the time range is reasonable
			if hasGteAfter && hasLteAfter {
				now := time.Now()
				maxDaysAgo := now.AddDate(0, 0, -(tt.defaultDays + 1))

				// Parse the timestamps
				if gteStr, exists := params["block_timestamp_gte"]; exists {
					if gteUnix, err := strconv.ParseInt(gteStr, 10, 64); err == nil {
						gte := time.Unix(gteUnix, 0)
						if gte.Before(maxDaysAgo) {
							t.Errorf("block_timestamp_gte is too old: %v (expected within %d days)", gte, tt.defaultDays)
						}
					}
				}

				if lteStr, exists := params["block_timestamp_lte"]; exists {
					if lteUnix, err := strconv.ParseInt(lteStr, 10, 64); err == nil {
						lte := time.Unix(lteUnix, 0)
						if lte.After(now) {
							t.Errorf("block_timestamp_lte is in the future: %v", lte)
						}
					}
				}
			}
		})
	}

	// Restore original config
	config.Cfg = originalConfig
}
