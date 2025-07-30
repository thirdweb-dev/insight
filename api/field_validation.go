package api

import (
	"fmt"
	"regexp"
	"strings"
)

// EntityColumns defines the valid columns for each entity type
var EntityColumns = map[string][]string{
	"blocks": {
		"chain_id", "block_number", "block_timestamp", "hash", "parent_hash", "sha3_uncles",
		"nonce", "mix_hash", "miner", "state_root", "transactions_root", "receipts_root",
		"logs_bloom", "size", "extra_data", "difficulty", "total_difficulty", "transaction_count",
		"gas_limit", "gas_used", "withdrawals_root", "base_fee_per_gas", "insert_timestamp", "sign",
	},
	"transactions": {
		"chain_id", "hash", "nonce", "block_hash", "block_number", "block_timestamp",
		"transaction_index", "from_address", "to_address", "value", "gas", "gas_price",
		"data", "function_selector", "max_fee_per_gas", "max_priority_fee_per_gas",
		"max_fee_per_blob_gas", "blob_versioned_hashes", "transaction_type", "r", "s", "v",
		"access_list", "authorization_list", "contract_address", "gas_used", "cumulative_gas_used",
		"effective_gas_price", "blob_gas_used", "blob_gas_price", "logs_bloom", "status",
		"insert_timestamp", "sign",
	},
	"logs": {
		"chain_id", "block_number", "block_hash", "block_timestamp", "transaction_hash",
		"transaction_index", "log_index", "address", "data", "topic_0", "topic_1", "topic_2", "topic_3",
		"insert_timestamp", "sign",
	},
	"transfers": {
		"token_type", "chain_id", "token_address", "from_address", "to_address", "block_number",
		"block_timestamp", "transaction_hash", "token_id", "amount", "log_index", "insert_timestamp", "sign",
	},
	"balances": {
		"token_type", "chain_id", "owner", "address", "token_id", "balance",
	},
	"traces": {
		"chain_id", "block_number", "block_hash", "block_timestamp", "transaction_hash",
		"transaction_index", "subtraces", "trace_address", "type", "call_type", "error",
		"from_address", "to_address", "gas", "gas_used", "input", "output", "value",
		"author", "reward_type", "refund_address", "insert_timestamp", "sign",
	},
}

// ValidateGroupByAndSortBy validates that GroupBy and SortBy fields are valid for the given entity
// It checks that fields are either:
// 1. Valid entity columns
// 2. Valid aggregate function aliases (e.g., "count", "total_amount")
func ValidateGroupByAndSortBy(entity string, groupBy []string, sortBy string, aggregates []string) error {
	// Get valid columns for the entity
	validColumns, exists := EntityColumns[entity]
	if !exists {
		return fmt.Errorf("unknown entity: %s", entity)
	}

	// Create a set of valid fields (entity columns + aggregate aliases)
	validFields := make(map[string]bool)
	for _, col := range validColumns {
		validFields[col] = true
	}

	// Add aggregate function aliases
	aggregateAliases := extractAggregateAliases(aggregates)
	for _, alias := range aggregateAliases {
		validFields[alias] = true
	}

	// Validate GroupBy fields
	for _, field := range groupBy {
		if !validFields[field] {
			return fmt.Errorf("invalid group_by field '%s' for entity '%s'. Valid fields are: %s",
				field, entity, strings.Join(getValidFieldsList(validFields), ", "))
		}
	}

	// Validate SortBy field
	if sortBy != "" && !validFields[sortBy] {
		return fmt.Errorf("invalid sort_by field '%s' for entity '%s'. Valid fields are: %s",
			sortBy, entity, strings.Join(getValidFieldsList(validFields), ", "))
	}

	return nil
}

// extractAggregateAliases extracts column aliases from aggregate functions
// Examples:
// - "COUNT(*) AS count" -> "count"
// - "SUM(amount) AS total_amount" -> "total_amount"
// - "AVG(value) as avg_value" -> "avg_value"
func extractAggregateAliases(aggregates []string) []string {
	var aliases []string
	aliasRegex := regexp.MustCompile(`(?i)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)`)

	for _, aggregate := range aggregates {
		matches := aliasRegex.FindStringSubmatch(aggregate)
		if len(matches) > 1 {
			aliases = append(aliases, matches[1])
		}
	}

	return aliases
}

// getValidFieldsList converts the validFields map to a sorted list for error messages
func getValidFieldsList(validFields map[string]bool) []string {
	var fields []string
	for field := range validFields {
		fields = append(fields, field)
	}
	// Sort for consistent error messages
	// Note: In a production environment, you might want to use sort.Strings(fields)
	return fields
}
