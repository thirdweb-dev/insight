package api

import (
	"strings"
	"testing"
)

func TestValidateGroupByAndSortBy(t *testing.T) {
	tests := []struct {
		name       string
		entity     string
		groupBy    []string
		sortBy     string
		aggregates []string
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "valid blocks fields",
			entity:     "blocks",
			groupBy:    []string{"block_number", "hash"},
			sortBy:     "block_timestamp",
			aggregates: nil,
			wantErr:    false,
		},
		{
			name:       "valid transactions fields",
			entity:     "transactions",
			groupBy:    []string{"from_address", "to_address"},
			sortBy:     "value",
			aggregates: nil,
			wantErr:    false,
		},
		{
			name:       "valid logs fields",
			entity:     "logs",
			groupBy:    []string{"address", "topic_0"},
			sortBy:     "block_number",
			aggregates: nil,
			wantErr:    false,
		},
		{
			name:       "valid transfers fields",
			entity:     "transfers",
			groupBy:    []string{"token_address", "from_address"},
			sortBy:     "amount",
			aggregates: nil,
			wantErr:    false,
		},
		{
			name:       "valid balances fields",
			entity:     "balances",
			groupBy:    []string{"owner", "token_id"},
			sortBy:     "balance",
			aggregates: nil,
			wantErr:    false,
		},
		{
			name:       "valid with aggregate aliases",
			entity:     "transactions",
			groupBy:    []string{"from_address"},
			sortBy:     "total_value",
			aggregates: []string{"SUM(value) AS total_value", "COUNT(*) AS count"},
			wantErr:    false,
		},
		{
			name:       "invalid entity",
			entity:     "invalid_entity",
			groupBy:    []string{"field"},
			sortBy:     "field",
			aggregates: nil,
			wantErr:    true,
			errMsg:     "unknown entity: invalid_entity",
		},
		{
			name:       "invalid group_by field",
			entity:     "blocks",
			groupBy:    []string{"invalid_field"},
			sortBy:     "block_number",
			aggregates: nil,
			wantErr:    true,
			errMsg:     "invalid group_by field 'invalid_field' for entity 'blocks'",
		},
		{
			name:       "invalid sort_by field",
			entity:     "transactions",
			groupBy:    []string{"hash"},
			sortBy:     "invalid_field",
			aggregates: nil,
			wantErr:    true,
			errMsg:     "invalid sort_by field 'invalid_field' for entity 'transactions'",
		},
		{
			name:       "invalid aggregate alias",
			entity:     "logs",
			groupBy:    []string{"address"},
			sortBy:     "invalid_alias",
			aggregates: []string{"COUNT(*) AS count"},
			wantErr:    true,
			errMsg:     "invalid sort_by field 'invalid_alias' for entity 'logs'",
		},
		{
			name:       "empty sort_by is valid",
			entity:     "blocks",
			groupBy:    []string{"block_number"},
			sortBy:     "",
			aggregates: nil,
			wantErr:    false,
		},
		{
			name:       "empty group_by is valid",
			entity:     "transactions",
			groupBy:    []string{},
			sortBy:     "hash",
			aggregates: nil,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGroupByAndSortBy(tt.entity, tt.groupBy, tt.sortBy, tt.aggregates)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateGroupByAndSortBy() expected error but got none")
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateGroupByAndSortBy() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateGroupByAndSortBy() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestExtractAggregateAliases(t *testing.T) {
	tests := []struct {
		name       string
		aggregates []string
		want       []string
	}{
		{
			name:       "simple aliases",
			aggregates: []string{"COUNT(*) AS count", "SUM(value) AS total_value"},
			want:       []string{"count", "total_value"},
		},
		{
			name:       "case insensitive AS",
			aggregates: []string{"AVG(amount) as avg_amount", "MAX(price) As max_price"},
			want:       []string{"avg_amount", "max_price"},
		},
		{
			name:       "no aliases",
			aggregates: []string{"COUNT(*)", "SUM(value)"},
			want:       []string{},
		},
		{
			name:       "mixed with and without aliases",
			aggregates: []string{"COUNT(*) AS count", "SUM(value)", "AVG(price) as avg_price"},
			want:       []string{"count", "avg_price"},
		},
		{
			name:       "empty aggregates",
			aggregates: []string{},
			want:       []string{},
		},
		{
			name:       "complex aliases",
			aggregates: []string{"COUNT(DISTINCT address) AS unique_addresses", "SUM(CASE WHEN value > 0 THEN 1 ELSE 0 END) AS positive_transactions"},
			want:       []string{"unique_addresses", "positive_transactions"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractAggregateAliases(tt.aggregates)
			if len(got) != len(tt.want) {
				t.Errorf("extractAggregateAliases() length = %v, want %v", len(got), len(tt.want))
				return
			}
			for i, alias := range got {
				if alias != tt.want[i] {
					t.Errorf("extractAggregateAliases()[%d] = %v, want %v", i, alias, tt.want[i])
				}
			}
		})
	}
}
