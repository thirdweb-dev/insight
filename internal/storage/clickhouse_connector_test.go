package storage

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	config "github.com/thirdweb-dev/indexer/configs"
)

// TestMapClickHouseTypeToGoType tests the mapClickHouseTypeToGoType function
func TestMapClickHouseTypeToGoType(t *testing.T) {
	testCases := []struct {
		dbType       string
		expectedType interface{}
	}{
		// Signed integers
		{"Int8", int8(0)},
		{"Nullable(Int8)", (**int8)(nil)},
		{"Int16", int16(0)},
		{"Nullable(Int16)", (**int16)(nil)},
		{"Int32", int32(0)},
		{"Nullable(Int32)", (**int32)(nil)},
		{"Int64", int64(0)},
		{"Nullable(Int64)", (**int64)(nil)},
		// Unsigned integers
		{"UInt8", uint8(0)},
		{"Nullable(UInt8)", (**uint8)(nil)},
		{"UInt16", uint16(0)},
		{"Nullable(UInt16)", (**uint16)(nil)},
		{"UInt32", uint32(0)},
		{"Nullable(UInt32)", (**uint32)(nil)},
		{"UInt64", uint64(0)},
		{"Nullable(UInt64)", (**uint64)(nil)},
		// Big integers
		{"Int128", big.NewInt(0)},
		{"Nullable(Int128)", (**big.Int)(nil)},
		{"UInt128", big.NewInt(0)},
		{"Nullable(UInt128)", (**big.Int)(nil)},
		{"Int256", big.NewInt(0)},
		{"Nullable(Int256)", (**big.Int)(nil)},
		{"UInt256", big.NewInt(0)},
		{"Nullable(UInt256)", (**big.Int)(nil)},
		// Floating-point numbers
		{"Float32", float32(0)},
		{"Nullable(Float32)", (**float32)(nil)},
		{"Float64", float64(0)},
		{"Nullable(Float64)", (**float64)(nil)},
		// Decimal types
		{"Decimal", big.NewFloat(0)},
		{"Nullable(Decimal)", (**big.Float)(nil)},
		{"Decimal32", big.NewFloat(0)},
		{"Nullable(Decimal32)", (**big.Float)(nil)},
		{"Decimal64", big.NewFloat(0)},
		{"Nullable(Decimal64)", (**big.Float)(nil)},
		{"Decimal128", big.NewFloat(0)},
		{"Nullable(Decimal128)", (**big.Float)(nil)},
		{"Decimal256", big.NewFloat(0)},
		{"Nullable(Decimal256)", (**big.Float)(nil)},
		// String types
		{"String", ""},
		{"Nullable(String)", (**string)(nil)},
		{"FixedString(42)", ""},
		{"Nullable(FixedString(42))", (**string)(nil)},
		{"UUID", ""},
		{"Nullable(UUID)", (**string)(nil)},
		{"IPv4", ""},
		{"Nullable(IPv4)", (**string)(nil)},
		{"IPv6", ""},
		{"Nullable(IPv6)", (**string)(nil)},
		// Date and time types
		{"Date", time.Time{}},
		{"Nullable(Date)", (**time.Time)(nil)},
		{"DateTime", time.Time{}},
		{"Nullable(DateTime)", (**time.Time)(nil)},
		{"DateTime64", time.Time{}},
		{"Nullable(DateTime64)", (**time.Time)(nil)},
		// Enums
		{"Enum8('a' = 1, 'b' = 2)", ""},
		{"Nullable(Enum8('a' = 1, 'b' = 2))", (**string)(nil)},
		{"Enum16('a' = 1, 'b' = 2)", ""},
		{"Nullable(Enum16('a' = 1, 'b' = 2))", (**string)(nil)},
		// Arrays
		{"Array(Int32)", &[]*int64{}},
		{"Array(String)", &[]*string{}},
		{"Array(Float64)", &[]*float64{}},
		// LowCardinality
		{"LowCardinality(String)", ""},
		{"LowCardinality(Nullable(String))", (**string)(nil)},
		// Unknown type
		{"UnknownType", new(interface{})},
		{"Nullable(UnknownType)", new(interface{})},
	}

	for _, tc := range testCases {
		t.Run(tc.dbType, func(t *testing.T) {
			result := mapClickHouseTypeToGoType(tc.dbType)

			expectedType := reflect.TypeOf(tc.expectedType)
			resultType := reflect.TypeOf(result)

			// Handle pointers
			if expectedType.Kind() == reflect.Ptr {
				if resultType.Kind() != reflect.Ptr {
					t.Errorf("Expected pointer type for dbType %s, got %s", tc.dbType, resultType.Kind())
					return
				}
				expectedElemType := expectedType.Elem()
				resultElemType := resultType.Elem()
				if expectedElemType.Kind() == reflect.Ptr {
					// Expected pointer to pointer
					if resultElemType.Kind() != reflect.Ptr {
						t.Errorf("Expected pointer to pointer for dbType %s, got %s", tc.dbType, resultElemType.Kind())
						return
					}
					expectedElemType = expectedElemType.Elem()
					resultElemType = resultElemType.Elem()
				}
				if expectedElemType != resultElemType {
					t.Errorf("Type mismatch for dbType %s: expected %s, got %s", tc.dbType, expectedElemType, resultElemType)
				}
			} else {
				// Non-pointer types
				if resultType.Kind() != reflect.Ptr {
					t.Errorf("Expected pointer type for dbType %s, got %s", tc.dbType, resultType.Kind())
					return
				}
				resultElemType := resultType.Elem()
				if expectedType != resultElemType {
					t.Errorf("Type mismatch for dbType %s: expected %s, got %s", tc.dbType, expectedType, resultElemType)
				}
			}
		})
	}
}

// TestUnionQueryLogic tests the UNION query logic for wallet addresses in transactions
func TestUnionQueryLogic(t *testing.T) {
	// Create a mock config with valid connection details
	cfg := &config.ClickhouseConfig{
		Database:     "default",
		Host:         "localhost",
		Port:         9000,
		Username:     "default",
		Password:     "",
		MaxQueryTime: 30,
	}

	// Create connector
	connector, err := NewClickHouseConnector(cfg)
	if err != nil {
		// Skip test if we can't connect to ClickHouse (likely in CI environment)
		t.Skipf("Skipping test - cannot connect to ClickHouse: %v", err)
	}

	// Test case 1: Standard query without wallet address (should not use UNION)
	t.Run("Standard query without wallet address", func(t *testing.T) {
		qf := QueryFilter{
			ChainId:   big.NewInt(8453),
			Limit:     5,
			SortBy:    "block_number",
			SortOrder: "DESC",
		}

		query := connector.TestQueryGeneration("transactions", "*", qf)

		// Should not contain UNION ALL
		if strings.Contains(query, "UNION ALL") {
			t.Errorf("Standard query should not contain UNION ALL: %s", query)
		}

		// Should contain standard WHERE clause
		if !strings.Contains(query, "WHERE") {
			t.Errorf("Query should contain WHERE clause: %s", query)
		}
	})

	// Test case 2: UNION query with wallet address
	t.Run("UNION query with wallet address", func(t *testing.T) {
		qf := QueryFilter{
			ChainId:       big.NewInt(8453),
			WalletAddress: "0x0b230949b38fa651aefffcfa5e664554df8ae900",
			Limit:         5,
			SortBy:        "block_number",
			SortOrder:     "DESC",
		}

		query := connector.TestQueryGeneration("transactions", "*", qf)

		// Should contain UNION ALL
		if !strings.Contains(query, "UNION ALL") {
			t.Errorf("Query should contain UNION ALL: %s", query)
		}

		// Should contain from_address and to_address conditions
		if !strings.Contains(query, "from_address = '0x0b230949b38fa651aefffcfa5e664554df8ae900'") {
			t.Errorf("Query should contain from_address condition: %s", query)
		}

		if !strings.Contains(query, "to_address = '0x0b230949b38fa651aefffcfa5e664554df8ae900'") {
			t.Errorf("Query should contain to_address condition: %s", query)
		}

		// Should have proper ORDER BY and LIMIT at the end
		if !strings.Contains(query, "ORDER BY block_number DESC") {
			t.Errorf("Query should contain ORDER BY clause: %s", query)
		}

		if !strings.Contains(query, "LIMIT 5") {
			t.Errorf("Query should contain LIMIT clause: %s", query)
		}

		// Should have SETTINGS at the very end
		if !strings.Contains(query, "SETTINGS max_execution_time = 30") {
			t.Errorf("Query should contain SETTINGS clause: %s", query)
		}
	})

	// Test case 3: Standard query for logs table (should not use UNION)
	t.Run("Standard query for logs table", func(t *testing.T) {
		qf := QueryFilter{
			ChainId:       big.NewInt(8453),
			WalletAddress: "0x0b230949b38fa651aefffcfa5e664554df8ae900",
			Limit:         5,
			SortBy:        "block_number",
			SortOrder:     "DESC",
		}

		query := connector.TestQueryGeneration("logs", "*", qf)

		// Should not contain UNION ALL (logs table doesn't use UNION)
		if strings.Contains(query, "UNION ALL") {
			t.Errorf("Logs query should not contain UNION ALL: %s", query)
		}

		// Logs table doesn't have wallet address clauses since it doesn't have from_address/to_address fields
		// So it should just have the basic WHERE clause without wallet address
		if !strings.Contains(query, "WHERE") {
			t.Errorf("Logs query should contain WHERE clause: %s", query)
		}

		// Should not contain wallet address conditions since logs don't have those fields
		if strings.Contains(query, "from_address") || strings.Contains(query, "to_address") {
			t.Errorf("Logs query should not contain address conditions: %s", query)
		}
	})

	// Test case 4: UNION query with GROUP BY
	t.Run("UNION query with GROUP BY", func(t *testing.T) {
		qf := QueryFilter{
			ChainId:       big.NewInt(8453),
			WalletAddress: "0x0b230949b38fa651aefffcfa5e664554df8ae900",
			GroupBy:       []string{"block_number"},
			Limit:         5,
			SortBy:        "block_number",
			SortOrder:     "DESC",
		}

		query := connector.TestQueryGeneration("transactions", "block_number, COUNT(*) as count", qf)

		// Should contain UNION ALL
		if !strings.Contains(query, "UNION ALL") {
			t.Errorf("Query should contain UNION ALL: %s", query)
		}

		// Should contain GROUP BY wrapped in subquery
		if !strings.Contains(query, "SELECT * FROM (") {
			t.Errorf("Query should wrap UNION in subquery for GROUP BY: %s", query)
		}

		if !strings.Contains(query, "GROUP BY block_number") {
			t.Errorf("Query should contain GROUP BY clause: %s", query)
		}
	})
}
