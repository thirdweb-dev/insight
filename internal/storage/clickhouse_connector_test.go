package storage

import (
	"math/big"
	"reflect"
	"testing"
	"time"
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
