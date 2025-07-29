package common

import (
	"math/big"
)

func SliceToChunks[T any](values []T, chunkSize int) [][]T {
	if chunkSize >= len(values) || chunkSize <= 0 {
		return [][]T{values}
	}
	var chunks [][]T
	for i := 0; i < len(values); i += chunkSize {
		end := i + chunkSize
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[i:end])
	}
	return chunks
}

func ConvertBigNumbersToString(data interface{}) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		for key, value := range v {
			v[key] = ConvertBigNumbersToString(value)
		}
		return v
	case []interface{}:
		for i, value := range v {
			v[i] = ConvertBigNumbersToString(value)
		}
		return v
	case []*big.Int:
		result := make([]string, len(v))
		for i, num := range v {
			if num == nil {
				result[i] = "0"
			} else {
				result[i] = num.String()
			}
		}
		return result
	case *big.Int:
		if v == nil {
			return "0"
		}
		return v.String()
	default:
		return v
	}
}
