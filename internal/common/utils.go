package common

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
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

var allowedFunctions = map[string]struct{}{
	"sum":                  {},
	"count":                {},
	"countdistinct":        {},
	"avg":                  {},
	"max":                  {},
	"min":                  {},
	"reinterpretasuint256": {},
	"reverse":              {},
	"unhex":                {},
	"substring":            {},
	"length":               {},
	"touint256":            {},
	"if":                   {},
	"tostartofmonth":       {},
	"tostartofday":         {},
	"tostartofhour":        {},
	"tostartofminute":      {},
	"todate":               {},
	"todatetime":           {},
	"concat":               {},
	"in":                   {},
	"and":                  {},
	"or":                   {},
}

var disallowedPatterns = []string{
	`(?i)\b(INSERT|DELETE|UPDATE|DROP|CREATE|ALTER|TRUNCATE|EXEC|;|--)`,
}

// ValidateQuery checks the query for disallowed patterns and ensures only allowed functions are used.
func ValidateQuery(query string) error {
	// Check for disallowed patterns
	for _, pattern := range disallowedPatterns {
		matched, err := regexp.MatchString(pattern, query)
		if err != nil {
			return fmt.Errorf("error checking disallowed patterns: %v", err)
		}
		if matched {
			return fmt.Errorf("query contains disallowed keywords or patterns")
		}
	}

	// Ensure the query is a SELECT statement
	trimmedQuery := strings.TrimSpace(strings.ToUpper(query))
	if !strings.HasPrefix(trimmedQuery, "SELECT") {
		return fmt.Errorf("only SELECT queries are allowed")
	}

	// Extract function names and validate them
	// Use a more specific pattern to avoid matching SQL keywords
	functionPattern := regexp.MustCompile(`(?i)(\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*\(`)
	matches := functionPattern.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		funcName := match[1]

		// Skip common SQL keywords that might be matched
		sqlKeywords := map[string]bool{
			"FROM": true, "WHERE": true, "AND": true, "OR": true, "ORDER": true,
			"GROUP": true, "HAVING": true, "LIMIT": true, "OFFSET": true, "AS": true,
			"IN": true, "NOT": true, "IS": true, "NULL": true, "TRUE": true, "FALSE": true,
			"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true, "DROP": true,
			"CREATE": true, "ALTER": true, "TABLE": true, "INDEX": true, "VIEW": true,
			"UNION": true, "ALL": true, "DISTINCT": true, "CASE": true, "WHEN": true,
			"THEN": true, "ELSE": true, "END": true, "JOIN": true, "LEFT": true, "RIGHT": true,
			"INNER": true, "OUTER": true, "ON": true, "USING": true, "WITH": true,
			"RECURSIVE": true, "CTE": true, "EXISTS": true, "BETWEEN": true, "LIKE": true,
			"ILIKE": true, "SIMILAR": true, "REGEXP": true, "MATCH": true, "AGAINST": true,
			"FULLTEXT": true, "BOOLEAN": true, "MODE": true, "QUERY": true, "EXPANSION": true,
		}

		if sqlKeywords[strings.ToUpper(funcName)] {
			continue
		}

		// Check if it's actually a function call (not just a word followed by parenthesis)
		// Look for patterns that indicate it's not a function call
		funcNameUpper := strings.ToUpper(funcName)
		if funcNameUpper == "BY" || funcNameUpper == "ASC" || funcNameUpper == "DESC" {
			continue
		}

		if _, ok := allowedFunctions[strings.ToLower(funcName)]; !ok {
			return fmt.Errorf("function '%s' is not allowed", funcName)
		}
	}

	return nil
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
