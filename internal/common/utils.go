package common

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"unicode"
)

func BigIntSliceToChunks(values []*big.Int, chunkSize int) [][]*big.Int {
	if chunkSize >= len(values) || chunkSize <= 0 {
		return [][]*big.Int{values}
	}
	var chunks [][]*big.Int
	for i := 0; i < len(values); i += chunkSize {
		end := i + chunkSize
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[i:end])
	}
	return chunks
}

// StripPayload removes parameter names, 'indexed' keywords,
// and extra whitespaces from a Solidity function or event signature.
func StripPayload(signature string) string {
	// Find the index of the first '(' and last ')'
	start := strings.Index(signature, "(")
	end := strings.LastIndex(signature, ")")
	if start == -1 || end == -1 || end <= start {
		// Return the original signature if it doesn't match the expected pattern
		return signature
	}

	functionName := strings.TrimSpace(signature[:start])
	paramsStr := signature[start+1 : end]

	// Parse parameters
	strippedParams := parseParameters(paramsStr)

	// Reconstruct the cleaned-up signature
	strippedSignature := fmt.Sprintf("%s(%s)", functionName, strings.Join(strippedParams, ","))
	return strippedSignature
}

// parseParameters parses the parameter string and returns a slice of cleaned-up parameter types
func parseParameters(paramsStr string) []string {
	var params []string
	var currentParam strings.Builder
	bracketDepth := 0
	var inType bool // Indicates if we are currently parsing a type

	runes := []rune(paramsStr)
	i := 0
	for i < len(runes) {
		char := runes[i]
		switch char {
		case '(', '[', '{':
			bracketDepth++
			inType = true
			currentParam.WriteRune(char)
			i++
		case ')', ']', '}':
			bracketDepth--
			currentParam.WriteRune(char)
			i++
		case ',':
			if bracketDepth == 0 {
				// End of current parameter
				paramType := cleanType(currentParam.String())
				if paramType != "" {
					params = append(params, paramType)
				}
				currentParam.Reset()
				inType = false
				i++
			} else {
				currentParam.WriteRune(char)
				i++
			}
		case ' ':
			if inType {
				currentParam.WriteRune(char)
			}
			i++
		default:
			// Check if the word is a keyword to ignore
			if unicode.IsLetter(char) {
				wordStart := i
				for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i])) {
					i++
				}
				word := string(runes[wordStart:i])

				// Ignore 'indexed' and parameter names
				if isType(word) {
					inType = true
					currentParam.WriteString(word)
				} else if word == "indexed" {
					// Skip 'indexed'
					inType = false
				} else {
					// Ignore parameter names
					if inType {
						// If we are in the middle of parsing a type and encounter a parameter name, skip it
						inType = false
					}
				}
			} else {
				if inType {
					currentParam.WriteRune(char)
				}
				i++
			}
		}
	}

	// Add the last parameter
	if currentParam.Len() > 0 {
		paramType := cleanType(currentParam.String())
		if paramType != "" {
			params = append(params, paramType)
		}
	}

	return params
}

// cleanType cleans up a parameter type string by removing extra spaces and 'tuple' keyword
func cleanType(param string) string {
	// Remove 'tuple' keyword
	param = strings.ReplaceAll(param, "tuple", "")
	// Remove 'indexed' keyword
	param = strings.ReplaceAll(param, "indexed", "")
	// Remove any parameter names (already handled in parsing)
	param = strings.TrimSpace(param)
	// Remove extra whitespaces
	param = strings.Join(strings.Fields(param), "")
	return param
}

// isType checks if a word is a Solidity type
func isType(word string) bool {
	if strings.HasPrefix(word, "uint") || strings.HasPrefix(word, "int") {
		return true
	}
	types := map[string]bool{
		"address":  true,
		"bool":     true,
		"string":   true,
		"bytes":    true,
		"fixed":    true,
		"ufixed":   true,
		"function": true,
		// Add other types as needed
	}

	return types[word]
}

var allowedFunctions = map[string]struct{}{
	"sum":                  {},
	"count":                {},
	"reinterpretAsUInt256": {},
	"reverse":              {},
	"unhex":                {},
	"substring":            {},
	"length":               {},
	"toUInt256":            {},
	"if":                   {},
	"toStartOfDay":         {},
	"toDate":               {},
	"concat":               {},
}

var disallowedPatterns = []string{
	`(?i)\b(UNION|INSERT|DELETE|UPDATE|DROP|CREATE|ALTER|TRUNCATE|EXEC|;|--)`,
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
	functionPattern := regexp.MustCompile(`(?i)(\b\w+\b)\s*\(`)
	matches := functionPattern.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		funcName := match[1]
		if _, ok := allowedFunctions[funcName]; !ok {
			return fmt.Errorf("function '%s' is not allowed", funcName)
		}
	}

	return nil
}
