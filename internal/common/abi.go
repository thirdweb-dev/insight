package common

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

func ConstructFunctionABI(signature string) (*abi.Method, error) {
	regex := regexp.MustCompile(`^(\w+)\((.*)\)$`)
	matches := regex.FindStringSubmatch(strings.TrimSpace(signature))
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid event signature format")
	}

	functionName := matches[1]
	params := matches[2]

	inputs, err := parseParamsToAbiArguments(params)
	if err != nil {
		return nil, fmt.Errorf("failed to parse params to abi arguments '%s': %v", params, err)
	}

	function := abi.NewMethod(functionName, functionName, abi.Function, "", false, false, inputs, nil)

	return &function, nil
}

func parseParamsToAbiArguments(params string) (abi.Arguments, error) {
	paramList := splitParams(strings.TrimSpace(params))
	var inputs abi.Arguments
	for idx, param := range paramList {
		arg, err := parseParamToAbiArgument(param, fmt.Sprintf("%d", idx))
		if err != nil {
			return nil, fmt.Errorf("failed to parse param to arg '%s': %v", param, err)
		}
		inputs = append(inputs, *arg)
	}
	return inputs, nil
}

/**
 * Splits a string of parameters into a list of parameters
 */
func splitParams(params string) []string {
	var result []string
	depth := 0
	current := ""
	for _, r := range params {
		switch r {
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(current))
				current = ""
				continue
			}
		case '(':
			depth++
		case ')':
			depth--
		}
		current += string(r)
	}
	if strings.TrimSpace(current) != "" {
		result = append(result, strings.TrimSpace(current))
	}
	return result
}

func parseParamToAbiArgument(param string, fallbackName string) (*abi.Argument, error) {
	argName, paramType, err := getArgNameAndType(param, fallbackName)
	if err != nil {
		return nil, fmt.Errorf("failed to get arg name and type '%s': %v", param, err)
	}
	if isTuple(paramType) {
		argType, err := marshalTupleParamToArgumentType(paramType)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tuple: %v", err)
		}
		return &abi.Argument{
			Name: argName,
			Type: argType,
		}, nil
	} else {
		argType, err := abi.NewType(paramType, paramType, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to parse type '%s': %v", paramType, err)
		}
		return &abi.Argument{
			Name: argName,
			Type: argType,
		}, nil
	}
}

func getArgNameAndType(param string, fallbackName string) (name string, paramType string, err error) {
	if isTuple(param) {
		lastParenIndex := strings.LastIndex(param, ")")
		if lastParenIndex == -1 {
			return "", "", fmt.Errorf("invalid tuple format")
		}
		if len(param)-1 == lastParenIndex {
			return fallbackName, param, nil
		}
		paramsEndIdx := lastParenIndex + 1
		if strings.HasPrefix(param[paramsEndIdx:], "[]") {
			paramsEndIdx = lastParenIndex + 3
		}
		return strings.TrimSpace(param[paramsEndIdx:]), param[:paramsEndIdx], nil
	} else {
		tokens := strings.Fields(param)
		if len(tokens) == 1 {
			return fallbackName, strings.TrimSpace(tokens[0]), nil
		}
		return strings.TrimSpace(tokens[len(tokens)-1]), strings.Join(tokens[:len(tokens)-1], " "), nil
	}
}

func isTuple(param string) bool {
	return strings.HasPrefix(param, "(")
}

func marshalTupleParamToArgumentType(paramType string) (abi.Type, error) {
	typ := "tuple"
	isSlice := strings.HasSuffix(paramType, "[]")
	strippedParamType := strings.TrimPrefix(paramType, "(")
	if isSlice {
		strippedParamType = strings.TrimSuffix(strippedParamType, "[]")
		typ = "tuple[]"
	}
	strippedParamType = strings.TrimSuffix(strippedParamType, ")")
	components, err := marshalParamArguments(strippedParamType)
	if err != nil {
		return abi.Type{}, fmt.Errorf("failed to marshal tuple: %v", err)
	}
	return abi.NewType(typ, typ, components)
}

func marshalParamArguments(param string) ([]abi.ArgumentMarshaling, error) {
	paramList := splitParams(param)
	components := []abi.ArgumentMarshaling{}
	for idx, param := range paramList {
		argName, paramType, err := getArgNameAndType(param, fmt.Sprintf("field%d", idx))
		if err != nil {
			return nil, fmt.Errorf("failed to get arg name and type '%s': %v", param, err)
		}
		if isTuple(paramType) {
			subComponents, err := marshalParamArguments(paramType[1 : len(paramType)-1])
			if err != nil {
				return nil, fmt.Errorf("failed to marshal tuple: %v", err)
			}
			components = append(components, abi.ArgumentMarshaling{
				Type:       "tuple",
				Name:       argName,
				Components: subComponents,
			})
		} else {
			components = append(components, abi.ArgumentMarshaling{
				Type: paramType,
				Name: argName,
			})
		}
	}
	return components, nil
}
