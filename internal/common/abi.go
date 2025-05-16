package common

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

var (
	httpClient     *http.Client
	httpClientOnce sync.Once
)

func getHTTPClient() *http.Client {
	httpClientOnce.Do(func() {
		httpClient = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        config.Cfg.API.ContractApiRequest.MaxIdleConns,
				MaxIdleConnsPerHost: config.Cfg.API.ContractApiRequest.MaxIdleConnsPerHost,
				MaxConnsPerHost:     config.Cfg.API.ContractApiRequest.MaxConnsPerHost,
				IdleConnTimeout:     time.Duration(config.Cfg.API.ContractApiRequest.IdleConnTimeout) * time.Second,
				DisableCompression:  config.Cfg.API.ContractApiRequest.DisableCompression,
			},
			Timeout: time.Duration(config.Cfg.API.ContractApiRequest.Timeout) * time.Second,
		}
	})
	return httpClient
}

func GetABIForContractWithCache(chainId string, contract string, abiCache *sync.Map) *abi.ABI {
	if abiValue, ok := abiCache.Load(contract); ok {
		if abi, ok := abiValue.(*abi.ABI); ok {
			return abi
		}
	}

	abiResult, err := GetABIForContract(chainId, contract)
	if err != nil {
		abiCache.Store(contract, nil)
		return nil
	}
	abiCache.Store(contract, abiResult)
	return abiResult
}

func GetABIForContract(chainId string, contract string) (*abi.ABI, error) {
	url := fmt.Sprintf("%s/abi/%s/%s", config.Cfg.API.ThirdwebContractApi, chainId, contract)

	resp, err := getHTTPClient().Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get contract abi: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get contract abi: unexpected status code %d", resp.StatusCode)
	}

	abi, err := abi.JSON(resp.Body)
	if err != nil {
		log.Warn().Err(err).Str("contract", contract).Str("chainId", chainId).Msg("Failed to parse contract ABI")
		return nil, fmt.Errorf("failed to load contract abi: %v", err)
	}
	return &abi, nil
}

func ConstructEventABI(signature string) (*abi.Event, error) {
	// Regex to extract the event name and parameters
	regex := regexp.MustCompile(`^(\w+)\s*\((.*)\)$`)
	matches := regex.FindStringSubmatch(strings.TrimSpace(signature))
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid event signature format")
	}

	eventName := matches[1]
	parameters := matches[2]

	inputs, err := parseParamsToAbiArguments(parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to parse params to abi arguments '%s': %v", parameters, err)
	}

	event := abi.NewEvent(eventName, eventName, false, inputs)

	return &event, nil
}

func ConstructFunctionABI(signature string) (*abi.Method, error) {
	regex := regexp.MustCompile(`^(\w+)\s*\((.*)\)$`)
	matches := regex.FindStringSubmatch(strings.TrimSpace(signature))
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid function signature format")
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
	argName, paramType, indexed, err := getArgNameAndType(param, fallbackName)
	if err != nil {
		return nil, fmt.Errorf("failed to get arg name and type '%s': %v", param, err)
	}
	if isTuple(paramType) {
		argType, err := marshalTupleParamToArgumentType(paramType)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tuple: %v", err)
		}
		return &abi.Argument{
			Name:    argName,
			Type:    argType,
			Indexed: indexed,
		}, nil
	} else {
		argType, err := abi.NewType(paramType, paramType, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to parse type '%s': %v", paramType, err)
		}
		return &abi.Argument{
			Name:    argName,
			Type:    argType,
			Indexed: indexed,
		}, nil
	}
}

func getArgNameAndType(param string, fallbackName string) (name string, paramType string, indexed bool, err error) {
	param, indexed = checkIfParamIsIndexed(param)
	if isTuple(param) {
		lastParenIndex := strings.LastIndex(param, ")")
		if lastParenIndex == -1 {
			return "", "", false, fmt.Errorf("invalid tuple format")
		}
		if len(param)-1 == lastParenIndex {
			return fallbackName, param, indexed, nil
		}
		paramsEndIdx := lastParenIndex + 1
		if strings.HasPrefix(param[paramsEndIdx:], "[]") {
			paramsEndIdx = lastParenIndex + 3
		}
		return strings.TrimSpace(param[paramsEndIdx:]), param[:paramsEndIdx], indexed, nil
	} else {
		tokens := strings.Fields(param)
		if len(tokens) == 1 {
			return fallbackName, strings.TrimSpace(tokens[0]), indexed, nil
		}
		return strings.TrimSpace(tokens[len(tokens)-1]), strings.Join(tokens[:len(tokens)-1], " "), indexed, nil
	}
}

func checkIfParamIsIndexed(param string) (string, bool) {
	tokens := strings.Fields(param)
	indexed := false
	for i, token := range tokens {
		if token == "indexed" || strings.HasPrefix(token, "index_topic_") {
			tokens = append(tokens[:i], tokens[i+1:]...)
			indexed = true
			break
		}
	}
	param = strings.Join(tokens, " ")
	return param, indexed
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
		argName, paramType, indexed, err := getArgNameAndType(param, fmt.Sprintf("field%d", idx))
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
				Indexed:    indexed,
			})
		} else {
			components = append(components, abi.ArgumentMarshaling{
				Type:    paramType,
				Name:    argName,
				Indexed: indexed,
			})
		}
	}
	return components, nil
}
