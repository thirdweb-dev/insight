package common

import (
	"math/big"
	"testing"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestDecodeTransaction(t *testing.T) {
	transaction := Transaction{
		Data: "0x095ea7b3000000000000000000000000971add32ea87f10bd192671630be3be8a11b862300000000000000000000000000000000000000000000010df58ac64e49b91ea0",
	}

	abi, err := ConstructFunctionABI("approve(address _spender, uint256 _value)")
	assert.NoError(t, err)
	decodedTransaction := transaction.Decode(abi)

	assert.Equal(t, "approve", decodedTransaction.Decoded.Name)
	assert.Equal(t, gethCommon.HexToAddress("0x971add32Ea87f10bD192671630be3BE8A11b8623"), decodedTransaction.Decoded.Inputs["_spender"])
	expectedValue := big.NewInt(0)
	expectedValue.SetString("4979867327953494417056", 10)
	assert.Equal(t, expectedValue, decodedTransaction.Decoded.Inputs["_value"])

	transaction2 := Transaction{
		Data: "0x27c777a9000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000c0000000000000000000000000000000000000000000000000000000000000007b00000000000000000000000000000000000000000000000000000000672c0c60302aafae8a36ffd8c12b32f1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000038d7ea4c680000000000000000000000000000734d56da60852a03e2aafae8a36ffd8c12b32f10000000000000000000000000000000000000000000000000000000000000000",
	}
	abi2, err := ConstructFunctionABI("allocatedWithdrawal((bytes,uint256,uint256,uint256,uint256,address) _withdrawal)")
	assert.NoError(t, err)
	decodedTransaction2 := transaction2.Decode(abi2)

	assert.Equal(t, "allocatedWithdrawal", decodedTransaction2.Decoded.Name)
	withdrawal := decodedTransaction2.Decoded.Inputs["_withdrawal"].(struct {
		Field0 []uint8            `json:"field0"`
		Field1 *big.Int           `json:"field1"`
		Field2 *big.Int           `json:"field2"`
		Field3 *big.Int           `json:"field3"`
		Field4 *big.Int           `json:"field4"`
		Field5 gethCommon.Address `json:"field5"`
	})

	assert.Equal(t, []uint8{}, withdrawal.Field0)
	assert.Equal(t, "123", withdrawal.Field1.String())
	assert.Equal(t, "1730940000", withdrawal.Field2.String())
	assert.Equal(t, "21786436819914608908212656341824591317420268878283544900672692017070052737024", withdrawal.Field3.String())
	assert.Equal(t, "1000000000000000", withdrawal.Field4.String())
	assert.Equal(t, "0x0734d56DA60852A03e2Aafae8a36FFd8c12B32f1", withdrawal.Field5.Hex())
}
