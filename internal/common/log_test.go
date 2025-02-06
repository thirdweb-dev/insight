package common

import (
	"math/big"
	"testing"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestDecodeLog(t *testing.T) {
	event := Log{
		Data:   "0x000000000000000000000000000000000000000000000000b2da0f6658944b0600000000000000000000000000000000000000000000000000000000000000003492dc030870ae719a0babc07807601edd3fc7e150a6b4878d1c5571bd9995c00000000000000000000000000000000000000000000000e076c8d70085af000000000000000000000000000000000000000000000000000000469c6478f693140000000000000000000000000000000000000000000000000000000000000000",
		Topic0: "0x7be266734f0c132a415c32a35b76cbf3d8a02fa3d88628b286dcf713f53f1e2d",
		Topic1: "0xc148159472ef0bbd3a304d3d3637b8deeda456572700669fda4f8d0fad814402",
		Topic2: "0x000000000000000000000000ff0cb0351a356ad16987e5809a8daaaf34f5adbe",
	}

	eventABI, err := ConstructEventABI("LogCanonicalOrderFilled(bytes32 indexed orderHash,address indexed orderMaker,uint256 fillAmount,uint256 triggerPrice,bytes32 orderFlags,(uint256 price,uint128 fee,bool isNegativeFee) fill)")
	assert.NoError(t, err)
	decodedEvent := event.Decode(eventABI)

	assert.Equal(t, "LogCanonicalOrderFilled", decodedEvent.Decoded.Name)
	assert.Equal(t, "0xc148159472ef0bbd3a304d3d3637b8deeda456572700669fda4f8d0fad814402", decodedEvent.Decoded.IndexedParams["orderHash"])
	assert.Equal(t, gethCommon.HexToAddress("0xff0cb0351a356ad16987e5809a8daaaf34f5adbe"), decodedEvent.Decoded.IndexedParams["orderMaker"])

	expectedFillAmountValue := big.NewInt(0)
	expectedFillAmountValue.SetString("12887630215921289990", 10)
	assert.Equal(t, expectedFillAmountValue, decodedEvent.Decoded.NonIndexedParams["fillAmount"])
	assert.Equal(t, "0x3492dc030870ae719a0babc07807601edd3fc7e150a6b4878d1c5571bd9995c0", decodedEvent.Decoded.NonIndexedParams["orderFlags"])
	expectedTriggerPriceValue := big.NewInt(0)
	assert.Equal(t, expectedTriggerPriceValue.String(), decodedEvent.Decoded.NonIndexedParams["triggerPrice"].(*big.Int).String())

	fillTuple := decodedEvent.Decoded.NonIndexedParams["fill"].(struct {
		Price         *big.Int `json:"price"`
		Fee           *big.Int `json:"fee"`
		IsNegativeFee bool     `json:"isNegativeFee"`
	})

	assert.Equal(t, "4140630000000000000000", fillTuple.Price.String())
	assert.Equal(t, "19875203709834004", fillTuple.Fee.String())
	assert.Equal(t, false, fillTuple.IsNegativeFee)
}
