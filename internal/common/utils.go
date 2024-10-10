package common

import "math/big"

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
