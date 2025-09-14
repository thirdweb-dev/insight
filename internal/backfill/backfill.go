package backfill

import (
	"github.com/thirdweb-dev/indexer/internal/libs"
)

func Init() {
	libs.InitOldClickHouseV1()
	libs.InitNewClickHouseV2()
	libs.InitS3()
	libs.InitRPCClient()
}

func RunBackfill() {

}
