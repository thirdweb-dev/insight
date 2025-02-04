package common

import (
	"fmt"
	"time"
)

// temporary type to handle backwards compatibility with old block timestamps
type BlockTimestamp struct {
	time.Time
}

// Implement sql.Scanner interface
func (fs *BlockTimestamp) Scan(value interface{}) error {
	if value == nil {
		*fs = BlockTimestamp{time.Time{}}
		return nil
	}

	switch v := value.(type) {
	case uint64:
		*fs = BlockTimestamp{time.Unix(int64(v), 0)}
		return nil
	case time.Time:
		*fs = BlockTimestamp{v}
		return nil
	default:
		return fmt.Errorf("expected uint64 or time.Time for BlockTimestamp, got %T", value)
	}
}
