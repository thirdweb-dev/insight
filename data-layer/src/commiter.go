package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const DEFAULT_COMMITER_TRIGGER_INTERVAL = 250
const DEFAULT_BLOCKS_PER_COMMIT = 10

type Commiter struct {
	triggerIntervalMs int
	blocksPerCommit   int
}

func NewCommiter() *Commiter {
	triggerInterval, err := strconv.Atoi(os.Getenv("COMMITER_TRIGGER_INTERVAL"))
	if err != nil || triggerInterval == 0 {
		triggerInterval = DEFAULT_COMMITER_TRIGGER_INTERVAL
	}
	blocksPerCommit, err := strconv.Atoi(os.Getenv("BLOCKS_PER_COMMIT"))
	if err != nil || blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}
	return &Commiter{
		triggerIntervalMs: triggerInterval,
		blocksPerCommit:   blocksPerCommit,
	}
}

func (c *Commiter) Start() error {
	interval := time.Duration(c.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	go func() error {
		for t := range ticker.C {
			fmt.Println("Commiter running at", t)
			// TODO: fetch max block number from main table
			// TODO: fetch sequential block numbers from staging table
			// TODO: save to main table
			// TODO: delete from staging table
		}
		return nil
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}
