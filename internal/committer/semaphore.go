package committer

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

// SafeSemaphore wraps the standard semaphore to prevent negative values
type SafeSemaphore struct {
	sem    *semaphore.Weighted
	mu     sync.Mutex
	held   int64 // Track how much we've actually acquired
	maxCap int64 // Maximum capacity
}

func NewSafeSemaphore(maxCapacity int64) *SafeSemaphore {
	return &SafeSemaphore{
		sem:    semaphore.NewWeighted(maxCapacity),
		held:   0,
		maxCap: maxCapacity,
	}
}

func (s *SafeSemaphore) Acquire(ctx context.Context, n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.sem.Acquire(ctx, n)
	if err == nil {
		s.held += n
	}
	return err
}

func (s *SafeSemaphore) Release(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only release what we actually have, prevent going below 0
	releaseAmount := n
	if s.held < n {
		releaseAmount = s.held
		log.Warn().
			Int64("requested_release", n).
			Int64("actually_held", s.held).
			Int64("actual_release", releaseAmount).
			Msg("Attempted to release more than held, releasing only what was held to prevent negative semaphore")
	}

	if releaseAmount > 0 {
		s.sem.Release(releaseAmount)
		s.held -= releaseAmount
	}
}

func (s *SafeSemaphore) TryAcquire(n int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sem.TryAcquire(n) {
		s.held += n
		return true
	}
	return false
}
