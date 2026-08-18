package retry

import (
	"fmt"
	"time"
)

type Retryer interface {
	IsErrorRetryable(error) bool
	MaxAttempts() int
	RetryDelay(attempt int, opErr error) (time.Duration, error)
}

type NopRetryer struct{}

func (NopRetryer) IsErrorRetryable(error) bool { return false }

func (NopRetryer) MaxAttempts() int { return 1 }

func (NopRetryer) RetryDelay(int, error) (time.Duration, error) {
	return 0, fmt.Errorf("not retrying any attempt errors")
}
