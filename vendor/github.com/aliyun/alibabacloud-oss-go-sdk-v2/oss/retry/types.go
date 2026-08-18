package retry

import "time"

type RetryOptions struct {
	MaxAttempts     int
	MaxBackoff      time.Duration
	BaseDelay       time.Duration
	Backoff         BackoffDelayer
	ErrorRetryables []ErrorRetryable
}

type BackoffDelayer interface {
	BackoffDelay(attempt int, err error) (time.Duration, error)
}

type ErrorRetryable interface {
	IsErrorRetryable(error) bool
}
