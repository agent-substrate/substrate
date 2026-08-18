package retry

import (
	"time"
)

const (
	DefaultMaxAttempts = 3
	DefaultMaxBackoff  = 20 * time.Second
	DefaultBaseDelay   = 200 * time.Millisecond
)

var DefaultErrorRetryables = []ErrorRetryable{
	&HTTPStatusCodeRetryable{},
	&ServiceErrorCodeRetryable{},
	&ConnectionErrorRetryable{},
}

type Standard struct {
	maxAttempts int
	retryables  []ErrorRetryable
	backoff     BackoffDelayer
}

func NewStandard(fnOpts ...func(*RetryOptions)) *Standard {
	o := RetryOptions{
		MaxAttempts:     DefaultMaxAttempts,
		MaxBackoff:      DefaultMaxBackoff,
		BaseDelay:       DefaultBaseDelay,
		ErrorRetryables: DefaultErrorRetryables,
	}

	for _, fn := range fnOpts {
		fn(&o)
	}

	if o.MaxAttempts <= 0 {
		o.MaxAttempts = DefaultMaxAttempts
	}

	if o.BaseDelay <= 0 {
		o.BaseDelay = DefaultBaseDelay
	}

	if o.Backoff == nil {
		o.Backoff = NewFullJitterBackoff(o.BaseDelay, o.MaxBackoff)
	}

	return &Standard{
		maxAttempts: o.MaxAttempts,
		retryables:  o.ErrorRetryables,
		backoff:     o.Backoff,
	}
}

func (s *Standard) MaxAttempts() int {
	return s.maxAttempts
}

func (s *Standard) IsErrorRetryable(err error) bool {
	for _, re := range s.retryables {
		if v := re.IsErrorRetryable(err); v {
			return v
		}
	}
	return false
}

func (s *Standard) RetryDelay(attempt int, err error) (time.Duration, error) {
	return s.backoff.BackoffDelay(attempt, err)
}
