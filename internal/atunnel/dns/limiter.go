// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dns

import "sync/atomic"

const (
	// maxInFlight UDP queries.
	maxInFlight = 64
	// maxConnections bounds open TCP connections.
	maxConnections = 16
)

// limiter caps tracks DNS query limits.
type limiter struct {
	inFlight    *limiterSlots
	connections *limiterSlots
}

func newLimiter() *limiter {
	return &limiter{
		inFlight:    &limiterSlots{max: maxInFlight},
		connections: &limiterSlots{max: maxConnections},
	}
}

type limiterSlots struct {
	max  int32
	used atomic.Int32
}

// tryAcquire a virtual slot. Returns false if this slot is at capacity.
func (s *limiterSlots) tryAcquire() bool {
	// Disabled if zero.
	if s == nil {
		return true
	}

	if s.used.Add(1) > s.max {
		s.used.Add(-1)
		return false
	}
	return true
}

// release a use slot. MUST be paired with a successful tryAcquire.
func (s *limiterSlots) release() {
	// Disabled if zero.
	if s == nil {
		return
	}

	if s.used.Add(-1) < 0 {
		panic("dns: slot released more times than acquired")
	}
}

// occupied returns the current slots occupied. Warning: this should only be
// used for testing as the count will not be accurate due to concurrency.
func (s *limiterSlots) occupied() int32 {
	return s.used.Load()
}
