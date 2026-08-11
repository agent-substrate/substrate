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

package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/imagecache"
)

func TestImageCacheGCTarget(t *testing.T) {
	const gib = int64(1 << 30)
	tests := []struct {
		name                string
		capacity, available uint64
		cacheSize, maxBytes int64
		highPct, lowPct     int
		want                int64
	}{
		{
			name:     "below high watermark: no target",
			capacity: 100 * uint64(gib), available: 30 * uint64(gib), // 70% used
			highPct: 85, lowPct: 80,
			want: 0,
		},
		{
			name:     "at high watermark: free down to low",
			capacity: 100 * uint64(gib), available: 10 * uint64(gib), // 90% used
			cacheSize: 50 * gib, // cache is big enough to cover the shortfall
			highPct:   85, lowPct: 80,
			// available must climb to 20% of capacity: free 20GiB - 10GiB.
			want: 10 * gib,
		},
		{
			name:     "exactly high watermark triggers",
			capacity: 100 * uint64(gib), available: 15 * uint64(gib), // 85% used
			cacheSize: 50 * gib,
			highPct:   85, lowPct: 80,
			want: 5 * gib,
		},
		{
			// The kubelet formula assumes it owns the filesystem; we don't.
			// A near-full boot disk shared with containerd/kubelet/logs must
			// not ask an 11 MiB cache to free 18.9 GiB — uncapped, that
			// evicts the entire cache on every tick forever (0% hit rate)
			// without materially moving disk usage.
			name:     "watermark target capped at what the cache holds",
			capacity: 105 * uint64(gib), available: 2 * uint64(gib), // ~98% used
			cacheSize: 11 << 20,
			highPct:   85, lowPct: 80,
			want: 11 << 20,
		},
		{
			name:     "empty cache under volume pressure: nothing to free",
			capacity: 100 * uint64(gib), available: 1 * uint64(gib),
			cacheSize: 0,
			highPct:   85, lowPct: 80,
			want: 0,
		},
		{
			name:     "max-bytes cap independent of watermarks",
			capacity: 100 * uint64(gib), available: 90 * uint64(gib), // 10% used
			cacheSize: 8 * gib, maxBytes: 5 * gib,
			highPct: 85, lowPct: 80,
			want: 3 * gib,
		},
		{
			name:     "both: larger target wins",
			capacity: 100 * uint64(gib), available: 10 * uint64(gib), // watermark target 10GiB
			cacheSize: 60 * gib, maxBytes: 40 * gib, // cap target 20GiB
			highPct: 85, lowPct: 80,
			want: 20 * gib,
		},
		{
			name:     "max-bytes zero means no cap",
			capacity: 100 * uint64(gib), available: 90 * uint64(gib),
			cacheSize: 500 * gib, maxBytes: 0,
			highPct: 85, lowPct: 80,
			want: 0,
		},
		{
			name:     "zero capacity: watermark half disabled",
			capacity: 0, available: 0,
			cacheSize: 2 * gib, maxBytes: gib,
			highPct: 85, lowPct: 80,
			want: gib,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := imageCacheGCTarget(tc.capacity, tc.available, tc.cacheSize, tc.maxBytes, tc.highPct, tc.lowPct)
			if got != tc.want {
				t.Errorf("imageCacheGCTarget() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestValidateImageCacheGCFlags(t *testing.T) {
	setFlags := func(high, low int, minAge time.Duration) {
		*imageCacheHighPct = high
		*imageCacheLowPct = low
		*imageCacheMinAge = minAge
	}
	t.Cleanup(func() { setFlags(85, 80, 2*time.Minute) })

	cases := []struct {
		name      string
		high, low int
		minAge    time.Duration
		wantErr   bool
	}{
		{"defaults", 85, 80, 2 * time.Minute, false},
		{"boundary high=100 low=0", 100, 0, 0, false},
		{"high over 100", 101, 80, 0, true},
		{"low equals high", 85, 85, 0, true},
		{"low above high", 85, 90, 0, true},
		{"negative low", 85, -1, 0, true},
		{"negative min-age inverts the veto", 85, 80, -time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setFlags(tc.high, tc.low, tc.minAge)
			err := validateImageCacheGCFlags()
			if (err != nil) != tc.wantErr {
				t.Errorf("high=%d low=%d minAge=%v: err=%v, wantErr=%v", tc.high, tc.low, tc.minAge, err, tc.wantErr)
			}
		})
	}
}

func TestClassifyGCPass(t *testing.T) {
	gated := fmt.Errorf("pass gated: %w", imagecache.ErrIncompleteEnumeration)
	perItem := errors.New("while removing retired layer: permission denied")
	cases := []struct {
		name          string
		err           error
		target, freed int64
		want          gcPassOutcome
	}{
		{"gated pass", gated, 100, 0, gcPassSkipped},
		{"gated wins even with zero target", gated, 0, 0, gcPassSkipped},
		{"per-item errors are not a skip", perItem, 100, 100, gcPassComplete},
		{"per-item errors with shortfall", perItem, 100, 40, gcPassShortfall},
		{"shortfall", nil, 100, 40, gcPassShortfall},
		{"target met", nil, 100, 100, gcPassComplete},
		{"no target", nil, 0, 0, gcPassQuiet},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyGCPass(tc.err, tc.target, tc.freed); got != tc.want {
				t.Errorf("classifyGCPass(%v, %d, %d) = %d, want %d", tc.err, tc.target, tc.freed, got, tc.want)
			}
		})
	}
}
