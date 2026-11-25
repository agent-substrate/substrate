//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"testing"

	"go.ate.dev/substrate/internal/ateworker/memborrow"
)

func TestCheckpointFlush(t *testing.T) {
	// a pod with room to spare, so Tight reports false
	s := &AteWorkerService{borrow: &memborrow.Ledger{
		Unbooked:  func(int64) int64 { return 3 << 30 },
		PodMemory: func() (int64, int64, error) { return 4 << 30, 1 << 30, nil },
	}}
	for _, tc := range []struct {
		name                string
		cacheHigh, borrowed int64
		// wantNow is whether write-out starts right away.
		wantNow bool
	}{
		{name: "whole write lent", cacheHigh: 0, borrowed: 512 << 20, wantNow: false},
		{name: "partial loan", cacheHigh: 528 << 20, borrowed: 256 << 20, wantNow: true},
		{name: "sandbox memory reads 0", cacheHigh: 0, borrowed: 0, wantNow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flush := s.checkpointFlush(tc.cacheHigh, tc.borrowed)
			// StartWriteback starts write-out right away for a nil flush.
			now := flush == nil
			if !now {
				now, _ = flush()
			}
			if now != tc.wantNow {
				t.Errorf("checkpointFlush(%d, %d) starts write-out right away = %v, want %v", tc.cacheHigh, tc.borrowed, now, tc.wantNow)
			}
		})
	}
}
