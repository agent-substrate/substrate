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
	"context"
	"testing"

	"google.golang.org/grpc/connectivity"
)

// TestAteomDialerClosesEvictedConns pins the eviction behavior: a connection
// pushed out of the cache must be closed, not forgotten. A forgotten conn
// keeps its goroutines and buffers for the life of the process, which turns
// worker pod churn past the cache size into an unbounded leak.
func TestAteomDialerClosesEvictedConns(t *testing.T) {
	d := newAteomDialer(1)

	first, err := d.DialAteomPod(context.Background(), "pod-a")
	if err != nil {
		t.Fatalf("DialAteomPod(pod-a) error = %v", err)
	}
	if _, err := d.DialAteomPod(context.Background(), "pod-b"); err != nil {
		t.Fatalf("DialAteomPod(pod-b) error = %v", err)
	}

	if got := first.GetState(); got != connectivity.Shutdown {
		t.Errorf("evicted conn state = %v, want %v (closed on eviction)", got, connectivity.Shutdown)
	}
}

// TestAteomDialerReusesCachedConn pins the cache hit path: dialing the same
// pod twice returns the same live connection.
func TestAteomDialerReusesCachedConn(t *testing.T) {
	d := newAteomDialer(2)

	first, err := d.DialAteomPod(context.Background(), "pod-a")
	if err != nil {
		t.Fatalf("DialAteomPod(pod-a) error = %v", err)
	}
	second, err := d.DialAteomPod(context.Background(), "pod-a")
	if err != nil {
		t.Fatalf("DialAteomPod(pod-a) again error = %v", err)
	}
	if first != second {
		t.Errorf("second dial returned a different conn; want the cached one")
	}
	if got := first.GetState(); got == connectivity.Shutdown {
		t.Errorf("cached conn is shut down; want it live")
	}
}
