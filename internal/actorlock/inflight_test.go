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

package actorlock

import (
	"context"
	"testing"
	"time"
)

// TestWaitIdleWaitsForEveryActor verifies all RPCs finish before the wait returns.
func TestWaitIdleWaitsForEveryActor(t *testing.T) {
	f := NewInFlight()
	releaseA := f.Add("actor-a", "RunWorkload")
	releaseB := f.Add("actor-b", "CheckpointWorkload")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if f.WaitIdle(ctx) {
		t.Fatal("WaitIdle returned while two RPCs were in flight")
	}

	releaseA()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	if f.WaitIdle(ctx2) {
		t.Fatal("WaitIdle returned while actor-b was still in flight")
	}

	releaseB()
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	if !f.WaitIdle(ctx3) {
		t.Error("WaitIdle did not return once nothing was in flight")
	}
}
