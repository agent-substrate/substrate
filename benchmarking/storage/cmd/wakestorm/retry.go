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
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
)

// retryPolicy says whether a failed call is retried and after how long.
type retryPolicy struct {
	maxConflict int  // retries on Aborted (concurrent update)
	contention  bool // retry a volume claim that failed with "please retry"
	noRoom      bool // retry while no worker has room
}

// next reports whether to retry after err on the given attempt (1 for the
// first retry), and how long to wait first. Only ResumeActor uses contention
// and noRoom.
func (p retryPolicy) next(err error, attempt int, resume bool) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	if status.Code(err) == codes.Aborted && attempt <= p.maxConflict {
		return min(time.Duration(200*attempt)*time.Millisecond, 3*time.Second), true
	}
	if !resume {
		return 0, false
	}
	// A full cluster counts as a wait: retry until a slot frees up or the
	// call times out.
	if p.noRoom && status.Code(err) == codes.ResourceExhausted && strings.Contains(err.Error(), "no worker has room") {
		return time.Second, true
	}
	// The volume pool returns "concurrency contention ... please retry" when
	// many first resumes bind volumes at once. Back off from 50 ms, doubling
	// to 1 s, like the spawn retries in internal/benchmarking/boomer/glutton.
	if p.contention && strings.Contains(err.Error(), "please retry") {
		return min(50*time.Millisecond<<min(attempt-1, 5), time.Second), true
	}
	return 0, false
}

// do calls fn until it succeeds, the policy stops retrying, or ctx ends. It
// returns the number of retries and the last error.
func (p retryPolicy) do(ctx context.Context, resume bool, fn func() error) (int, error) {
	for attempt := 1; ; attempt++ {
		err := fn()
		wait, ok := p.next(err, attempt, resume)
		if !ok {
			return attempt - 1, err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return attempt - 1, err
		}
	}
}

func (r *runner) resumeCall(ctx context.Context, name string) (int, error) {
	return r.retry.do(ctx, true, func() error {
		_, err := r.cli.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: r.ref(name)})
		return err
	})
}

// suspendCall suspends asynchronously: SuspendActor returns once the snapshot
// is captured on the node, and the upload to durable storage continues in the
// background. This is the API default, set here so the measurement does not
// change if the default does.
func (r *runner) suspendCall(ctx context.Context, name string) (int, error) {
	return r.retry.do(ctx, false, func() error {
		_, err := r.cli.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor:   r.ref(name),
			Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
		})
		return err
	})
}
