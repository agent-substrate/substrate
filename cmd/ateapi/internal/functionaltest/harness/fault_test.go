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

package harness

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errInjected = status.Error(codes.Aborted, "injected")

func TestFault(t *testing.T) {
	type result struct {
		resp any
		err  error
	}
	// newCall returns a call that replies "ok", and the count of calls made.
	newCall := func() (func() (any, error), *int) {
		var made int
		return func() (any, error) { made++; return "ok", nil }, &made
	}

	t.Run("nothing set makes the call", func(t *testing.T) {
		var f Fault
		call, made := newCall()
		if resp, err := f.run(t.Context(), call); resp != "ok" || err != nil || *made != 1 {
			t.Errorf("run = %v, %v after %d calls; want ok after one call", resp, err, *made)
		}
	})

	t.Run("FailNext fails exactly one call without making it", func(t *testing.T) {
		var f Fault
		f.FailNext(errInjected)
		call, made := newCall()
		if _, err := f.run(t.Context(), call); err != errInjected || *made != 0 {
			t.Errorf("first run = %v after %d calls; want errInjected and no call", err, *made)
		}
		if resp, err := f.run(t.Context(), call); resp != "ok" || err != nil || *made != 1 {
			t.Errorf("second run = %v, %v after %d calls; want ok after one call", resp, err, *made)
		}
	})

	t.Run("Block holds the call until Release", func(t *testing.T) {
		var f Fault
		gate := f.Block()
		call, made := newCall()
		done := make(chan result)
		go func() {
			resp, err := f.run(context.Background(), call)
			done <- result{resp, err}
		}()
		gate.Wait(t)
		select {
		case r := <-done:
			t.Fatalf("run returned %+v before Release", r)
		default:
		}
		gate.Release()
		if r := <-done; r.resp != "ok" || r.err != nil || *made != 1 {
			t.Errorf("released run = %+v after %d calls; want ok after one call", r, *made)
		}
	})

	t.Run("HoldReply makes the call and holds its reply until Release", func(t *testing.T) {
		var f Fault
		gate := f.HoldReply()
		call, made := newCall()
		done := make(chan result)
		go func() {
			resp, err := f.run(context.Background(), call)
			done <- result{resp, err}
		}()
		gate.Wait(t)
		select {
		case r := <-done:
			t.Fatalf("run returned %+v before Release", r)
		default:
		}
		gate.Release()
		if r := <-done; r.resp != "ok" || r.err != nil || *made != 1 {
			t.Errorf("released run = %+v after %d calls; want ok after one call", r, *made)
		}
	})

	t.Run("a held call ends with its context without being made", func(t *testing.T) {
		var f Fault
		gate := f.Block()
		call, made := newCall()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan result)
		go func() {
			resp, err := f.run(ctx, call)
			done <- result{resp, err}
		}()
		gate.Wait(t)
		cancel()
		if r := <-done; status.Code(r.err) != codes.Canceled || *made != 0 {
			t.Errorf("canceled run = %+v after %d calls; want Canceled and no call", r, *made)
		}
	})
}
