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

package harness

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errInjected = errors.New("injected")

func TestFault(t *testing.T) {
	t.Run("nothing set lets calls through", func(t *testing.T) {
		var f Fault
		if out := f.decide(t.Context()); !out.apply || out.err != nil {
			t.Errorf("decide = %+v, want applied without error", out)
		}
	})

	t.Run("FailNext fails exactly one call", func(t *testing.T) {
		var f Fault
		f.FailNext(errInjected)
		if out := f.decide(t.Context()); out.apply || out.err != errInjected {
			t.Errorf("first decide = %+v, want not applied, failing with errInjected", out)
		}
		if out := f.decide(t.Context()); !out.apply || out.err != nil {
			t.Errorf("second decide = %+v, want applied without error", out)
		}
	})

	t.Run("Block holds the call until Release", func(t *testing.T) {
		var f Fault
		gate := f.Block()
		done := make(chan outcome)
		go func() { done <- f.decide(context.Background()) }()
		gate.Wait(t)
		select {
		case out := <-done:
			t.Fatalf("call returned %+v before Release", out)
		default:
		}
		gate.Release()
		if out := <-done; !out.apply || out.err != nil {
			t.Errorf("released call = %+v, want applied without error", out)
		}
	})

	t.Run("a held call ends with its context", func(t *testing.T) {
		var f Fault
		gate := f.Block()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan outcome)
		go func() { done <- f.decide(ctx) }()
		gate.Wait(t)
		cancel()
		if out := <-done; out.apply || status.Code(out.err) != codes.Canceled {
			t.Errorf("canceled call = %+v, want not applied, failing with Canceled", out)
		}
	})
}
