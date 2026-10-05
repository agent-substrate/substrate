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

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// CreateActor creates the actor ref from template, and suspends and deletes it
// when the test ends. Cleanup is best-effort and gets its own context so it
// still runs after the test's has been canceled.
func CreateActor(t *testing.T, ctx context.Context, clients *Clients, ref, template *ateapipb.ObjectRef) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		ActorTemplate: template,
	}}); err != nil {
		t.Fatalf("CreateActor %s/%s: %v", ref.GetAtespace(), ref.GetName(), err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		// DeleteActor requires a suspended actor.
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: ref})
	})
}

// WaitForActorState polls the actor until it reaches want, and fails the test
// with the last state or error it saw if that takes longer than timeout. A
// failed GetActor counts as not there yet.
func WaitForActorState(t *testing.T, ctx context.Context, clients *Clients, ref *ateapipb.ObjectRef, want ateapipb.ActorState, timeout time.Duration) {
	t.Helper()
	if last, ok := pollActorState(ctx, clients.SubstrateAPI, ref, want, timeout, time.Second); !ok {
		t.Fatalf("timed out after %v waiting for actor %s/%s to reach %v; last seen: %s", timeout, ref.GetAtespace(), ref.GetName(), want, last)
	}
}

func pollActorState(ctx context.Context, api ateapipb.ControlClient, ref *ateapipb.ObjectRef, want ateapipb.ActorState, timeout, interval time.Duration) (last string, ok bool) {
	deadline := time.Now().Add(timeout)
	for {
		resp, err := api.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
		switch {
		case err != nil:
			last = err.Error()
		case resp.GetStatus().GetState() == want:
			return "", true
		default:
			last = resp.GetStatus().GetState().String()
		}
		if time.Now().After(deadline) {
			return last, false
		}
		time.Sleep(interval)
	}
}
