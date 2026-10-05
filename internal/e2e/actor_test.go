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
	"errors"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// fakeControl answers GetActor from a script, repeating its last entry, and
// records the lifecycle calls CreateActor makes.
type fakeControl struct {
	ateapipb.ControlClient
	script []fakeGetActor
	calls  int
	log    []string
}

type fakeGetActor struct {
	state ateapipb.ActorState
	err   error
}

func (f *fakeControl) GetActor(context.Context, *ateapipb.GetActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
	step := f.script[min(f.calls, len(f.script)-1)]
	f.calls++
	if step.err != nil {
		return nil, step.err
	}
	return &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: step.state}}, nil
}

func (f *fakeControl) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.log = append(f.log, "create "+req.GetActor().GetMetadata().GetAtespace()+"/"+req.GetActor().GetMetadata().GetName()+" from "+req.GetActor().GetActorTemplate().GetName())
	return req.GetActor(), nil
}

func (f *fakeControl) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.log = append(f.log, "suspend "+req.GetActor().GetName())
	return nil, nil
}

func (f *fakeControl) DeleteActor(_ context.Context, req *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.log = append(f.log, "delete "+req.GetActor().GetName())
	return nil, nil
}

func TestCreateActor(t *testing.T) {
	api := &fakeControl{}
	clients := &Clients{SubstrateAPI: &ateclient.Client{ControlClient: api}}
	t.Run("test", func(t *testing.T) {
		CreateActor(t, context.Background(), clients, &ateapipb.ObjectRef{Atespace: "ns", Name: "a"}, &ateapipb.ObjectRef{Atespace: "ns", Name: "tmpl"})
		if want := []string{"create ns/a from tmpl"}; !slices.Equal(api.log, want) {
			t.Errorf("calls during the test = %q, want %q", api.log, want)
		}
	})
	if want := []string{"create ns/a from tmpl", "suspend a", "delete a"}; !slices.Equal(api.log, want) {
		t.Errorf("calls after the test = %q, want %q", api.log, want)
	}
}

func TestPollActorState(t *testing.T) {
	ref := &ateapipb.ObjectRef{Atespace: "ns", Name: "a"}
	running := ateapipb.ActorState_ACTOR_STATE_RUNNING
	suspended := ateapipb.ActorState_ACTOR_STATE_SUSPENDED

	t.Run("errors and other states are retried", func(t *testing.T) {
		api := &fakeControl{script: []fakeGetActor{{err: errors.New("unavailable")}, {state: suspended}, {state: running}}}
		if last, ok := pollActorState(context.Background(), api, ref, running, time.Minute, time.Millisecond); !ok {
			t.Fatalf("pollActorState = not reached (last %q), want reached", last)
		}
		if api.calls != 3 {
			t.Errorf("GetActor calls = %d, want 3", api.calls)
		}
	})

	t.Run("timeout reports the last state", func(t *testing.T) {
		api := &fakeControl{script: []fakeGetActor{{err: errors.New("unavailable")}, {state: suspended}}}
		last, ok := pollActorState(context.Background(), api, ref, running, 20*time.Millisecond, time.Millisecond)
		if ok {
			t.Fatal("pollActorState = reached, want a timeout")
		}
		if last != suspended.String() {
			t.Errorf("last = %q, want %q", last, suspended.String())
		}
	})

	t.Run("timeout reports the last error", func(t *testing.T) {
		api := &fakeControl{script: []fakeGetActor{{err: errors.New("unavailable")}}}
		if last, _ := pollActorState(context.Background(), api, ref, running, 0, time.Millisecond); last != "unavailable" {
			t.Errorf("last = %q, want the GetActor error", last)
		}
	})
}
