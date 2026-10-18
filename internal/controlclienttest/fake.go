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

// Package controlclienttest provides a shared fake ateapipb.ControlClient for
// tests that only need a handful of RPCs, so callers stop hand-rolling their
// own copy of the same embed-and-override struct.
package controlclienttest

import (
	"context"
	"sync"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

// Fake is an ateapipb.ControlClient for tests. Each RPC it implements records
// its name, then defers to the matching *Func field if set, or a zero-value
// response otherwise. RPCs it does not implement panic if called, through the
// embedded nil interface.
type Fake struct {
	ateapipb.ControlClient

	mu              sync.Mutex
	calls           []string
	deleteRequests  []*ateapipb.DeleteActorRequest
	suspendRequests []*ateapipb.SuspendActorRequest

	CreateAtespaceFunc func(ctx context.Context, in *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error)
	CreateActorFunc    func(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
	ResumeActorFunc    func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)
	SuspendActorFunc   func(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error)
	DeleteActorFunc    func(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
	ListActorsFunc     func(ctx context.Context, in *ateapipb.ListActorsRequest, opts ...grpc.CallOption) (*ateapipb.ListActorsResponse, error)
}

func (f *Fake) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

// RecordedCalls returns the RPC names invoked so far, in call order.
func (f *Fake) RecordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// RecordedDeleteActorRequests returns every DeleteActor request received so far.
func (f *Fake) RecordedDeleteActorRequests() []*ateapipb.DeleteActorRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*ateapipb.DeleteActorRequest(nil), f.deleteRequests...)
}

// RecordedSuspendActorRequests returns every SuspendActor request received so far.
func (f *Fake) RecordedSuspendActorRequests() []*ateapipb.SuspendActorRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*ateapipb.SuspendActorRequest(nil), f.suspendRequests...)
}

func (f *Fake) CreateAtespace(ctx context.Context, in *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	f.record("CreateAtespace")
	if f.CreateAtespaceFunc != nil {
		return f.CreateAtespaceFunc(ctx, in, opts...)
	}
	return &ateapipb.Atespace{}, nil
}

func (f *Fake) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.record("CreateActor")
	if f.CreateActorFunc != nil {
		return f.CreateActorFunc(ctx, in, opts...)
	}
	return &ateapipb.Actor{}, nil
}

func (f *Fake) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.record("ResumeActor")
	if f.ResumeActorFunc != nil {
		return f.ResumeActorFunc(ctx, in, opts...)
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

// SuspendActor records "SuspendActor(async)" for an asynchronous-durability
// suspend and "SuspendActor" for any other, so tests can tell them apart.
func (f *Fake) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	name := "SuspendActor"
	if in.GetOptions().GetMode() == ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS {
		name = "SuspendActor(async)"
	}
	f.mu.Lock()
	f.calls = append(f.calls, name)
	f.suspendRequests = append(f.suspendRequests, in)
	f.mu.Unlock()
	if f.SuspendActorFunc != nil {
		return f.SuspendActorFunc(ctx, in, opts...)
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *Fake) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "DeleteActor")
	f.deleteRequests = append(f.deleteRequests, in)
	f.mu.Unlock()
	if f.DeleteActorFunc != nil {
		return f.DeleteActorFunc(ctx, in, opts...)
	}
	return &ateapipb.Actor{}, nil
}

func (f *Fake) ListActors(ctx context.Context, in *ateapipb.ListActorsRequest, opts ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	f.record("ListActors")
	if f.ListActorsFunc != nil {
		return f.ListActorsFunc(ctx, in, opts...)
	}
	return &ateapipb.ListActorsResponse{}, nil
}

// Sequence returns a function that hands back the next error in errs on each
// call, or nil once the list is drained. Safe for concurrent use, so a *Func
// field can inject a run of errors before falling back to success.
func Sequence(errs ...error) func() error {
	var mu sync.Mutex
	i := 0
	return func() error {
		mu.Lock()
		defer mu.Unlock()
		if i >= len(errs) {
			return nil
		}
		err := errs[i]
		i++
		return err
	}
}
