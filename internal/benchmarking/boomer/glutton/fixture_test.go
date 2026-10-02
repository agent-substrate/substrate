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

package glutton

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	"github.com/agent-substrate/substrate/internal/controlclienttest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

// resumeActorFunc returns a ResumeActorFunc that fails with each of errs in
// order, then succeeds.
func resumeActorFunc(errs ...error) func(context.Context, *ateapipb.ResumeActorRequest, ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	next := controlclienttest.Sequence(errs...)
	return func(context.Context, *ateapipb.ResumeActorRequest, ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
		if err := next(); err != nil {
			return nil, err
		}
		return &ateapipb.ResumeActorResponse{}, nil
	}
}

// suspendActorFunc is resumeActorFunc for SuspendActor.
func suspendActorFunc(errs ...error) func(context.Context, *ateapipb.SuspendActorRequest, ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	next := controlclienttest.Sequence(errs...)
	return func(context.Context, *ateapipb.SuspendActorRequest, ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
		if err := next(); err != nil {
			return nil, err
		}
		return &ateapipb.SuspendActorResponse{}, nil
	}
}

// createActorFunc adapts fn, which gets each actor's name and may fail the
// create, to a CreateActorFunc.
func createActorFunc(fn func(name string) error) func(context.Context, *ateapipb.CreateActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
	return func(_ context.Context, in *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
		if err := fn(in.GetActor().GetMetadata().GetName()); err != nil {
			return nil, err
		}
		return &ateapipb.Actor{}, nil
	}
}

// newTestConfig starts srv, sets HTTPClient and RouterURL, and ensures
// APIStub, Tracer, and Dyn are populated if nil.
func newTestConfig(t *testing.T, srv *fake.Server, cfg *userclass.Config) *userclass.Config {
	t.Helper()
	ts := srv.Start(t)
	if cfg == nil {
		cfg = &userclass.Config{}
	}
	if cfg.APIStub == nil {
		cfg.APIStub = &controlclienttest.Fake{}
	}
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("test")
	}
	if cfg.Dyn == nil {
		cfg.Dyn = dynconfig.NewHolder(dynconfig.Config{})
	}
	cfg.HTTPClient = ts.Client()
	cfg.RouterURL = ts.URL
	return cfg
}

func newTestDurDirUser(t *testing.T, srv *fake.Server, cfg *userclass.Config) *durDirUser {
	t.Helper()
	c := newTestConfig(t, srv, cfg)
	return &durDirUser{
		cfg:          c,
		actorName:    "duractor",
		templateName: defaultDurTemplate,
		userClass:    durDirUserClass,
		expectedSize: int64(len(srv.Data)),
	}
}
