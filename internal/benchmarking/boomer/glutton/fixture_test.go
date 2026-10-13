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
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

type fakeControlClient struct {
	ateapipb.ControlClient
	mu             sync.Mutex
	calls          []string
	createRequests []*ateapipb.CreateActorRequest
	deleteRequests []*ateapipb.DeleteActorRequest
	// resumeErrs is returned by successive ResumeActor calls, in order, until
	// it is drained; every call after that succeeds.
	resumeErrs []error
	// suspendErrs does the same for SuspendActor.
	suspendErrs []error

	// createActorFn, when set, is called with each CreateActor's actor name;
	// a non-nil error is returned in place of the actor.
	createActorFn func(name string) error

	// hook, when set, runs at the start of every CreateAtespace,
	// ResumeActor, SuspendActor, PauseActor, and DeleteActor with the
	// method and actor name ("" for CreateAtespace). It runs outside mu, so
	// it may block, e.g. until ctx is done; a non-nil error is returned in
	// place of the response.
	hook func(ctx context.Context, method, actor string) error
}

// begin records a call and runs hook, if set.
func (f *fakeControlClient) begin(ctx context.Context, method, actor string) error {
	f.mu.Lock()
	f.calls = append(f.calls, method)
	hook := f.hook
	f.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(ctx, method, actor)
}

// nextErr pops the head of errs, or returns nil once it is drained. Callers
// hold f.mu.
func nextErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}

func (f *fakeControlClient) CreateAtespace(ctx context.Context, in *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	if err := f.begin(ctx, "CreateAtespace", ""); err != nil {
		return nil, err
	}
	return &ateapipb.Atespace{}, nil
}

func (f *fakeControlClient) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "CreateActor")
	f.createRequests = append(f.createRequests, in)
	fn := f.createActorFn
	f.mu.Unlock()
	if fn != nil {
		actorName := ""
		if in != nil && in.Actor != nil && in.Actor.Metadata != nil {
			actorName = in.Actor.Metadata.Name
		}
		if err := fn(actorName); err != nil {
			return nil, err
		}
	}
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if err := f.begin(ctx, "ResumeActor", in.GetActor().GetName()); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := nextErr(&f.resumeErrs); err != nil {
		return nil, err
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

func (f *fakeControlClient) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	if err := f.begin(ctx, "SuspendActor", in.GetActor().GetName()); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := nextErr(&f.suspendErrs); err != nil {
		return nil, err
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControlClient) PauseActor(ctx context.Context, in *ateapipb.PauseActorRequest, opts ...grpc.CallOption) (*ateapipb.PauseActorResponse, error) {
	if err := f.begin(ctx, "PauseActor", in.GetActor().GetName()); err != nil {
		return nil, err
	}
	return &ateapipb.PauseActorResponse{}, nil
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	f.deleteRequests = append(f.deleteRequests, in)
	f.mu.Unlock()
	if err := f.begin(ctx, "DeleteActor", in.GetActor().GetName()); err != nil {
		return nil, err
	}
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeControlClient) recordedDeleteRequests() []*ateapipb.DeleteActorRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*ateapipb.DeleteActorRequest(nil), f.deleteRequests...)
}

func (f *fakeControlClient) recordedCreateRequests() []*ateapipb.CreateActorRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*ateapipb.CreateActorRequest(nil), f.createRequests...)
}

// metricRows sums one locust metric family's rows booked under userClass,
// keyed "<name>/<status>": request counts for locust_requests_total, and
// latency sums in ms for locust_request_duration_milliseconds.
func metricRows(t *testing.T, family, userClass string) map[string]float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	rows := map[string]float64{}
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["user_class"] != userClass {
				continue
			}
			v := m.GetCounter().GetValue()
			if h := m.GetHistogram(); h != nil {
				v = h.GetSampleSum()
			}
			rows[labels["name"]+"/"+labels["status"]] += v
		}
	}
	return rows
}

// requestRows returns locust_requests_total for each of userClass's rows.
func requestRows(t *testing.T, userClass string) map[string]float64 {
	t.Helper()
	return metricRows(t, "locust_requests_total", userClass)
}

// rowsGrown returns the rows that changed from before to after, and by how
// much.
func rowsGrown(before, after map[string]float64) map[string]float64 {
	grew := map[string]float64{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			grew[k] = d
		}
	}
	return grew
}

// rowCounter snapshots userClass's request rows and returns a func that
// reports how much one "<name>/<status>" row has grown since.
func rowCounter(t *testing.T, userClass string) func(row string) float64 {
	t.Helper()
	before := requestRows(t, userClass)
	return func(row string) float64 {
		return requestRows(t, userClass)[row] - before[row]
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
		cfg.APIStub = &fakeControlClient{}
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
