//go:build linux

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
	"fmt"
	"io"
	"sync"
	"syscall"
	"testing"

	"go.ate.dev/substrate/cmd/ateworker-kata/internal/shim"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

// fakeShim stands in for a shim. The VM it runs ends when a signal in endOn is
// sent to any container, or when end is called.
type fakeShim struct {
	endOn map[syscall.Signal]bool

	mu    sync.Mutex
	calls []string

	vmEnd     chan struct{}
	endOnce   sync.Once
	exited    chan struct{}
	exitOnce  sync.Once
	killErrOn map[syscall.Signal]error
}

func newFakeShim(endOn ...syscall.Signal) *fakeShim {
	f := &fakeShim{endOn: map[syscall.Signal]bool{}, vmEnd: make(chan struct{}), exited: make(chan struct{})}
	for _, sig := range endOn {
		f.endOn[sig] = true
	}
	return f
}

func (f *fakeShim) record(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf(format, args...))
}

func (f *fakeShim) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// end ends the VM, as when it dies.
func (f *fakeShim) end() { f.endOnce.Do(func() { close(f.vmEnd) }) }

func (f *fakeShim) Create(_ context.Context, req shim.CreateRequest) (uint32, error) {
	f.record("Create %s", req.ID)
	return 1, nil
}

func (f *fakeShim) StartContainer(_ context.Context, id string) error {
	f.record("Start %s", id)
	return nil
}

func (f *fakeShim) Wait(ctx context.Context, id, _ string) (uint32, error) {
	f.record("Wait %s", id)
	select {
	case <-f.vmEnd:
		return 137, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *fakeShim) Kill(_ context.Context, id, _ string, sig syscall.Signal) error {
	f.record("Kill %s %d", id, sig)
	if err := f.killErrOn[sig]; err != nil {
		return err
	}
	if f.endOn[sig] {
		f.end()
	}
	return nil
}

func (f *fakeShim) Delete(_ context.Context, id, _ string) error {
	f.record("Delete %s", id)
	return nil
}

func (f *fakeShim) Shutdown(_ context.Context, id string, now bool) error {
	f.record("Shutdown %s now=%v", id, now)
	f.exitOnce.Do(func() { close(f.exited) })
	return nil
}

func (f *fakeShim) Exited() <-chan struct{} { return f.exited }

func (f *fakeShim) Close() error {
	f.record("Close")
	return nil
}

var _ shimClient = (*fakeShim)(nil)

// fakeTunnel records what the service asks of the tunnel, and builds no
// network.
type fakeTunnel struct {
	mu          sync.Mutex
	deactivated []string
	ingress     map[string]ingress.DialFunc
	sessions    []*actornet.SandboxSession
	netErr      error
}

func (f *fakeTunnel) PrepareEgress(context.Context, resources.ActorAttribution, *ateworkerpb.EgressGateway) (*atunnel.ActorEgress, error) {
	return nil, nil
}

func (f *fakeTunnel) ActivateEgress(resources.ActorAttribution, *atunnel.ActorEgress) error {
	return nil
}

func (f *fakeTunnel) ActivateIngress(a resources.ActorAttribution, dial ingress.DialFunc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ingress == nil {
		f.ingress = map[string]ingress.DialFunc{}
	}
	f.ingress[a.UID] = dial
	return nil
}

func (f *fakeTunnel) Deactivate(_ context.Context, a resources.ActorAttribution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deactivated = append(f.deactivated, a.UID)
	return nil
}

func (f *fakeTunnel) ServeNetwork(context.Context, resources.ActorAttribution) (*actornet.SandboxSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.netErr != nil {
		return nil, f.netErr
	}
	s := &actornet.SandboxSession{}
	f.sessions = append(f.sessions, s)
	return s, nil
}

var _ tunnel = (*fakeTunnel)(nil)

// fakeCrashReporter records the crashes it is told of.
type fakeCrashReporter struct {
	reports chan resources.ActorAttribution
}

func newFakeCrashReporter() *fakeCrashReporter {
	return &fakeCrashReporter{reports: make(chan resources.ActorAttribution, 4)}
}

func (f *fakeCrashReporter) ReportCrashed(_ context.Context, a resources.ActorAttribution, _ string) error {
	f.reports <- a
	return nil
}

const testUID = "0b1a6a3c-7d5e-4f8a-9c2d-3e4f5a6b7c8d"

func testAttribution(uid string) resources.ActorAttribution {
	return resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "team", Name: "agent"}, UID: uid}
}

// newTestService returns a service with a fake tunnel, and the temp directory
// its stats are read from.
func newTestService(t *testing.T, maxActors int) (*AteWorkerService, *fakeTunnel) {
	t.Helper()
	tun := &fakeTunnel{}
	s := NewService(tun, actorlog.NewActorLogger(io.Discard, false), "/bin/true", maxActors)
	s.cgroupRoot = t.TempDir()
	s.epoch.Store(7)
	return s, tun
}
