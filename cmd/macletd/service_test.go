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
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testImage = "registry.example/mac@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeChild struct{ done <-chan error }

func (c fakeChild) Wait() error { return <-c.done }

type fakeRunner struct {
	mu                     sync.Mutex
	states                 map[string]vmStatus
	done                   map[string]chan error
	creates, starts, stops int
	failStart, failReady   bool
	startGate              chan struct{}
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{states: make(map[string]vmStatus), done: make(map[string]chan error)}
}

func (f *fakeRunner) Run(_ context.Context, out io.Writer, _ string, args ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch args[0] {
	case "create":
		f.creates++
		if err := os.Mkdir(args[2], 0o700); err != nil {
			return err
		}
		f.states[args[2]] = vmStatus{ActorID: args[3], Phase: "stopped"}
	case "status":
		return json.NewEncoder(out).Encode(f.states[args[1]])
	case "stop":
		f.stops++
		st := f.states[args[1]]
		st.Phase = "stopped"
		st.Ready = false
		f.states[args[1]] = st
		if done := f.done[args[1]]; done != nil {
			done <- nil
			close(done)
			delete(f.done, args[1])
		}
	}
	return nil
}

func (f *fakeRunner) Start(_ io.Writer, _ string, args ...string) (childProcess, error) {
	f.mu.Lock()
	if f.failStart {
		f.mu.Unlock()
		return nil, errors.New("start failed")
	}
	f.starts++
	bundle := args[1]
	done := make(chan error, 1)
	f.done[bundle] = done
	gate := f.startGate
	f.mu.Unlock()
	go func() {
		if gate != nil {
			<-gate
		}
		f.mu.Lock()
		st := f.states[bundle]
		if f.failReady {
			st.Phase, st.Detail = "failed", "probe failed"
		} else {
			st.Phase, st.Ready, st.IPAddress = "running", true, "127.0.0.1"
		}
		f.states[bundle] = st
		f.mu.Unlock()
	}()
	return fakeChild{done: done}, nil
}

func testServer(t *testing.T, f *fakeRunner) *server {
	t.Helper()
	s := newServer("maclet", t.TempDir(), testImage, "/source", "mac-worker.example", "127.0.0.1:0")
	s.runner, s.pollInterval = f, time.Millisecond
	return s
}

func request(uid string) *hostruntimepb.ActivateRequest {
	return &hostruntimepb.ActivateRequest{ActorUid: uid, Image: testImage, ReadinessProbe: &hostruntimepb.HTTPReadinessProbe{Port: 8123, Path: "/ready", TimeoutSeconds: 2}}
}

func TestActivateValidation(t *testing.T) {
	s := testServer(t, newFakeRunner())
	for _, req := range []*hostruntimepb.ActivateRequest{nil, request("../bad"), request("ok")} {
		if req != nil && req.ActorUid == "ok" {
			req.Image = "unconfigured"
		}
		if _, err := s.Activate(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("Activate(%v) code = %v", req, status.Code(err))
		}
	}
	bad := request("ok")
	bad.ReadinessProbe.Path = "relative"
	if _, err := s.Activate(context.Background(), bad); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad probe code = %v", status.Code(err))
	}
}

func TestActivateReadyAndIdempotent(t *testing.T) {
	f, s := newFakeRunner(), (*server)(nil)
	s = testServer(t, f)
	for i := 0; i < 2; i++ {
		got, err := s.Activate(context.Background(), request("actor-1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.GetEndpoint().GetHost() != "mac-worker.example" || got.GetEndpoint().GetPort() < 1 {
			t.Fatalf("endpoint = %v", got.Endpoint)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.creates != 1 || f.starts != 1 {
		t.Fatalf("creates=%d starts=%d", f.creates, f.starts)
	}
}

func TestEndpointProxyForwardsToPrivateGuest(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()
	host, portText, err := net.SplitHostPort(strings.TrimPrefix(backend.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseInt(portText, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	s := newServer("maclet", t.TempDir(), testImage, "/source", "127.0.0.1", "127.0.0.1:0")
	got, err := s.endpoint("actor", &vmStatus{IPAddress: host, Ready: true}, int32(port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.mu.Lock()
		proxy := s.proxies["actor"]
		s.mu.Unlock()
		_ = proxy.server.Close()
	})
	resp, err := http.Get("http://" + net.JoinHostPort(got.GetEndpoint().GetHost(), strconv.Itoa(int(got.GetEndpoint().GetPort()))) + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("proxy status = %s", resp.Status)
	}
}

func TestConcurrentDistinctActors(t *testing.T) {
	f := newFakeRunner()
	f.startGate = make(chan struct{})
	s := testServer(t, f)
	errCh := make(chan error, 2)
	for _, uid := range []string{"one", "two"} {
		go func() { _, err := s.Activate(context.Background(), request(uid)); errCh <- err }()
	}
	deadline := time.Now().Add(time.Second)
	for {
		f.mu.Lock()
		starts := f.starts
		f.mu.Unlock()
		if starts == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("distinct Actors serialized")
		}
		time.Sleep(time.Millisecond)
	}
	close(f.startGate)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func TestTerminateAndRetry(t *testing.T) {
	f := newFakeRunner()
	s := testServer(t, f)
	if _, err := s.Activate(context.Background(), request("actor")); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(s.stateDir, "actor")
	for i := 0; i < 2; i++ {
		if _, err := s.Terminate(context.Background(), &hostruntimepb.TerminateRequest{ActorUid: "actor"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(bundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bundle remains: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stops != 1 {
		t.Fatalf("stops = %d", f.stops)
	}
}

func TestFailedChildAndReadiness(t *testing.T) {
	for _, tc := range []struct {
		name         string
		start, ready bool
	}{{"start", true, false}, {"readiness", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunner()
			f.failStart, f.failReady = tc.start, tc.ready
			if _, err := testServer(t, f).Activate(context.Background(), request("actor")); status.Code(err) != codes.Internal {
				t.Fatalf("code = %v, err %v", status.Code(err), err)
			}
		})
	}
}
