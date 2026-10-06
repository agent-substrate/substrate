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
	"fmt"
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
	case "snapshot":
		if err := os.Mkdir(args[2], 0o700); err != nil {
			return err
		}
		for _, name := range snapshotFiles {
			data, err := os.ReadFile(filepath.Join(args[1], name))
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(args[2], name), data, 0o600); err != nil {
				return err
			}
		}
	case "restore":
		if err := os.Mkdir(args[2], 0o700); err != nil {
			return err
		}
		for _, name := range snapshotFiles {
			data, err := os.ReadFile(filepath.Join(args[1], name))
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(args[2], name), data, 0o600); err != nil {
				return err
			}
		}
		f.states[args[2]] = vmStatus{ActorID: args[3], Phase: "stopped"}
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
			done <- errors.New("probe failed")
			close(done)
			delete(f.done, bundle)
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

type fakeVolumeStager struct {
	path                     string
	staged                   []*hostruntimepb.DurableVolume
	stageCalls, unstageCalls int
}

func (f *fakeVolumeStager) Stage(_ context.Context, _ string, volumes []*hostruntimepb.DurableVolume) ([]stagedVolume, error) {
	f.stageCalls++
	f.staged = volumes
	if len(volumes) == 0 {
		return nil, nil
	}
	return []stagedVolume{{Name: volumes[0].GetName(), MountPath: volumes[0].GetMountPath(), Tag: "ate-data", HostPath: f.path}}, nil
}

func (f *fakeVolumeStager) Unstage(context.Context, string) error {
	f.unstageCalls++
	return nil
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

func TestActivateAndPauseStageDurableVolume(t *testing.T) {
	f := newFakeRunner()
	s := testServer(t, f)
	stager := &fakeVolumeStager{path: t.TempDir()}
	s.volumes = stager
	req := request("actor-volume")
	req.DurableVolumes = []*hostruntimepb.DurableVolume{{
		Name: "data", MountPath: "/workspace", VolumeId: "volume-1", Driver: nfsCSIDriver,
		VolumeContext: map[string]string{"server": "nfs.internal", "share": "/exports", "subdir": "actor-volume"},
	}}
	if _, err := s.Activate(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if stager.stageCalls != 1 || len(stager.staged) != 1 {
		t.Fatalf("Stage calls = %d, request = %v", stager.stageCalls, stager.staged)
	}
	if _, err := os.Stat(filepath.Join(s.bundle("actor-volume"), vmVolumeConfigName)); err != nil {
		t.Fatalf("volume config: %v", err)
	}
	if _, err := s.Pause(context.Background(), &hostruntimepb.PauseRequest{ActorUid: "actor-volume", LocalSnapshotName: "pause-1"}); err != nil {
		t.Fatal(err)
	}
	if stager.unstageCalls != 1 {
		t.Fatalf("Unstage calls = %d, want 1", stager.unstageCalls)
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
	if err := os.Mkdir(s.bundle("actor"), 0o700); err != nil {
		t.Fatal(err)
	}
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

func TestReconcileProxiesPreservesEndpointPort(t *testing.T) {
	f := newFakeRunner()
	stateDir := t.TempDir()
	first := newServer("maclet", stateDir, testImage, "/source", "mac-worker.example", "127.0.0.1:0")
	first.runner, first.pollInterval = f, time.Millisecond
	activated, err := first.Activate(context.Background(), request("actor"))
	if err != nil {
		t.Fatal(err)
	}
	oldPort := activated.GetEndpoint().GetPort()
	first.mu.Lock()
	oldProxy := first.proxies["actor"]
	first.mu.Unlock()
	if err := oldProxy.listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oldProxy.server.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := newServer("maclet", stateDir, testImage, "/source", "mac-worker.example", "127.0.0.1:0")
	restarted.runner = f
	if err := restarted.ReconcileProxies(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		restarted.mu.Lock()
		proxy := restarted.proxies["actor"]
		restarted.mu.Unlock()
		_ = proxy.server.Close()
	})
	got, err := restarted.Activate(context.Background(), request("actor"))
	if err != nil {
		t.Fatal(err)
	}
	if got.GetEndpoint().GetPort() != oldPort {
		t.Fatalf("reconciled port = %d, want %d", got.GetEndpoint().GetPort(), oldPort)
	}
}

func TestReconcileProxiesIgnoresPrivateProviderState(t *testing.T) {
	s := testServer(t, newFakeRunner())
	for _, name := range []string{".receipts", ".snapshot-abandoned", ".restore-abandoned"} {
		if err := os.Mkdir(filepath.Join(s.stateDir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReconcileProxies(context.Background()); err != nil {
		t.Fatalf("ReconcileProxies: %v", err)
	}
}

func TestReconcileProxiesFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata string
		occupy   bool
		symlink  bool
	}{
		{name: "malformed metadata", metadata: `{"guestPort":"wrong","proxyPort":1234}`},
		{name: "conflicting port", occupy: true},
		{name: "metadata symlink", symlink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunner()
			stateDir := t.TempDir()
			bundle := filepath.Join(stateDir, "actor")
			if err := os.Mkdir(bundle, 0o700); err != nil {
				t.Fatal(err)
			}
			f.states[bundle] = vmStatus{ActorID: "actor", Phase: "running", Ready: true, IPAddress: "127.0.0.1"}
			metadata := tc.metadata
			var occupied net.Listener
			if tc.occupy {
				var err error
				occupied, err = net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer occupied.Close()
				port, err := listenerPort(occupied)
				if err != nil {
					t.Fatal(err)
				}
				metadata = fmt.Sprintf(`{"guestPort":8123,"proxyPort":%d}`, port)
			}
			metadataPath := filepath.Join(bundle, proxyMetadataName)
			if tc.symlink {
				target := filepath.Join(t.TempDir(), "metadata")
				if err := os.WriteFile(target, []byte(`{"guestPort":8123,"proxyPort":1234}`), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, metadataPath); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(metadataPath, []byte(metadata), 0o600); err != nil {
				t.Fatal(err)
			}
			s := newServer("maclet", stateDir, testImage, "/source", "host", "127.0.0.1:0")
			s.runner = f
			if err := s.ReconcileProxies(context.Background()); err == nil {
				t.Fatal("ReconcileProxies succeeded")
			}
		})
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
