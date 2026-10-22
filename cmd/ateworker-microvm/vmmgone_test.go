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
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/child"
)

func TestVMMGone(t *testing.T) {
	tests := []struct {
		name string
		// proc is the VMM process: "running", "exited", or "" for none.
		proc string
		// sock is the api-socket: "listening", "stale" (a socket file with
		// no listener), or "" for none.
		sock     string
		wantGone bool
	}{
		{name: "a running VMM that listens", proc: "running", sock: "listening"},
		{name: "a listening socket without a process", sock: "listening"},
		{name: "an exited VMM", proc: "exited", sock: "listening", wantGone: true},
		{name: "a running VMM whose socket has no listener", proc: "running", sock: "stale", wantGone: true},
		{name: "a running VMM without its socket", proc: "running", wantGone: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A short path: unix socket paths are limited to 108 bytes.
			dir, err := os.MkdirTemp("", "vmm")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			sock := filepath.Join(dir, "api.sock")
			switch tc.sock {
			case "listening":
				l, err := net.Listen("unix", sock)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = l.Close() })
			case "stale":
				l, err := net.Listen("unix", sock)
				if err != nil {
					t.Fatal(err)
				}
				l.(*net.UnixListener).SetUnlinkOnClose(false)
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
			}
			err = vmmGone(t.Context(), startProcess(t, tc.proc), sock)
			if got := errors.Is(err, errVMMGone); got != tc.wantGone {
				t.Errorf("vmmGone() = %v, want gone %v", err, tc.wantGone)
			}
		})
	}
}

// TestVMMProbe checks that vmmProbe's API calls report a gone VMM, which makes
// a failed freeze fail the checkpoint instead of aborting it.
func TestVMMProbe(t *testing.T) {
	tests := []struct {
		name string
		// proc is the VMM process, as in TestVMMGone.
		proc string
		// sock is the api-socket: "ok" (answers vmm.ping and a paused
		// vm.info), "failing" (answers every call with status 500), or
		// "stale" (a socket file with no listener).
		sock       string
		wantErr    bool
		wantGone   bool
		wantPaused bool
	}{
		{name: "a VMM that answers", proc: "running", sock: "ok", wantPaused: true},
		{name: "a running VMM whose API fails", proc: "running", sock: "failing", wantErr: true},
		{name: "a running VMM whose socket has no listener", proc: "running", sock: "stale", wantErr: true, wantGone: true},
		{name: "an exited VMM whose socket still answers", proc: "exited", sock: "failing", wantErr: true, wantGone: true},
		{name: "a stale socket without a process", sock: "stale", wantErr: true, wantGone: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "vmm")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			sock := filepath.Join(dir, "api.sock")
			l, err := net.Listen("unix", sock)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.sock {
			case "ok", "failing":
				srv := &http.Server{Handler: fakeVMMAPI(tc.sock == "failing")}
				go func() { _ = srv.Serve(l) }()
				t.Cleanup(func() { _ = srv.Close() })
			case "stale":
				l.(*net.UnixListener).SetUnlinkOnClose(false)
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unknown socket state %q", tc.sock)
			}
			probe := vmmProbe{client: ch.NewClient(sock), proc: startProcess(t, tc.proc), apiSocket: sock, readyTimeout: 50 * time.Millisecond}

			err = probe.ready(t.Context())
			if (err != nil) != tc.wantErr || errors.Is(err, errVMMGone) != tc.wantGone {
				t.Errorf("ready() = %v, want error %v, gone %v", err, tc.wantErr, tc.wantGone)
			}
			paused, err := probe.paused(t.Context())
			if (err != nil) != tc.wantErr || errors.Is(err, errVMMGone) != tc.wantGone {
				t.Errorf("paused() error = %v, want error %v, gone %v", err, tc.wantErr, tc.wantGone)
			}
			if paused != tc.wantPaused {
				t.Errorf("paused() = %v, want %v", paused, tc.wantPaused)
			}
		})
	}
}

// TestVMMProbeReadyBoundsAnUnansweredPing checks that readyTimeout bounds
// ready against a running VMM that accepts the connection but never answers,
// and that such a VMM, only slow, is not reported gone.
func TestVMMProbeReadyBoundsAnUnansweredPing(t *testing.T) {
	dir, err := os.MkdirTemp("", "vmm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "api.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	probe := vmmProbe{client: ch.NewClient(sock), proc: startProcess(t, "running"), apiSocket: sock, readyTimeout: 100 * time.Millisecond}

	start := time.Now()
	err = probe.ready(t.Context())
	if err == nil || errors.Is(err, errVMMGone) {
		t.Errorf("ready() = %v, want an error that is not errVMMGone", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("ready() returned after %s, want about its 100ms readyTimeout", elapsed)
	}
}

// fakeVMMAPI serves the Cloud Hypervisor API calls vmmProbe makes, for a
// paused VM, or fails every call with status 500.
func fakeVMMAPI(failing bool) http.Handler {
	mux := http.NewServeMux()
	reply := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			if failing {
				http.Error(w, "injected failure", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}
	}
	mux.HandleFunc("GET /api/v1/vmm.ping", reply(`{"version":"53.0.0"}`))
	mux.HandleFunc("GET /api/v1/vm.info", reply(`{"state":"Paused"}`))
	return mux
}

// startProcess starts a process in the state proc names (see TestVMMGone) and
// returns it, or nil when proc is empty. Cleanup stops every process it starts.
func startProcess(t *testing.T, proc string) *child.Process {
	t.Helper()
	var cmd *exec.Cmd
	switch proc {
	case "":
		return nil
	case "running":
		cmd = exec.Command("sleep", "60")
	case "exited":
		cmd = exec.Command("true")
	default:
		t.Fatalf("unknown process state %q", proc)
	}
	p, err := child.Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Stop)
	if proc == "exited" {
		select {
		case <-p.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("the process did not exit")
		}
	}
	return p
}
