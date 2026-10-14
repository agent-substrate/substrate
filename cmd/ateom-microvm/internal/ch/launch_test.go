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

package ch

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// When set, the test binary acts as a cloud-hypervisor that serves vmm.ping on the
// listening socket it inherited as fd 3 (LaunchVMM's --api-socket fd=3).
const fakeVMMEnv = "ATE_CH_TEST_FAKE_VMM"

func TestMain(m *testing.M) {
	if os.Getenv(fakeVMMEnv) == "1" {
		runFakeVMM()
		return
	}
	os.Exit(m.Run())
}

func runFakeVMM() {
	lis, err := net.FileListener(os.NewFile(3, "api-socket"))
	if err != nil {
		os.Exit(2)
	}
	_ = http.Serve(lis, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"build_version":"v53.0","version":"53.0.0"}`))
	}))
}

func TestListenUnixFileAcceptsBeforeServing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api.sock")
	f, err := listenUnixFile(path)
	if err != nil {
		t.Fatalf("listenUnixFile: %v", err)
	}
	defer f.Close()
	// Nothing calls accept, yet a connect succeeds: it waits in the backlog.
	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatalf("dial before serving: %v", err)
	}
	c.Close()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("socket file should outlive the listener handoff: %v", err)
	}
}

func TestWaitReadyBoundsPingOnSilentSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api.sock")
	f, err := listenUnixFile(path)
	if err != nil {
		t.Fatalf("listenUnixFile: %v", err)
	}
	defer f.Close()
	start := time.Now()
	if _, err := NewClient(path).WaitReady(context.Background(), 100*time.Millisecond); err == nil {
		t.Fatal("WaitReady returned nil for a socket nobody serves")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("WaitReady took %s, want it bounded by its 100ms deadline", d)
	}
}

func TestLaunchVMMPassesListeningSocket(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(fakeVMMEnv, "1")
	sock := filepath.Join(t.TempDir(), "api.sock")
	cmd, client, err := LaunchVMM(context.Background(), LaunchVMMOptions{Binary: exe, APISocket: sock})
	if err != nil {
		t.Fatalf("LaunchVMM: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})
	if got := cmd.Args[1:]; len(got) != 2 || got[0] != "--api-socket" || got[1] != "fd=3" {
		t.Errorf("VMM args = %q, want [--api-socket fd=3]", got)
	}
	if got := client.Info().Version; got != "53.0.0" {
		t.Errorf("Info().Version = %q, want 53.0.0 from the VMM's vmm.ping", got)
	}
}
