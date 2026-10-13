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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/hardware"
	"google.golang.org/grpc/codes"
)

// devRootWith returns a device root holding a character device (a symlink to
// /dev/null) under each name, since a test cannot mknod the real devices.
func devRootWith(t *testing.T, names ...string) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range names {
		if err := os.Symlink("/dev/null", filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestDetectHypervisor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		devices []string
		want    hypervisor
		wantErr bool
	}{
		{name: "kvm", devices: []string{"kvm"}, want: hypervisorKVM},
		{name: "mshv", devices: []string{"mshv"}, want: hypervisorMSHV},
		{name: "none", wantErr: true},
		// Granting both is a misconfigured pod; guessing could stamp snapshots
		// with the wrong backend.
		{name: "both", devices: []string{"kvm", "mshv"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := detectHypervisor(devRootWith(t, tc.devices...))
			if (err != nil) != tc.wantErr {
				t.Fatalf("detectHypervisor() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("detectHypervisor() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDetectHypervisorRejectsNonDevice(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "mshv"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := detectHypervisor(root); err == nil {
		t.Error("detectHypervisor() accepted a regular file named mshv")
	}
}

func TestSnapshotHypervisorRoundTrip(t *testing.T) {
	for _, h := range []hypervisor{hypervisorKVM, hypervisorMSHV} {
		t.Run(string(h), func(t *testing.T) {
			dir := t.TempDir()
			// A stale marker in a reused dir must not survive a KVM snapshot.
			if err := os.WriteFile(filepath.Join(dir, hypervisorFile), []byte(hypervisorMSHV), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := writeSnapshotHypervisor(dir, h); err != nil {
				t.Fatalf("writeSnapshotHypervisor: %v", err)
			}
			got, err := snapshotHypervisor(dir)
			if err != nil {
				t.Fatalf("snapshotHypervisor: %v", err)
			}
			if got != h {
				t.Errorf("snapshotHypervisor() = %q, want %q", got, h)
			}
			if err := validateSnapshotHypervisor(dir, h); err != nil {
				t.Errorf("validateSnapshotHypervisor(matching): %v", err)
			}
		})
	}
}

// KVM is the default backend, so a snapshot without a marker is a KVM one.
func TestSnapshotHypervisorAbsentMarkerIsKVM(t *testing.T) {
	dir := t.TempDir()
	if err := validateSnapshotHypervisor(dir, hypervisorKVM); err != nil {
		t.Errorf("validateSnapshotHypervisor(kvm): %v", err)
	}
	if err := validateSnapshotHypervisor(dir, hypervisorMSHV); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("validateSnapshotHypervisor(mshv) code = %v, want %v (err: %v)", apierror.Code(err), codes.FailedPrecondition, err)
	}
}

func TestValidateSnapshotHypervisorFailedPrecondition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		marker string
		want   hypervisor
	}{
		{name: "mshv snapshot on kvm", marker: "mshv", want: hypervisorKVM},
		{name: "unknown marker", marker: "xen", want: hypervisorMSHV},
		{name: "empty marker", marker: "", want: hypervisorMSHV},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, hypervisorFile), []byte(tc.marker), 0o600); err != nil {
				t.Fatal(err)
			}
			err := validateSnapshotHypervisor(dir, tc.want)
			if apierror.Code(err) != codes.FailedPrecondition {
				t.Errorf("validateSnapshotHypervisor() code = %v, want %v (err: %v)", apierror.Code(err), codes.FailedPrecondition, err)
			}
		})
	}
}

func TestRuntimeAttributes(t *testing.T) {
	for _, h := range []hypervisor{hypervisorKVM, hypervisorMSHV} {
		attrs := runtimeAttributes(h)
		if len(attrs) != 1 || attrs[hardware.AttrHypervisor] != string(h) {
			t.Errorf("runtimeAttributes(%s) = %v, want map[%s:%s]", h, attrs, hardware.AttrHypervisor, h)
		}
	}
}

// fakeSnapshotVMM serves vm.snapshot on a unix socket and returns a client for it.
func fakeSnapshotVMM(t *testing.T) *ch.Client {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "api.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("Listen(%s): %v", sock, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/v1/vm.snapshot", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ch.NewClient(sock)
}

// snapshotVMState must leave the marker that RestoreWorkload checks: one naming
// the backend on MSHV, and none on KVM, even over a stale one in the dir.
func TestSnapshotVMStateRecordsHypervisor(t *testing.T) {
	for _, h := range []hypervisor{hypervisorKVM, hypervisorMSHV} {
		t.Run(string(h), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, hypervisorFile), []byte("stale"), 0o600); err != nil {
				t.Fatal(err)
			}
			s := &AteWorkerService{hypervisor: h}
			if _, err := s.snapshotVMState(t.Context(), fakeSnapshotVMM(t), nil, "actor-1", dir); err != nil {
				t.Fatalf("snapshotVMState: %v", err)
			}
			if err := validateSnapshotHypervisor(dir, h); err != nil {
				t.Errorf("validateSnapshotHypervisor(%s) after snapshot on %s: %v", h, h, err)
			}
			other := hypervisorMSHV
			if h == hypervisorMSHV {
				other = hypervisorKVM
			}
			if err := validateSnapshotHypervisor(dir, other); apierror.Code(err) != codes.FailedPrecondition {
				t.Errorf("validateSnapshotHypervisor(%s) after snapshot on %s code = %v, want %v", other, h, apierror.Code(err), codes.FailedPrecondition)
			}
		})
	}
}
