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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
)

func TestParseVMMFeatures(t *testing.T) {
	tests := []struct {
		name   string
		out    string
		want   []string
		wantOK bool
	}{{
		// Output of the v53.0 release binary.
		name:   "release build",
		out:    "cloud-hypervisor v53.0\nMigration Protocol Versions: 0\nEnabled features: [\"io_uring\", \"kvm\", \"mshv\"]\n",
		want:   []string{"io_uring", "kvm", "mshv"},
		wantOK: true,
	}, {
		name:   "mshv-only build",
		out:    "cloud-hypervisor v53.0.0\nMigration Protocol Versions: 0\nEnabled features: [\"mshv\"]\n",
		want:   []string{"mshv"},
		wantOK: true,
	}, {
		name:   "no features compiled in",
		out:    "cloud-hypervisor v53.0\nEnabled features: []\n",
		want:   []string{},
		wantOK: true,
	}, {
		name: "no feature line",
		out:  "cloud-hypervisor v53.0\n",
	}, {
		name: "malformed list",
		out:  "Enabled features: kvm, mshv\n",
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseVMMFeatures([]byte(tc.out))
			if ok != tc.wantOK || !slices.Equal(got, tc.want) {
				t.Errorf("parseVMMFeatures() = %q, %v, want %q, %v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// fakeVMM writes a stand-in cloud-hypervisor that prints features as
// `--version -v` does and appends a line to a call log on every run.
func fakeVMM(t *testing.T, features string, exitCode int) (bin, calls string) {
	t.Helper()
	dir := t.TempDir()
	bin = filepath.Join(dir, "cloud-hypervisor")
	calls = filepath.Join(dir, "calls")
	script := "#!/bin/sh\n" +
		"echo \"$@\" >> " + calls + "\n" +
		"echo 'cloud-hypervisor v53.0'\n" +
		"echo 'Enabled features: " + features + "'\n" +
		"exit " + strconv.Itoa(exitCode) + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("WriteFile(%s): %v", bin, err)
	}
	return bin, calls
}

func TestVMMBackendsCheck(t *testing.T) {
	t.Run("build without the worker's backend is a failed precondition", func(t *testing.T) {
		bin, _ := fakeVMM(t, `["mshv"]`, 0)
		var v vmmBackends
		err := v.check(t.Context(), bin, hypervisorKVM)
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("check(kvm) code = %v, want %v (err: %v)", apierror.Code(err), codes.FailedPrecondition, err)
		}
		if !strings.Contains(err.Error(), "without kvm support") {
			t.Errorf("check(kvm) error = %q, want it to name the missing backend", err)
		}
		if err := v.check(t.Context(), bin, hypervisorMSHV); err != nil {
			t.Errorf("check(mshv) = %v, want nil", err)
		}
	})

	t.Run("build with both backends serves either", func(t *testing.T) {
		bin, _ := fakeVMM(t, `["io_uring", "kvm", "mshv"]`, 0)
		var v vmmBackends
		for _, h := range []hypervisor{hypervisorKVM, hypervisorMSHV} {
			if err := v.check(t.Context(), bin, h); err != nil {
				t.Errorf("check(%s) = %v, want nil", h, err)
			}
		}
	})

	t.Run("features are read once per binary", func(t *testing.T) {
		bin, calls := fakeVMM(t, `["kvm"]`, 0)
		var v vmmBackends
		for range 3 {
			if err := v.check(t.Context(), bin, hypervisorKVM); err != nil {
				t.Fatalf("check(kvm) = %v, want nil", err)
			}
		}
		data, err := os.ReadFile(calls)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", calls, err)
		}
		if got := strings.Split(strings.TrimSpace(string(data)), "\n"); len(got) != 1 || got[0] != "--version -v" {
			t.Errorf("binary ran with %q, want once with \"--version -v\"", got)
		}
	})

	t.Run("actors sharing a build read it once", func(t *testing.T) {
		bin, calls := fakeVMM(t, `["kvm"]`, 0)
		var v vmmBackends
		// Each actor's sandbox assets dir hard-links the cached build.
		for i := range 3 {
			link := filepath.Join(t.TempDir(), "cloud-hypervisor")
			if err := os.Link(bin, link); err != nil {
				t.Fatalf("Link(%s, %s): %v", bin, link, err)
			}
			if err := v.check(t.Context(), link, hypervisorKVM); err != nil {
				t.Fatalf("check(actor %d) = %v, want nil", i, err)
			}
		}
		if n := callCount(t, calls); n != 1 {
			t.Errorf("binary ran %d times, want once for one build", n)
		}
	})

	t.Run("a different build staged at the same path is read again", func(t *testing.T) {
		mshvOnly, _ := fakeVMM(t, `["mshv"]`, 0)
		both, _ := fakeVMM(t, `["kvm", "mshv"]`, 0)
		bin := filepath.Join(t.TempDir(), "cloud-hypervisor")
		var v vmmBackends
		if err := os.Link(mshvOnly, bin); err != nil {
			t.Fatal(err)
		}
		if err := v.check(t.Context(), bin, hypervisorKVM); apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("check(kvm) on the mshv-only build = %v, want FailedPrecondition", err)
		}
		// A later restore of the same actor stages another build at the
		// same path.
		if err := os.Remove(bin); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(both, bin); err != nil {
			t.Fatal(err)
		}
		if err := v.check(t.Context(), bin, hypervisorKVM); err != nil {
			t.Errorf("check(kvm) after restaging a build with kvm = %v, want nil", err)
		}
	})

	t.Run("bare name runs the file lookPath resolves", func(t *testing.T) {
		bin, calls := fakeVMM(t, `["mshv"]`, 0)
		var looked []string
		v := vmmBackends{lookPath: func(name string) (string, error) {
			looked = append(looked, name)
			return bin, nil
		}}
		if err := v.check(t.Context(), "cloud-hypervisor", hypervisorKVM); apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("check(kvm) = %v, want FailedPrecondition", err)
		}
		if !slices.Equal(looked, []string{"cloud-hypervisor"}) {
			t.Errorf("lookPath called with %q, want [cloud-hypervisor]", looked)
		}
		if n := callCount(t, calls); n != 1 {
			t.Errorf("binary ran %d times, want 1", n)
		}
	})

	t.Run("unresolvable name passes", func(t *testing.T) {
		v := vmmBackends{lookPath: func(string) (string, error) { return "", exec.ErrNotFound }}
		if err := v.check(t.Context(), "cloud-hypervisor", hypervisorKVM); err != nil {
			t.Errorf("check(kvm) = %v, want nil when the binary is not found", err)
		}
	})

	t.Run("unreadable features pass", func(t *testing.T) {
		bin, calls := fakeVMM(t, `["mshv"]`, 1)
		var v vmmBackends
		for range 2 {
			if err := v.check(t.Context(), bin, hypervisorKVM); err != nil {
				t.Errorf("check(kvm) = %v, want nil when the binary fails", err)
			}
		}
		data, err := os.ReadFile(calls)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", calls, err)
		}
		if n := strings.Count(string(data), "\n"); n != 2 {
			t.Errorf("binary ran %d times, want 2 (a failed read is not cached)", n)
		}
		if err := v.check(t.Context(), filepath.Join(t.TempDir(), "missing"), hypervisorKVM); err != nil {
			t.Errorf("check(missing binary) = %v, want nil", err)
		}
	})
}

// callCount returns how many times a fakeVMM binary ran.
func callCount(t *testing.T, calls string) int {
	t.Helper()
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", calls, err)
	}
	return strings.Count(string(data), "\n")
}
