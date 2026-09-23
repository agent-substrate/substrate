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

package ateomnet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSandboxResolvConf(t *testing.T) {
	// The shape kubelet writes into a pod.
	pod := "nameserver 10.96.0.10\n" +
		"search ate-demo.svc.cluster.local svc.cluster.local cluster.local\n" +
		"options ndots:5\n"

	got := string(SandboxResolvConf([]byte(pod)))

	want := "nameserver 169.254.17.1\n" +
		"search ate-demo.svc.cluster.local svc.cluster.local cluster.local\n" +
		"options ndots:5\n"
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("actor resolv.conf mismatch (-want +got):\n%s", diff)
	}
}

// Short service names depend on the search list and on ndots, so an actor given
// only a nameserver line resolves public names but not cluster ones.
func TestSandboxResolvConfKeepsSearchAndOptions(t *testing.T) {
	pod := "search svc.cluster.local\nnameserver 10.96.0.10\nnameserver 10.96.0.11\noptions ndots:5 timeout:1\n"
	got := string(SandboxResolvConf([]byte(pod)))

	if strings.Contains(got, "10.96.0.10") || strings.Contains(got, "10.96.0.11") {
		t.Errorf("a pod resolver survived into the actor's file, so its DNS would bypass atunnel:\n%s", got)
	}
	if !strings.Contains(got, "search svc.cluster.local") {
		t.Errorf("search list lost; cluster short names would stop resolving:\n%s", got)
	}
	if !strings.Contains(got, "options ndots:5 timeout:1") {
		t.Errorf("options lost:\n%s", got)
	}
	if n := strings.Count(got, "nameserver"); n != 1 {
		t.Errorf("got %d nameserver lines, want exactly the gateway", n)
	}
}

func TestWriteRootfsResolvConf(t *testing.T) {
	rootfs := t.TempDir()
	if err := WriteRootfsResolvConf(rootfs, []byte("nameserver 169.254.17.1\n")); err != nil {
		t.Fatalf("WriteRootfsResolvConf: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(rootfs, "etc", "resolv.conf"))
	if err != nil {
		t.Fatalf("reading what was written: %v", err)
	}
	if string(got) != "nameserver 169.254.17.1\n" {
		t.Errorf("wrote %q", got)
	}

	// Replacing an existing file is the ordinary case: the image usually ships one.
	if err := WriteRootfsResolvConf(rootfs, []byte("nameserver 169.254.17.1\nsearch x\n")); err != nil {
		t.Fatalf("rewriting: %v", err)
	}

	if err := WriteRootfsResolvConf(rootfs, nil); err == nil {
		t.Error("an empty resolv.conf was accepted; the actor would resolve nothing")
	}
}

// The rootfs comes from an untrusted image, so a planted symlink must not be
// followed out of it and clobber the worker pod's own file.
func TestWriteRootfsResolvConfDoesNotFollowAPlantedSymlink(t *testing.T) {
	rootfs := t.TempDir()
	outside := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(outside, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(rootfs, "etc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(rootfs, "etc", "resolv.conf")); err != nil {
		t.Fatal(err)
	}

	if err := WriteRootfsResolvConf(rootfs, []byte("nameserver 169.254.17.1\n")); err != nil {
		t.Fatalf("WriteRootfsResolvConf: %v", err)
	}
	victim, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if string(victim) != "original" {
		t.Errorf("the symlink was followed and the file outside the rootfs was overwritten with %q", victim)
	}
}
