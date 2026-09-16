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

//go:build linux

package sandboxd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteHostTapDANConfig(t *testing.T) {
	dir := t.TempDir()
	path, err := WriteHostTapDANConfig(dir, "sandbox-1", HostTapDANConfig{
		NetNSPath:   "/run/netns/ateom:test",
		TapName:     "tap0_kata",
		GuestMAC:    "02:a8:1e:00:00:02",
		IPAddresses: []string{"169.254.17.2/30"},
		MTU:         1500,
		Routes:      []DANRoute{{Gateway: "169.254.17.1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(dir, "sandbox-1.json") {
		t.Fatalf("path = %q", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	devices, ok := got["devices"].([]any)
	if !ok || len(devices) != 1 {
		t.Fatalf("devices = %#v", got["devices"])
	}
	device := devices[0].(map[string]any)["device"].(map[string]any)
	if device["type"] != "host-tap" || device["tap_name"] != "tap0_kata" {
		t.Fatalf("device = %#v", device)
	}
	if err := RemoveDANConfig(dir, "sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if err := RemoveDANConfig(dir, "sandbox-1"); err != nil {
		t.Fatal("remove must be idempotent:", err)
	}
}

func TestWriteHostTapDANConfigRejectsInvalidOwnership(t *testing.T) {
	base := HostTapDANConfig{
		NetNSPath:   "/run/netns/ateom:test",
		TapName:     "tap0_kata",
		GuestMAC:    "02:a8:1e:00:00:02",
		IPAddresses: []string{"169.254.17.2/30"},
		MTU:         1500,
	}
	tests := []struct {
		name   string
		dir    string
		id     string
		mutate func(*HostTapDANConfig)
	}{
		{name: "relative directory", dir: "relative", id: "sandbox", mutate: func(*HostTapDANConfig) {}},
		{name: "escaping id", dir: t.TempDir(), id: "../sandbox", mutate: func(*HostTapDANConfig) {}},
		{name: "foreign tap", dir: t.TempDir(), id: "sandbox", mutate: func(c *HostTapDANConfig) { c.TapName = "eth0" }},
		{name: "invalid mac", dir: t.TempDir(), id: "sandbox", mutate: func(c *HostTapDANConfig) { c.GuestMAC = "bad" }},
		{name: "invalid cidr", dir: t.TempDir(), id: "sandbox", mutate: func(c *HostTapDANConfig) { c.IPAddresses = []string{"bad"} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if _, err := WriteHostTapDANConfig(tc.dir, tc.id, cfg); err == nil {
				t.Fatal("invalid DAN config accepted")
			}
		})
	}
}
