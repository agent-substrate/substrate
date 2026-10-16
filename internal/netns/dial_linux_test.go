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

package netns

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestValidateDialTarget(t *testing.T) {
	for _, target := range []struct {
		network, address string
		wantErr          bool
	}{
		{"tcp", "127.0.0.1:80", false},
		{"tcp4", "127.0.0.1:80", false},
		{"tcp6", "[::1]:80", false},
		{"udp", "127.0.0.1:53", false},
		{"udp4", "127.0.0.1:53", false},
		{"udp6", "[fe80::1%eth0]:53", false},
		{"tcp", "localhost:80", true},
		{"tcp", ":80", true},
		{"tcp", "127.0.0.1", true},
		{"unix", "/tmp/socket", true},
		{"ip", "127.0.0.1:80", true},
	} {
		t.Run(target.network+"/"+target.address, func(t *testing.T) {
			err := validateDialTarget(target.network, target.address)
			if (err != nil) != target.wantErr {
				t.Errorf("validateDialTarget = %v, want error: %t", err, target.wantErr)
			}
		})
	}
	if err := validateDialTarget("unix", "/tmp/socket"); !errors.Is(err, net.UnknownNetworkError("unix")) {
		t.Errorf("unsupported network: got %v, want UnknownNetworkError", err)
	}
}

func TestDialerRejectsNonIPTargets(t *testing.T) {
	for _, target := range []struct{ network, address string }{
		{"tcp", "localhost:80"},
		{"tcp", ":80"},
		{"tcp", "127.0.0.1"},
		{"unix", "/tmp/socket"},
	} {
		if conn, err := Dialer(-1)(context.Background(), target.network, target.address); err == nil {
			_ = conn.Close()
			t.Errorf("accepted %s %s", target.network, target.address)
		}
	}
}

func TestDialerCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Dialer(-1)(ctx, "tcp", "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dial: got %v, want cancellation", err)
	}
}
