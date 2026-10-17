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

package router

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
)

// TestDialLoopback checks that dialLoopback reaches a listener on either
// loopback through "localhost", and dials any other host as given.
func TestDialLoopback(t *testing.T) {
	for _, tc := range []struct{ network, listen, dialHost string }{
		{"tcp4", "127.0.0.1:0", "localhost"},
		{"tcp6", "[::1]:0", "localhost"},
		{"tcp4", "127.0.0.1:0", "127.0.0.1"},
		{"tcp6", "[::1]:0", "::1"},
	} {
		t.Run(tc.network+"/"+tc.dialHost, func(t *testing.T) {
			ln, err := net.Listen(tc.network, tc.listen)
			if err != nil {
				t.Skipf("no %s loopback on this host: %v", tc.network, err)
			}
			defer ln.Close()
			accepted := make(chan net.Addr, 1)
			go func() {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				accepted <- c.LocalAddr()
				c.Close()
			}()

			port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
			conn, err := dialLoopback(&net.Dialer{})(context.Background(), "tcp", net.JoinHostPort(tc.dialHost, port))
			if err != nil {
				t.Fatalf("dial %s:%s: %v", tc.dialHost, port, err)
			}
			conn.Close()
			if got := <-accepted; got.String() != ln.Addr().String() {
				t.Errorf("connection accepted on %s, want %s", got, ln.Addr())
			}
		})
	}
}

// TestDialLoopbackNothingListening checks that a localhost dial with no
// listener on either loopback fails, naming both addresses it tried.
func TestDialLoopbackNothingListening(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()

	_, err = dialLoopback(&net.Dialer{})(context.Background(), "tcp", net.JoinHostPort("localhost", port))
	if err == nil {
		t.Fatal("dial succeeded with nothing listening")
	}
	for _, ip := range loopbackIPs {
		if want := net.JoinHostPort(ip, port); !strings.Contains(err.Error(), want) {
			t.Errorf("dial error %q does not mention %s", err, want)
		}
	}
}
