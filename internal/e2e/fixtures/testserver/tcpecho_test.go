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
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func serveLocal(t *testing.T, banner string) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	go func() {
		defer close(done)
		if err := serveTCPEcho(listener, banner); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Logf("serving: %v", err)
		}
	}()
	return listener.Addr().String()
}

func readAvailable(t *testing.T, conn net.Conn, within time.Duration) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(within)); err != nil {
		t.Fatalf("setting read deadline: %v", err)
	}
	buffer := make([]byte, 512)
	n, err := conn.Read(buffer)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
		t.Fatalf("reading: %v", err)
	}
	return string(buffer[:n])
}

func TestTCPEcho(t *testing.T) {
	tests := []struct {
		name       string
		banner     string
		bannerWait time.Duration
	}{
		{name: "with banner", banner: "TESTBANNER/1.0\r\n", bannerWait: 5 * time.Second},
		{name: "without banner", banner: "", bannerWait: 50 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := serveLocal(t, tt.banner)
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatalf("dialing %s: %v", addr, err)
			}
			defer conn.Close()

			if got := readAvailable(t, conn, tt.bannerWait); got != tt.banner {
				t.Fatalf("banner = %q, want %q", got, tt.banner)
			}
			if _, err := io.WriteString(conn, "ping"); err != nil {
				t.Fatalf("writing: %v", err)
			}
			if got := readAvailable(t, conn, 5*time.Second); got != "ping" {
				t.Fatalf("echo = %q, want %q", got, "ping")
			}
		})
	}
}
