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

// The egress suite reads whether this origin spoke first as evidence about the
// tunnel, so getting it wrong here would be read as a broken gateway. These
// tests pin the two orders against a loopback listener, with no gateway
// involved.

// serveLocal starts the echo origin on loopback and returns its address.
func serveLocal(t *testing.T, banner string) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		if err := serveTCPEcho(listener, banner); err != nil {
			t.Logf("serving: %v", err)
		}
	}()
	return listener.Addr().String()
}

// dialLocalEcho connects to address and closes the connection with the test.
func dialLocalEcho(t *testing.T, address string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatalf("dialing %s: %v", address, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("setting deadline: %v", err)
	}
	return conn
}

// readAvailable returns the bytes of a single read, treating a timeout as an
// empty answer rather than a failure: silence is what the quiet origin is
// supposed to produce.
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

// TestTCPEchoGreetsBeforeReading is the server-speaks-first order: the banner
// has to arrive without the peer having written anything.
func TestTCPEchoGreetsBeforeReading(t *testing.T) {
	const banner = "TESTBANNER/1.0\r\n"
	conn := dialLocalEcho(t, serveLocal(t, banner))

	if got := readAvailable(t, conn, 5*time.Second); got != banner {
		t.Fatalf("banner = %q, want %q", got, banner)
	}

	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("writing: %v", err)
	}
	if got := readAvailable(t, conn, 5*time.Second); got != "ping" {
		t.Fatalf("echo = %q, want %q", got, "ping")
	}
}

// TestTCPEchoStaysSilentWithoutBanner is the client-speaks-first order. An
// origin that greeted anyway would make the other subtest pass for the wrong
// reason, so the silence is asserted rather than assumed.
func TestTCPEchoStaysSilentWithoutBanner(t *testing.T) {
	conn := dialLocalEcho(t, serveLocal(t, ""))

	if got := readAvailable(t, conn, 250*time.Millisecond); got != "" {
		t.Fatalf("origin volunteered %q before being spoken to, want silence", got)
	}

	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("writing: %v", err)
	}
	if got := readAvailable(t, conn, 5*time.Second); got != "ping" {
		t.Fatalf("echo = %q, want %q", got, "ping")
	}
}

// TestTCPEchoServesConcurrentPeers covers the accept loop handing each
// connection to its own goroutine: a serial one would leave the second peer
// waiting on the first, which reads as a stalled tunnel.
func TestTCPEchoServesConcurrentPeers(t *testing.T) {
	const banner = "HELLO\r\n"
	address := serveLocal(t, banner)

	first := dialLocalEcho(t, address)
	second := dialLocalEcho(t, address)

	for i, conn := range []net.Conn{first, second} {
		if got := readAvailable(t, conn, 5*time.Second); got != banner {
			t.Fatalf("banner on connection %d = %q, want %q", i, got, banner)
		}
	}

	if _, err := io.WriteString(first, "ping"); err != nil {
		t.Fatalf("writing: %v", err)
	}
	if got := readAvailable(t, first, 5*time.Second); got != "ping" {
		t.Fatalf("echo = %q, want %q", got, "ping")
	}
}

// TestTCPEchoListenFailure covers the subcommand reporting a bad --listen
// rather than serving nothing: a pod that came up and never listened would fail
// its readiness probe with nothing in the log to say why.
func TestTCPEchoListenFailure(t *testing.T) {
	cmd := newTCPEchoCmd()
	cmd.SetArgs([]string{"--listen=not-an-address"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SilenceUsage = true

	if err := cmd.Execute(); err == nil {
		t.Fatal("tcpecho --listen=not-an-address returned no error, want a listen failure")
	}
}
