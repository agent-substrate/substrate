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
	"bytes"
	"net"
	"testing"
	"time"
)

// The UDP-egress e2e assertion reads a missing echo as a dropped datagram, so
// an origin that quietly fails to answer would read as a policy that is working.
// These tests pin the answering against a loopback listener, where no sandbox
// and no nftables rule are involved.

// serveLocalEcho starts the echo on loopback and returns its address.
func serveLocalEcho(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		// Closing conn is how the cleanup below ends the loop, so the error
		// this returns is the expected one and says nothing.
		_ = serveUDPEcho(conn)
	}()
	// Joining the goroutine, not just closing under it, so no echo server
	// outlives the test that started it.
	t.Cleanup(func() {
		conn.Close()
		<-stopped
	})
	return conn.LocalAddr().String()
}

// exchange sends payload to addr and returns the reply, or "" if none arrived
// before the deadline.
func exchange(t *testing.T, addr string, payload []byte) []byte {
	t.Helper()
	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dialing %s: %v", addr, err)
	}
	defer conn.Close()
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("sending to %s: %v", addr, err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("setting the read deadline: %v", err)
	}
	buf := make([]byte, udpEchoMaxDatagram)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("waiting for the echo from %s: %v", addr, err)
	}
	return buf[:n]
}

func TestServeUDPEcho(t *testing.T) {
	addr := serveLocalEcho(t)

	// Byte-for-byte, because the probe matches the echo against the exact
	// token it sent: anything else is reported as no echo at all.
	payload := []byte("probe-1234567890")
	if got := exchange(t, addr, payload); !bytes.Equal(got, payload) {
		t.Errorf("echo = %q, want %q", got, payload)
	}

	// The e2e probe retries within its budget, so the server has to answer
	// more than the first datagram it ever sees.
	second := []byte("probe-2345678901")
	if got := exchange(t, addr, second); !bytes.Equal(got, second) {
		t.Errorf("echo of a second datagram = %q, want %q", got, second)
	}
}

// TestServeUDPEchoEmptyDatagram covers the degenerate payload: a zero-length
// datagram is a datagram, and answering it keeps "nothing came back" meaning
// only that nothing came back.
func TestServeUDPEchoEmptyDatagram(t *testing.T) {
	addr := serveLocalEcho(t)
	if got := exchange(t, addr, nil); len(got) != 0 {
		t.Errorf("echo of an empty datagram = %q, want empty", got)
	}
}
