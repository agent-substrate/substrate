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

func TestServeUDPEcho(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = serveUDPEcho(conn)
	}()
	t.Cleanup(func() {
		conn.Close()
		<-stopped
	})

	client, err := net.Dial("udp", conn.LocalAddr().String())
	if err != nil {
		t.Fatalf("dialing %s: %v", conn.LocalAddr(), err)
	}
	defer client.Close()
	if err := client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("setting read deadline: %v", err)
	}

	payload := []byte("probe-1234567890")
	if _, err := client.Write(payload); err != nil {
		t.Fatalf("sending: %v", err)
	}
	buf := make([]byte, udpEchoMaxDatagram)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("reading echo: %v", err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Errorf("echo = %q, want %q", buf[:n], payload)
	}
}
