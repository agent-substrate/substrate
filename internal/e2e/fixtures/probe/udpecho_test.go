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
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestActorUDPEgress in the networking e2e suite reads Echoed=false as "the
// worker pod dropped the datagram", so a probe that reported it for its own
// reasons would manufacture a passing security assertion. These tests pin both
// answers against loopback, where nothing is dropping anything.

// callUDPEcho invokes the handler for addr and decodes its response.
func callUDPEcho(t *testing.T, addr string) udpEchoResult {
	t.Helper()
	recorder := httptest.NewRecorder()
	udpecho(recorder, httptest.NewRequest(http.MethodGet, "/udpecho?addr="+addr, nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("/udpecho for %q answered HTTP %d, want 200: a drop is a result, not an error", addr, recorder.Code)
	}
	var out udpEchoResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil {
		t.Fatalf("decoding the /udpecho response: %v (body %q)", err, recorder.Body)
	}
	return out
}

// TestUDPEchoAnswered covers the positive case, which is the e2e test's gate:
// an origin that answers must be reported as echoed.
func TestUDPEchoAnswered(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		buf := make([]byte, 512)
		for {
			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			if _, err := conn.WriteTo(buf[:n], from); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		conn.Close()
		<-stopped
	})

	got := callUDPEcho(t, conn.LocalAddr().String())
	if !got.Echoed {
		t.Errorf("Echoed = false against a live echo server after %d attempts: %s", got.Attempts, got.Error)
	}
	if got.Attempts != 1 {
		t.Errorf("Attempts = %d against a live echo server, want the first datagram to be answered", got.Attempts)
	}
	if got.Error != "" {
		t.Errorf("Error = %q on a successful echo, want it cleared", got.Error)
	}
}

// TestUDPEchoUnanswered covers what the e2e test's drop assertions actually
// observe: every attempt spent, nothing back, and an error that says why.
func TestUDPEchoUnanswered(t *testing.T) {
	// A discard address: nothing routes there, so no reply and no ICMP either.
	// Blackholed rather than refused is exactly the shape a dropped datagram
	// has, and it is what the probe must report as not echoed.
	addr := "192.0.2.1:9"

	start := time.Now()
	got := callUDPEcho(t, addr)
	if got.Echoed {
		t.Fatalf("Echoed = true for %s, which nothing answers", addr)
	}
	if got.Attempts != udpEchoAttempts {
		t.Errorf("Attempts = %d, want every one of the %d spent before giving up", got.Attempts, udpEchoAttempts)
	}
	if got.Error == "" {
		t.Error("Error is empty on a datagram that was never answered, leaving a failing e2e assertion nothing to explain itself with")
	}
	// The retries have to actually wait: a budget spent instantly would read a
	// slow-but-delivered echo as a drop.
	if elapsed := time.Since(start); elapsed < udpEchoWait {
		t.Errorf("gave up after %v, want at least one full %v wait", elapsed, udpEchoWait)
	}
}

// TestUDPEchoMissingAddr covers the caller error, which must not look like a
// drop to a test asserting one -- hence the error rather than a bare
// Echoed=false.
func TestUDPEchoMissingAddr(t *testing.T) {
	got := callUDPEcho(t, "")
	if got.Echoed {
		t.Error("Echoed = true with no addr to send to")
	}
	if got.Error == "" {
		t.Error("Error is empty with no addr to send to")
	}
	if got.Attempts != 0 {
		t.Errorf("Attempts = %d with no addr to send to, want 0", got.Attempts)
	}
}
