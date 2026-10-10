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

package dns

import (
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func startTestUDPHandler(t *testing.T, upstreams []string, configure func(*udpHandler, *dnsHandler)) net.Conn {
	t.Helper()
	ingress, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	egress, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		ingress.Close()
		t.Fatal(err)
	}

	udpAddrs := make([]*net.UDPAddr, len(upstreams))
	for i, u := range upstreams {
		addr, err := net.ResolveUDPAddr("udp", u)
		if err != nil {
			ingress.Close()
			egress.Close()
			t.Fatal(err)
		}
		udpAddrs[i] = addr
	}

	lim := newLimiter()
	pending := newPendingRequests(lim)
	dnsH := newDNSHandler(pending, lim, nil)
	h := newUDPHandler(ingress, egress, udpAddrs, dnsH, pending)
	if configure != nil {
		configure(h, dnsH)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = ingress.Close()
		_ = egress.Close()
		<-done
	})

	client, err := net.Dial("udp", ingress.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestUDPHandlerForwardsAndRestoresID(t *testing.T) {
	var seenUpstreamID atomic.Uint32
	upstream := newFakeResolver(t, func(query []byte) []byte {
		seenUpstreamID.Store(uint32(binary.BigEndian.Uint16(query[0:2])))
		return dnsAnswer(query, 0)
	})

	client := startTestUDPHandler(t, []string{upstream}, nil)
	const clientID = 0x4242
	query := dnsQuery(clientID)
	if _, err := client.Write(query); err != nil {
		t.Fatal(err)
	}
	got := readWithin(t, client)
	if gotID := binary.BigEndian.Uint16(got[0:2]); gotID != clientID {
		t.Errorf("response ID = %#x, want %#x", gotID, clientID)
	}
	if got[2]&0x80 == 0 {
		t.Error("expected response bit QR=1 to be set")
	}
}

func TestUDPHandlerSynthesizedReply(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := newFakeResolver(t, func(query []byte) []byte {
		upstreamCalls.Add(1)
		return dnsAnswer(query, 0)
	})

	client := startTestUDPHandler(t, []string{upstream}, func(_ *udpHandler, dnsH *dnsHandler) {
		dnsH.allow = func(dnsmessage.Question) bool { return false }
	})

	query := dnsQuery(0x9999)
	if _, err := client.Write(query); err != nil {
		t.Fatal(err)
	}
	got := readWithin(t, client)
	if rcode := dnsmessage.RCode(got[3] & 0x0f); rcode != dnsmessage.RCodeRefused {
		t.Errorf("synthesized rcode = %v, want %v", rcode, dnsmessage.RCodeRefused)
	}
	if upstreamCalls.Load() != 0 {
		t.Errorf("upstream was called %d times on synthesized reply, want 0", upstreamCalls.Load())
	}
}

func TestUDPHandlerFailoverOnSweepTimeoutAndDeferredDelivery(t *testing.T) {
	silent1, _ := newSilentResolver(t)
	healthy := newFakeResolver(t, func(query []byte) []byte {
		return dnsAnswer(query, 0)
	})

	client := startTestUDPHandler(t, []string{silent1, healthy}, func(h *udpHandler, _ *dnsHandler) {
		h.pending.attemptTimeout = 40 * time.Millisecond
		h.sweepInterval = 10 * time.Millisecond
	})

	if _, err := client.Write(dnsQuery(0x7777)); err != nil {
		t.Fatal(err)
	}
	got := readWithin(t, client)
	if gotID := binary.BigEndian.Uint16(got[0:2]); gotID != 0x7777 {
		t.Errorf("response ID = %#x, want 0x7777", gotID)
	}
	if rcode := got[3] & 0x0f; rcode != 0 {
		t.Errorf("rcode = %d, want 0 after timeout failover", rcode)
	}

	// Also verify that if the first upstream returns SERVFAIL and the second
	// upstream times out, the deferred SERVFAIL is delivered on expiry.
	sick := newFakeResolver(t, func(query []byte) []byte {
		return dnsAnswer(query, byte(dnsmessage.RCodeServerFailure))
	})
	silent2, _ := newSilentResolver(t)
	client2 := startTestUDPHandler(t, []string{sick, silent2}, func(h *udpHandler, _ *dnsHandler) {
		h.pending.attemptTimeout = 40 * time.Millisecond
		h.sweepInterval = 10 * time.Millisecond
	})

	if _, err := client2.Write(dnsQuery(0x8888)); err != nil {
		t.Fatal(err)
	}
	got2 := readWithin(t, client2)
	if gotID := binary.BigEndian.Uint16(got2[0:2]); gotID != 0x8888 {
		t.Errorf("deferred response ID = %#x, want 0x8888", gotID)
	}
	if rcode := dnsmessage.RCode(got2[3] & 0x0f); rcode != dnsmessage.RCodeServerFailure {
		t.Errorf("deferred rcode = %v, want %v", rcode, dnsmessage.RCodeServerFailure)
	}
}
