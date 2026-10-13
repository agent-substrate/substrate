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
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"golang.org/x/net/dns/dnsmessage"
)

// exampleQuestion is the question dnsQuery asks.
var exampleQuestion = dnsmessage.Question{
	Name:  dnsmessage.MustNewName("example.com."),
	Type:  dnsmessage.TypeA,
	Class: dnsmessage.ClassINET,
}

// recordQuery records dnsQuery(clientID) holding an in-flight slot, as a
// forwarded query does, and returns its upstream ID and the rewritten query.
func recordQuery(t *testing.T, p *pendingRequests, clientID uint16, client net.Addr, upstreams []string) (uint16, []byte) {
	t.Helper()
	p.limiter.inFlight.tryAcquire()
	raw := dnsQuery(clientID)
	id, ok := p.record(raw, clientID, exampleQuestion, client, upstreams)
	if !ok {
		t.Fatalf("record(%#x) failed", clientID)
	}
	return id, raw
}

// checkExpiry fails t unless expiry is timeout after an instant in [before, after].
func checkExpiry(t *testing.T, expiry, before, after time.Time, timeout time.Duration) {
	t.Helper()
	if expiry.Before(before.Add(timeout)) || expiry.After(after.Add(timeout)) {
		t.Errorf("expiry is %v after the call, want %v", expiry.Sub(before), timeout)
	}
}

func TestPendingRequestsTimeoutForAttempt(t *testing.T) {
	// Long enough that splitting it two ways still exceeds the cap.
	const exchange = 10 * defaultUpstreamAttemptTimeout

	for _, tc := range []struct {
		name            string
		exchangeTimeout time.Duration
		attemptTimeout  time.Duration
		upstreamIdx     int
		numUpstreams    int
		want            time.Duration
	}{
		{
			name:            "single upstream gets the whole exchange",
			exchangeTimeout: exchange,
			numUpstreams:    1,
			want:            exchange,
		},
		{
			name:            "attempt with fallbacks left is capped",
			exchangeTimeout: exchange,
			numUpstreams:    2,
			want:            defaultUpstreamAttemptTimeout,
		},
		{
			name:            "attempt with fallbacks left gets its share",
			exchangeTimeout: defaultUpstreamAttemptTimeout,
			upstreamIdx:     1,
			numUpstreams:    4,
			want:            defaultUpstreamAttemptTimeout / 4,
		},
		{
			name:            "last upstream gets the whole exchange",
			exchangeTimeout: exchange,
			upstreamIdx:     1,
			numUpstreams:    2,
			want:            exchange,
		},
		{
			name:         "unset exchange timeout uses the default",
			numUpstreams: 1,
			want:         dnsExchangeTimeout,
		},
		{
			name:            "attempt timeout overrides an attempt with fallbacks left",
			exchangeTimeout: exchange,
			attemptTimeout:  40 * time.Millisecond,
			numUpstreams:    2,
			want:            40 * time.Millisecond,
		},
		{
			name:            "attempt timeout overrides the last attempt",
			exchangeTimeout: exchange,
			attemptTimeout:  40 * time.Millisecond,
			upstreamIdx:     1,
			numUpstreams:    2,
			want:            40 * time.Millisecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPendingRequests(nil)
			p.exchangeTimeout = tc.exchangeTimeout
			p.attemptTimeout = tc.attemptTimeout
			if got := p.timeoutForAttempt(tc.upstreamIdx, tc.numUpstreams); got != tc.want {
				t.Errorf("timeoutForAttempt(%d, %d) = %v, want %v", tc.upstreamIdx, tc.numUpstreams, got, tc.want)
			}
		})
	}
}

func TestPendingRequestsRecord(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	client := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 54321}
	// Responses are matched on their unmapped source address, so the key must
	// be normalized.
	upstreams := []string{"[::ffff:10.96.0.10]:53", "10.96.0.11:53"}

	lim.inFlight.tryAcquire()
	raw := dnsQuery(0xbeef)
	before := time.Now()
	id, ok := p.record(raw, 0xbeef, exampleQuestion, client, upstreams)
	after := time.Now()
	if !ok {
		t.Fatal("record failed")
	}
	if got := binary.BigEndian.Uint16(raw[0:2]); got != id {
		t.Errorf("query ID = %#x, want the upstream ID %#x", got, id)
	}
	entry, ok := p.entries[id]
	if !ok {
		t.Fatal("no entry keyed by the upstream ID")
	}
	want := &pendingRequest{
		clientRequestID: 0xbeef,
		clientAddr:      client,
		question:        exampleQuestion,
		rawQuery:        raw,
		upstreams:       upstreams,
	}
	if diff := cmp.Diff(want, entry, cmp.AllowUnexported(pendingRequest{}), cmpopts.IgnoreFields(pendingRequest{}, "expiry")); diff != "" {
		t.Errorf("entry mismatch (-want +got):\n%s", diff)
	}
	checkExpiry(t, entry.expiry, before, after, p.timeoutForAttempt(0, len(upstreams)))
	if !p.inUse.Get(id) {
		t.Errorf("upstream ID %#x is not marked in use", id)
	}
	if got := lim.inFlight.occupied(); got != 1 {
		t.Errorf("%d in-flight slots held, want 1 until the request completes", got)
	}

	// The entry keeps its own copy of the query to resend on failover.
	raw[len(raw)-1] ^= 0xff
	if bytes.Equal(entry.rawQuery, raw) {
		t.Error("entry shares the caller's query buffer")
	}
}

// Responses are matched by upstream ID, so record must not hand out one still
// in flight.
func TestPendingRequestsRecordAllocatesFreeID(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53"}
	const free = 0xabcd
	for id := range 1 << 16 {
		if id != free {
			p.inUse.Set(uint16(id))
		}
	}

	if id, _ := recordQuery(t, p, 0x1234, nil, upstreams); id != free {
		t.Fatalf("record allocated %#x, want the only free ID %#x", id, free)
	}

	// With every ID in flight, record fails and frees the query's slot.
	lim.inFlight.tryAcquire()
	if _, ok := p.record(dnsQuery(0x5678), 0x5678, exampleQuestion, nil, upstreams); ok {
		t.Fatal("record succeeded with every upstream ID in flight")
	}
	if got := lim.inFlight.occupied(); got != 1 {
		t.Errorf("%d in-flight slots held, want 1 for the recorded query", got)
	}
}

func TestPendingRequestsDeleteEntryReleasesSlotOnce(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53"}
	lim.inFlight.tryAcquire() // held by another query
	id, _ := recordQuery(t, p, 0x1234, nil, upstreams)

	p.mu.Lock()
	p.deleteEntryLocked(id)
	p.deleteEntryLocked(id)
	p.mu.Unlock()

	if len(p.entries) != 0 || p.inUse.Count() != 0 {
		t.Errorf("deleted entry left %d entries and %d IDs in use", len(p.entries), p.inUse.Count())
	}
	if got := lim.inFlight.occupied(); got != 1 {
		t.Errorf("%d in-flight slots held, want 1: deleting twice must free the slot once", got)
	}
}

func TestPendingRequestsFailOverOnSendError(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	id, raw := recordQuery(t, p, 0x1234, nil, upstreams)

	// Errors for an unknown ID, or for an attempt not yet made, are ignored.
	if _, _, ok := p.failOverOnSendError(id+1, 0); ok {
		t.Error("failed over a request that was never recorded")
	}
	if _, _, ok := p.failOverOnSendError(id, 1); ok {
		t.Error("failed over on an error for an attempt not yet made")
	}

	// The first upstream failing moves the request to the second.
	before := time.Now()
	nextIdx, query, ok := p.failOverOnSendError(id, 0)
	after := time.Now()
	if !ok || nextIdx != 1 {
		t.Fatalf("failOverOnSendError(%#x, 0) = %d, _, %v; want 1, _, true", id, nextIdx, ok)
	}
	if !bytes.Equal(query, raw) {
		t.Error("failover query differs from the recorded query")
	}
	entry, ok := p.entries[id]
	if !ok {
		t.Fatal("entry missing after failover")
	}
	if entry.upstreamIdx != 1 {
		t.Errorf("upstreamIdx = %d, want 1", entry.upstreamIdx)
	}
	checkExpiry(t, entry.expiry, before, after, p.timeoutForAttempt(1, len(upstreams)))
	if got := lim.inFlight.occupied(); got != 1 {
		t.Errorf("%d in-flight slots held during failover, want 1", got)
	}

	// A repeated error for the first attempt is stale.
	if _, _, ok := p.failOverOnSendError(id, 0); ok {
		t.Error("failed over twice on errors for the same attempt")
	}

	// The last upstream failing drops the request and frees its slot.
	if _, _, ok := p.failOverOnSendError(id, 1); ok {
		t.Error("failed over past the last upstream")
	}
	if len(p.entries) != 0 || p.inUse.Count() != 0 {
		t.Errorf("dropped request left %d entries and %d IDs in use", len(p.entries), p.inUse.Count())
	}
	if got := lim.inFlight.occupied(); got != 0 {
		t.Errorf("%d in-flight slots held after the last upstream failed, want 0", got)
	}
}

func TestPendingRequestsClearAll(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	for i := range 3 {
		recordQuery(t, p, uint16(i), nil, upstreams)
	}

	p.clearAll()
	if len(p.entries) != 0 || p.inUse.Count() != 0 {
		t.Errorf("clearAll left %d entries and %d IDs in use", len(p.entries), p.inUse.Count())
	}
	if got := lim.inFlight.occupied(); got != 0 {
		t.Errorf("%d in-flight slots held after clearAll, want 0", got)
	}
}

func TestPendingRequestsSweep(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	id, raw := recordQuery(t, p, 0x1234, nil, upstreams)
	entry := p.entries[id]

	if failovers, deliveries := p.sweep(entry.expiry); len(failovers) != 0 || len(deliveries) != 0 {
		t.Fatalf("sweep at the expiry returned %d failovers and %d deliveries, want none until it has passed", len(failovers), len(deliveries))
	}

	// An expired attempt moves to the next upstream.
	now := entry.expiry.Add(time.Nanosecond)
	failovers, deliveries := p.sweep(now)
	wantFailovers := []reqFailover{{upstreamID: id, upstreamIdx: 1, query: raw}}
	if diff := cmp.Diff(wantFailovers, failovers, cmp.AllowUnexported(reqFailover{})); diff != "" {
		t.Errorf("failovers mismatch (-want +got):\n%s", diff)
	}
	if len(deliveries) != 0 {
		t.Errorf("sweep returned %d deliveries on failover, want 0", len(deliveries))
	}
	if len(p.entries) != 1 || p.entries[id] != entry || entry.upstreamIdx != 1 {
		t.Fatal("entry not advanced to the next upstream")
	}
	if want := now.Add(p.timeoutForAttempt(1, len(upstreams))); !entry.expiry.Equal(want) {
		t.Errorf("expiry = %v, want %v", entry.expiry, want)
	}

	// An expired last attempt with no answer held back is dropped silently.
	failovers, deliveries = p.sweep(entry.expiry.Add(time.Nanosecond))
	if len(failovers) != 0 || len(deliveries) != 0 {
		t.Errorf("sweep after the last attempt returned %d failovers and %d deliveries, want none", len(failovers), len(deliveries))
	}
	if len(p.entries) != 0 || p.inUse.Count() != 0 {
		t.Errorf("expired request left %d entries and %d IDs in use", len(p.entries), p.inUse.Count())
	}
	if got := lim.inFlight.occupied(); got != 0 {
		t.Errorf("%d in-flight slots held after expiry, want 0", got)
	}
}

// A SERVFAIL beats a timeout: when the last upstream never answers, the client
// gets the SERVFAIL held back from an earlier one.
func TestPendingRequestsSweepDeliversDeferredAnswer(t *testing.T) {
	lim := newLimiter()
	p := newPendingRequests(lim)
	client := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 54321}
	upstreams := []string{"10.96.0.10:53", "10.96.0.11:53"}
	id, raw := recordQuery(t, p, 0x1234, client, upstreams)

	// Leave the request as a SERVFAIL from the first upstream does: waiting on
	// the second, with the SERVFAIL held back.
	if _, _, ok := p.failOverOnSendError(id, 0); !ok {
		t.Fatal("failOverOnSendError failed")
	}
	entry := p.entries[id]
	entry.deferredResp = dnsAnswer(raw, byte(dnsmessage.RCodeServerFailure))

	failovers, deliveries := p.sweep(entry.expiry.Add(time.Nanosecond))
	if len(failovers) != 0 {
		t.Errorf("sweep returned %d failovers past the last upstream, want 0", len(failovers))
	}
	wantDeliveries := []reqDelivery{{
		clientAddr: client,
		payload:    dnsAnswer(dnsQuery(0x1234), byte(dnsmessage.RCodeServerFailure)),
	}}
	if diff := cmp.Diff(wantDeliveries, deliveries, cmp.AllowUnexported(reqDelivery{})); diff != "" {
		t.Errorf("deliveries mismatch (-want +got):\n%s", diff)
	}
	if len(p.entries) != 0 || p.inUse.Count() != 0 {
		t.Errorf("expired request left %d entries and %d IDs in use", len(p.entries), p.inUse.Count())
	}
	if got := lim.inFlight.occupied(); got != 0 {
		t.Errorf("%d in-flight slots held after expiry, want 0", got)
	}
}
