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
	"errors"
	"net"
	"sync/atomic"
	"testing"
)

type fakePacketConn struct {
	net.PacketConn
	id     int
	closed atomic.Bool
}

func (c *fakePacketConn) Close() error {
	c.closed.Store(true)
	return nil
}

func TestUDPConnPoolGetLeastUsedAndDiscardAfterMaxUses(t *testing.T) {
	var created int
	var newConnCalls int
	pool := newUDPConnPool(func(network, address string) (net.PacketConn, error) {
		if network != "udp" || address != ":0" {
			t.Errorf("listenPacket(%q, %q), want (\"udp\", \":0\")", network, address)
		}
		created++
		return &fakePacketConn{id: created}, nil
	})
	pool.maxUses = 3
	pool.onNewConn = func(net.PacketConn) {
		newConnCalls++
	}

	// Two concurrent Get calls create two sockets when none are idle.
	c1, err := pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	c2, err := pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	if c1 == c2 || created != 2 || newConnCalls != 2 {
		t.Fatalf("created = %d, newConnCalls = %d; want 2 distinct sockets", created, newConnCalls)
	}

	// Return c1 and use it a second time so c1 has 2 uses while c2 has 1 use.
	pool.Put(c1)
	got, err := pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	if got != c1 {
		t.Fatalf("Get() = %v, want idle socket c1", got)
	}
	pool.Put(c1) // c1 now has 2 uses
	pool.Put(c2) // c2 now has 1 use

	// With both c1 (2 uses) and c2 (1 use) idle, Get picks the least-used (c2).
	got, err = pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	if got != c2 {
		t.Fatalf("Get() picked %v, want least-used socket c2", got)
	}
	pool.Put(c2) // c2 now has 2 uses

	// Use c1 for its 3rd (maxUses) time; Put must discard and close it.
	got, err = pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	if got != c1 {
		t.Fatalf("Get() = %v, want c1", got)
	}
	pool.Put(c1)
	if !c1.(*fakePacketConn).closed.Load() {
		t.Error("c1 was not closed after reaching maxUses=3")
	}

	// Sockets returned beyond maxIdle are closed immediately instead of accumulating.
	pool.maxIdle = 1
	c3, err := pool.Get()
	if err != nil || c3 != c2 {
		t.Fatalf("Get() = %v, %v; want c2", c3, err)
	}
	c4, err := pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	c5, err := pool.Get()
	if err != nil {
		t.Fatal(err)
	}
	pool.Put(c4) // c4 has 1 use; fills idle pool (len == maxIdle == 1)
	pool.Put(c5) // c5 has 1 use; exceeds maxIdle -> closed immediately
	if !c5.(*fakePacketConn).closed.Load() {
		t.Error("excess idle socket c5 was not closed when idle pool reached maxIdle")
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !c4.(*fakePacketConn).closed.Load() {
		t.Error("idle socket c4 was not closed on pool.Close")
	}
	if !c3.(*fakePacketConn).closed.Load() {
		t.Error("checked-out socket c3 was not closed on pool.Close")
	}
	if _, err := pool.Get(); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Get after Close = %v, want %v", err, net.ErrClosed)
	}
}
