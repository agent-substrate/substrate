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

package ateomnet

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/vishvananda/netlink"
)

func TestApplyEgressBandwidthInvalid(t *testing.T) {
	for _, rate := range []int64{-1, 1, 1023, maxEgressBytesPerSecond + 1} {
		if err := ApplyEgressBandwidth(context.Background(), -1, "missing", rate); err == nil {
			t.Errorf("accepted invalid rate %d", rate)
		}
	}
	if err := ApplyEgressBandwidth(context.Background(), -1, "missing", 0); err != nil {
		t.Fatal(err)
	}
}

// The two Actors have the same addresses and share a worker, but the kernel
// keeps their buckets separate. Recreating the limited Actor models resume.
func TestEgressBandwidthIsolationAndReactivation(t *testing.T) {
	roottest.Require(t, "creates network namespaces and installs tc policers")
	ctx := context.Background()
	const rate = 128 * 1024
	for activation := range 2 {
		limited := bandwidthTestSession(t, "bandwidth-limited", rate)
		unlimited := bandwidthTestSession(t, "bandwidth-unlimited", 0)
		before, err := limited.NetworkStats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// The kernel may emit IPv6 discovery while the fresh veth comes up.
		if before.EgressBytesPerSecond != rate || before.EgressBytes > 1024 || before.EgressDroppedPackets != 0 {
			t.Fatalf("fresh activation stats: %v", before)
		}
		started := time.Now()
		var received [2]uint64
		var wg sync.WaitGroup
		for i, session := range []*SandboxSession{limited, unlimited} {
			wg.Add(1)
			go func() { defer wg.Done(); received[i] = sendEgressUDP(t, session) }()
		}
		wg.Wait()
		stats, err := limited.NetworkStats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		other, err := unlimited.NetworkStats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if stats.EgressDroppedPackets == 0 || other.EgressDroppedPackets != 0 {
			t.Fatalf("drop counters: limited=%v unlimited=%v", stats, other)
		}
		// The payload cannot exceed replenishment plus the initial burst.
		budget := uint64(time.Since(started).Seconds()*rate) + 64*1024
		if received[0] > budget {
			t.Fatalf("limited Actor admitted %d bytes, above its budget", received[0])
		}
		if received[0] < 64*1024 || received[1] < 2*received[0] {
			t.Fatalf("isolation: received limited=%d unlimited=%d", received[0], received[1])
		}
		t.Logf("activation=%d rate=%d received=%d unlimited=%d attempted=%d dropped_packets=%d", activation, rate, received[0], received[1], stats.EgressBytes, stats.EgressDroppedPackets)
		if err := limited.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err := unlimited.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func bandwidthTestSession(t *testing.T, uid string, rate int64) *SandboxSession {
	t.Helper()
	session, err := ServeSandbox(context.Background(), SandboxNetworkConfig{ActorUID: uid, Veth: true, EgressPort: testEgressPort, EgressBytesPerSecond: rate}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return session
}

func sendEgressUDP(t *testing.T, session *SandboxSession) uint64 {
	t.Helper()
	ctx := context.Background()
	var receiver net.PacketConn
	err := netns.Do(ctx, session.Network.GatewayNetNS, func(context.Context) error {
		var err error
		receiver, err = net.ListenPacket("udp4", ActorVethGateway+":0")
		return err
	})
	if err != nil {
		t.Error(err)
		return 0
	}
	defer receiver.Close()
	var received atomic.Uint64
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 2048)
		for {
			n, _, err := receiver.ReadFrom(buffer)
			if err != nil {
				return
			}
			received.Add(uint64(n))
		}
	}()
	var sender net.Conn
	err = netns.Do(ctx, session.Network.RuntimeNetNS, func(context.Context) error {
		var err error
		sender, err = net.Dial("udp4", receiver.LocalAddr().String())
		return err
	})
	if err != nil {
		t.Error(err)
		return 0
	}
	defer sender.Close()
	payload := make([]byte, 1400)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for range 1000 {
		<-ticker.C
		if _, err := sender.Write(payload); err != nil {
			t.Error(err)
			break
		}
	}
	time.Sleep(50 * time.Millisecond)
	_ = receiver.Close()
	<-done
	return received.Load()
}

// MicroVM tap traffic enters the same kernel ingress hook as the veth peer.
func TestEgressBandwidthTap(t *testing.T) {
	roottest.Require(t, "creates a tap and installs tc policers")
	ctx := context.Background()
	ns, err := netns.CreateNamed("bandwidth-tap")
	if err != nil {
		t.Fatal(err)
	}
	defer ns.Close()
	defer netns.RemoveNamed("bandwidth-tap")
	var tap *netlink.Tuntap
	err = netns.Do(ctx, ns, func(context.Context) error {
		tap = &netlink.Tuntap{LinkAttrs: netlink.LinkAttrs{Name: "tap0_kata"}, Mode: netlink.TUNTAP_MODE_TAP, Flags: netlink.TUNTAP_NO_PI, Queues: 1}
		if err := netlink.LinkAdd(tap); err != nil {
			return err
		}
		return netlink.LinkSetUp(tap)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, fd := range tap.Fds {
			_ = fd.Close()
		}
	}()
	if err := ApplyEgressBandwidth(ctx, ns, "tap0_kata", 1048576); err != nil {
		t.Fatal(err)
	}
	session := &SandboxSession{Network: &SandboxNetwork{GatewayNetNS: ns}, egressLink: "tap0_kata", egressBytesPerSecond: 1048576}
	// Inject padded ARP frames as the guest would write them to the tap.
	frame := make([]byte, 1400)
	for i := range 6 {
		frame[i] = 0xff
	}
	copy(frame[6:12], []byte{2, 0, 0, 0, 0, 2})
	frame[12], frame[13] = 0x08, 0x06
	frame[15], frame[16], frame[18], frame[19], frame[21] = 1, 8, 6, 4, 1
	copy(frame[22:28], frame[6:12])
	copy(frame[28:32], []byte{169, 254, 17, 2})
	copy(frame[38:42], []byte{169, 254, 17, 1})
	for range 2000 {
		if _, err := tap.Fds[0].Write(frame); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := session.NetworkStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.EgressBytesPerSecond != 1048576 || stats.EgressBytes == 0 || stats.EgressDroppedPackets == 0 {
		t.Fatalf("tap stats: %v", stats)
	}
}

func TestEgressBandwidthGatewayTCP(t *testing.T) {
	roottest.Require(t, "creates network namespaces and installs tc policers")
	ctx := context.Background()
	session := bandwidthTestSession(t, "bandwidth-gateway", 64*1024)
	listeners, err := netns.Listen(ctx, session.Network.GatewayNetNS, []uint16{testEgressPort})
	if err != nil {
		t.Fatal(err)
	}
	defer listeners[0].Close()
	const bytes = 128 * 1024
	received := make(chan error, 1)
	go func() {
		conn, err := listeners[0].Accept()
		if err != nil {
			received <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		_, err = io.CopyN(io.Discard, conn, bytes)
		received <- err
	}()
	var conn net.Conn
	err = netns.Do(ctx, session.Network.RuntimeNetNS, func(context.Context) error {
		var err error
		conn, err = net.DialTimeout("tcp4", "198.51.100.1:12345", 5*time.Second)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := conn.Write(make([]byte, bytes)); err != nil {
		t.Fatal(err)
	}
	if err := <-received; err != nil {
		t.Fatal(err)
	}
	stats, err := session.NetworkStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.EgressBytes < bytes || stats.EgressDroppedPackets == 0 {
		t.Fatalf("gateway traffic escaped policing: %v", stats)
	}
	t.Logf("redirected TCP bytes=%d attempted=%d dropped_packets=%d", bytes, stats.EgressBytes, stats.EgressDroppedPackets)
}
