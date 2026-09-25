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

package dns

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/roottest"
)

// stoppableDNS records that its serving contexts were canceled.
type stoppableDNS struct{ packet, stream chan struct{} }

func (d *stoppableDNS) ServePacket(ctx context.Context, pc net.PacketConn) error {
	<-ctx.Done()
	close(d.packet)
	return pc.Close()
}

func (d *stoppableDNS) Serve(ctx context.Context, l net.Listener) error {
	<-ctx.Done()
	close(d.stream)
	return l.Close()
}

func TestClosingSandboxDNSStopsServing(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	const nsName = "dns-teardown-test"
	ns, err := netns.CreateNamed(nsName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ns.Close()
		_ = netns.RemoveNamed(nsName)
	}()

	relay := &stoppableDNS{packet: make(chan struct{}), stream: make(chan struct{})}
	closers, serve, err := Serve(context.Background(), relay, ns, 53)
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range serve {
		go fn()
	}
	for _, c := range closers {
		_ = c.Close()
	}

	for _, tc := range []struct {
		name    string
		stopped chan struct{}
	}{{"UDP", relay.packet}, {"TCP", relay.stream}} {
		select {
		case <-tc.stopped:
		case <-time.After(5 * time.Second):
			t.Errorf("%s serving outlived the sandbox's sockets", tc.name)
		}
	}
}
