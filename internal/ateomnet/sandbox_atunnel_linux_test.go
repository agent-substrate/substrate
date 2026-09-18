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

// These integration tests use an external package to avoid an import cycle:
// atunnel imports ateomnet.
package ateomnet_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/roottest"
)

// Verify redirection preserves the destination, including unconfigured ports.
func TestSandboxEgressReachesAtunnelOnAnyPort(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()
	const egressPort = 15001

	n, err := ateomnet.SetupSandboxNetwork(ctx, ateomnet.SandboxNetworkConfig{
		ActorUID: "66666666-6666-6666-6666-666666666666", Veth: true, EgressPort: egressPort,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { ateomnet.CleanupSandboxNetwork(n) })

	listeners, err := ateomnet.ListenInNetNS(ctx, n.GatewayNetNS, []uint16{egressPort})
	if err != nil {
		t.Fatalf("ListenInNetNS: %v", err)
	}
	defer listeners[0].Close()

	type capture struct{ destination, payload string }
	got := make(chan capture, 1)
	accept := func() {
		c, err := listeners[0].Accept()
		if err != nil {
			return
		}
		defer c.Close()
		dst, err := atunnel.TCPOriginalDestination(c)
		if err != nil {
			t.Errorf("TCPOriginalDestination: %v", err)
			return
		}
		buf := make([]byte, len("hello"))
		io.ReadFull(c, buf)
		got <- capture{destination: dst, payload: string(buf)}
	}
	go accept()

	for _, want := range []string{"93.184.216.34:443", "93.184.216.34:8080", "93.184.216.34:9999"} {
		if err := ateomnet.NetNSDo(ctx, n.RuntimeNetNS, func(context.Context) error {
			c, err := net.Dial("tcp", want)
			if err != nil {
				return err
			}
			defer c.Close()
			_, err = io.WriteString(c, "hello")
			return err
		}); err != nil {
			t.Fatalf("sandbox egress dial to %s: %v", want, err)
		}
		select {
		case c := <-got:
			if c.destination != want {
				t.Errorf("atunnel saw destination %q, want %q", c.destination, want)
			}
			if c.payload != "hello" {
				t.Errorf("atunnel read %q, want %q", c.payload, "hello")
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("the sandbox's connection to %s never reached atunnel", want)
		}
		go accept()
	}
}
