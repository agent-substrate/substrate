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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/roottest"
)

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

	relay, err := NewRelayForUpstreams([]string{"127.0.0.1:53"})
	if err != nil {
		t.Fatal(err)
	}
	closers, serve, err := relay.Serve(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{}, len(serve))
	for _, fn := range serve {
		go func() {
			fn()
			stopped <- struct{}{}
		}()
	}
	for _, c := range closers {
		_ = c.Close()
	}

	for range serve {
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatal("DNS serving outlived the sandbox's sockets")
		}
	}
}
