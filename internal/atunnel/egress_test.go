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

package atunnel

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestEgressForwardsActiveActor(t *testing.T) {
	dialed := make(chan egressDial, 1)
	upstreamProxy, upstreamGateway := net.Pipe()
	t.Cleanup(func() {
		_ = upstreamProxy.Close()
		_ = upstreamGateway.Close()
	})
	dialer := egressDialerFunc(func(_ context.Context, destination string, metadata EgressMetadata) (net.Conn, error) {
		dialed <- egressDial{destination: destination, metadata: metadata}
		return upstreamProxy, nil
	})
	egress, err := NewEgress(func(net.Conn) (string, error) {
		return "192.0.2.10:443", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := egress.Activate(dialer, "team-a", "actor-1", 7, "actor-token"); err != nil {
		t.Fatal(err)
	}

	downstreamActor, downstreamProxy := net.Pipe()
	t.Cleanup(func() {
		_ = downstreamActor.Close()
		_ = downstreamProxy.Close()
	})
	egress.handle(downstreamProxy)

	gotDial := <-dialed
	if gotDial.destination != "192.0.2.10:443" {
		t.Errorf("destination = %q, want 192.0.2.10:443", gotDial.destination)
	}
	if gotDial.metadata != (EgressMetadata{Atespace: "team-a", ActorName: "actor-1", ActorVersion: 7, BearerToken: "actor-token"}) {
		t.Errorf("metadata = %+v", gotDial.metadata)
	}

	actorPayload := []byte("from actor")
	go func() { _, _ = downstreamActor.Write(actorPayload) }()
	gotAtGateway := make([]byte, len(actorPayload))
	if _, err := io.ReadFull(upstreamGateway, gotAtGateway); err != nil {
		t.Fatal(err)
	}
	if string(gotAtGateway) != string(actorPayload) {
		t.Errorf("gateway payload = %q, want %q", gotAtGateway, actorPayload)
	}

	gatewayPayload := []byte("from gateway")
	go func() { _, _ = upstreamGateway.Write(gatewayPayload) }()
	gotAtActor := make([]byte, len(gatewayPayload))
	if _, err := io.ReadFull(downstreamActor, gotAtActor); err != nil {
		t.Fatal(err)
	}
	if string(gotAtActor) != string(gatewayPayload) {
		t.Errorf("actor payload = %q, want %q", gotAtActor, gatewayPayload)
	}

	if err := egress.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEgressRejectsInactiveConnection(t *testing.T) {
	egress, err := NewEgress(func(net.Conn) (string, error) {
		t.Fatal("inactive egress resolved destination")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	actor, proxy := net.Pipe()
	defer actor.Close()
	if err := actor.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	egress.handle(proxy)
	if _, err := actor.Read(make([]byte, 1)); err == nil {
		t.Fatal("inactive connection remained open")
	}
}

type egressDial struct {
	destination string
	metadata    EgressMetadata
}

type egressDialerFunc func(context.Context, string, EgressMetadata) (net.Conn, error)

func (f egressDialerFunc) DialContext(ctx context.Context, destination string, metadata EgressMetadata) (net.Conn, error) {
	return f(ctx, destination, metadata)
}
