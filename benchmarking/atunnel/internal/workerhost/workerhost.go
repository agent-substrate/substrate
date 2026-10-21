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

//go:build linux

// Package workerhost hosts atunnel the way an ateworker does, with the
// worker's own code: atunnel.Start once, then per actor the calls
// cmd/ateworker-gvisor makes to run one, in its order. Only the sandbox itself
// is missing: each actor's network namespaces, veth, nftables redirect, egress
// listener, and DNS relay are real, but no gVisor runs in them.
package workerhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

// Atespace holds every benchmark actor.
const Atespace = "bench"

// ActorPort is where the ingress actor listens inside each sandbox: the port
// of actornet.ActorHTTPUpstream, which ingress proxies to.
const ActorPort = 80

// Sandbox is one hosted actor, as a process placing an ingress actor in it needs it.
type Sandbox struct {
	// Actor is atespace/name, the value of the routing header.
	Actor string `json:"actor"`
	UID   string `json:"uid"`
	// NetNS is the namespace the actor's runtime would run in, where its
	// ingress actor listens and its egress starts.
	NetNS string `json:"netns"`
}

// Info describes the hosted actors to the load generator and the supervisor.
type Info struct {
	Sandboxes []Sandbox `json:"sandboxes"`
	// ActorPort is the port a CONNECT should name to reach an ingress actor.
	ActorPort int `json:"actor_port"`
}

// Host is a running atunnel with its actors.
type Host struct {
	tunnel *atunnel.Tunnel

	mu     sync.Mutex
	actors []*actor
}

type actor struct {
	attribution resources.ActorAttribution
	session     *actornet.SandboxSession
}

// Start starts atunnel from cfg, as a worker's main does, and hosts actors
// actors whose egress tunnels to gatewayAddress.
func Start(ctx context.Context, cfg atunnel.Config, actors int, gatewayAddress string) (*Host, error) {
	t, err := atunnel.Start(ctx, cfg, actornet.ActorHTTPUpstream)
	if err != nil {
		return nil, err
	}
	h := &Host{tunnel: t}
	for i := range actors {
		name := "actor-" + strconv.Itoa(i)
		attribution := resources.ActorAttribution{
			Ref: resources.ActorRef{Atespace: Atespace, Name: name},
			UID: fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
		}
		if err := h.host(ctx, attribution, gatewayAddress); err != nil {
			return nil, errors.Join(fmt.Errorf("hosting %s: %w", name, err), h.Close(context.Background()))
		}
	}
	return h, nil
}

// host runs an actor's network as cmd/ateworker-gvisor's RunWorkload does:
// clear any previous activation, prepare egress (minting the actor
// certificate through atelet), set up the sandbox network, then activate.
func (h *Host) host(ctx context.Context, attribution resources.ActorAttribution, gatewayAddress string) error {
	if err := h.tunnel.Deactivate(ctx, attribution); err != nil {
		return err
	}
	egress, err := h.tunnel.PrepareEgress(ctx, attribution, &ateworkerpb.EgressGateway{Address: gatewayAddress})
	if err != nil {
		return err
	}
	session, err := actornet.ServeSandbox(ctx, actornet.SandboxNetworkConfig{
		ActorUID:   attribution.UID,
		Veth:       true,
		EgressPort: h.tunnel.EgressPort,
		DNSPolicy:  h.tunnel.ActorEgressPolicy(attribution),
	}, h.tunnel.Egress, h.tunnel.DNSRelay)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.actors = append(h.actors, &actor{attribution: attribution, session: session})
	h.mu.Unlock()
	return h.tunnel.Activate(attribution, session.Dialer(), egress)
}

// Info returns the hosted actors.
func (h *Host) Info() Info {
	h.mu.Lock()
	defer h.mu.Unlock()
	info := Info{ActorPort: ActorPort}
	for _, a := range h.actors {
		info.Sandboxes = append(info.Sandboxes, Sandbox{
			Actor: a.attribution.Ref.Atespace + "/" + a.attribution.Ref.Name,
			UID:   a.attribution.UID,
			NetNS: nodepath.ActorNetNSPath(a.attribution.UID),
		})
	}
	return info
}

// Close tears every actor down as a worker does when an actor stops:
// deactivate its traffic, then remove its network.
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	actors := h.actors
	h.actors = nil
	h.mu.Unlock()
	var errs []error
	for _, a := range actors {
		if err := h.tunnel.Deactivate(ctx, a.attribution); err != nil {
			errs = append(errs, err)
		}
		if err := a.session.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		slog.ErrorContext(ctx, "tearing down actors", slog.Any("err", err))
		return err
	}
	return nil
}
