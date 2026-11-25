// Copyright 2026 The Agent Substrate Authors
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
// cmd/ateworker-gvisor makes to run one, in its order. Only gVisor itself is
// missing: each actor's network namespace, veth, nftables redirect, egress
// listener, and DNS relay are real, and a guest namespace holds eth0 in place
// of gVisor's network stack.
package workerhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/vishvananda/netlink"

	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/netns"
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
	// NetNS is the guest namespace holding the actor's network stack, where
	// its ingress actor listens and its egress starts.
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
	guest       netns.Handle
}

// Start starts atunnel from cfg, as a worker's main does, and hosts actors
// actors whose egress tunnels to gatewayAddress.
func Start(ctx context.Context, cfg atunnel.Config, actors int, gatewayAddress string) (*Host, error) {
	t, err := atunnel.Start(ctx, cfg, actornet.ActorHTTPUpstream, noop.Meter{})
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
// certificate through atelet), set up the sandbox network, import eth0 and
// activate the gateway as a sandbox start does, then activate.
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
	guest, err := importEth0(ctx, session, attribution.UID)
	if err != nil {
		return errors.Join(err, session.Close(ctx))
	}
	h.mu.Lock()
	h.actors = append(h.actors, &actor{attribution: attribution, session: session, guest: guest})
	h.mu.Unlock()
	if err := h.tunnel.ActivateEgress(attribution, egress); err != nil {
		return err
	}
	return h.tunnel.ActivateIngress(attribution, session.Dialer())
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
			NetNS: filepath.Join("/run/netns", guestNetNSName(a.attribution.UID)),
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
		if err := a.guest.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := netns.RemoveNamed(guestNetNSName(a.attribution.UID)); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		slog.ErrorContext(ctx, "tearing down actors", slog.Any("err", err))
		return err
	}
	return nil
}

// guestNetNSName names the namespace standing in for gVisor's network stack
// in one hosted sandbox.
func guestNetNSName(actorUID string) string {
	return nodepath.ActorNetNSName(actorUID) + "-guest"
}

// importEth0 plays runsc: it removes eth0's kernel addresses, has
// ActivateGateway assign the gateway address, then moves eth0 into a guest
// namespace in place of gVisor's network stack.
func importEth0(ctx context.Context, session *actornet.SandboxSession, actorUID string) (_ netns.Handle, retErr error) {
	if err := netns.Do(ctx, session.Network.NetNS, func(context.Context) error {
		eth0, err := netlink.LinkByName(actornet.ActorVethName)
		if err != nil {
			return err
		}
		addrs, err := netlink.AddrList(eth0, netlink.FAMILY_V4)
		if err != nil {
			return err
		}
		for _, addr := range addrs {
			if err := netlink.AddrDel(eth0, &addr); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if err := session.Network.ActivateGateway(ctx); err != nil {
		return 0, err
	}
	guest, err := netns.CreateNamed(guestNetNSName(actorUID))
	if err != nil {
		return 0, err
	}
	defer func() {
		if retErr != nil {
			_ = guest.Close()
			_ = netns.RemoveNamed(guestNetNSName(actorUID))
		}
	}()
	if err := netns.Do(ctx, session.Network.NetNS, func(context.Context) error {
		eth0, err := netlink.LinkByName(actornet.ActorVethName)
		if err != nil {
			return err
		}
		return netlink.LinkSetNsFd(eth0, int(guest))
	}); err != nil {
		return 0, err
	}
	if err := netns.Do(ctx, guest, func(context.Context) error {
		// The ingress actor binds port 80 without privileges, as it could in
		// the sandbox namespace.
		if err := netns.SetSysctls(netns.Sysctl{Key: "net/ipv4/ip_unprivileged_port_start", Value: "0"}); err != nil {
			return err
		}
		lo, err := netlink.LinkByName("lo")
		if err != nil {
			return err
		}
		if err := netlink.LinkSetUp(lo); err != nil {
			return err
		}
		eth0, err := netlink.LinkByName(actornet.ActorVethName)
		if err != nil {
			return err
		}
		if err := netlink.AddrReplace(eth0, actornet.ActorVethAddr); err != nil {
			return err
		}
		if err := netlink.LinkSetUp(eth0); err != nil {
			return err
		}
		return netlink.RouteReplace(&netlink.Route{LinkIndex: eth0.Attrs().Index, Gw: actornet.ActorVethGwIP})
	}); err != nil {
		return 0, err
	}
	return guest, nil
}
