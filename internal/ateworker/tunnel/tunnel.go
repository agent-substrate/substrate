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

// Package tunnel wires atunnel into a worker: its flags, listeners, DNS
// relay, and per-actor ingress and egress. Both workers build their tunnel here
// so they cannot drift.
package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"time"

	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/atunnel/dns"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/egresspolicy"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/linuxsys"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/serverboot"
)

// resolvConfPath holds the worker pod's resolvers. Tests override it.
var resolvConfPath = "/etc/resolv.conf"

// FlagSet is the registration surface that flag.FlagSet and pflag.FlagSet share.
type FlagSet interface {
	StringVar(p *string, name, value, usage string)
}

// Config is the atunnel setup a worker takes from its flags.
type Config struct {
	// ListenAddress serves actor ingress HTTPS.
	ListenAddress string
	// ConnectListenAddress serves actor ingress mTLS CONNECT.
	ConnectListenAddress string
	// CredentialBundle holds the worker Pod certificate and key, used for
	// inbound serving, outbound mTLS, and authentication to the atelet broker.
	CredentialBundle string
	// TrustBundle verifies router clients and the node-local atelet.
	TrustBundle string
	// ClientIdentity is the SPIFFE ID allowed to call actor ingress HTTPS.
	ClientIdentity string
	// BrokerIdentity is the SPIFFE ID the node-local atelet must present on the
	// credential broker connection. It names atelet's namespace, not this
	// worker's, so it is configured rather than derived from the downward API.
	BrokerIdentity string
	// EgressListenAddress receives transparently intercepted actor egress TCP.
	EgressListenAddress string
	// EgressTrustBundle verifies the remote egress gateway's serving cert.
	EgressTrustBundle string
	// DNSEgressPolicy selects the DNS egress policy enforcement mode.
	DNSEgressPolicy string
}

// RegisterFlags registers the atunnel flags on fs and returns the Config they
// fill once fs is parsed.
func RegisterFlags(fs FlagSet) *Config {
	c := &Config{}
	// Every listen address here is an unspecified wildcard, which Go binds as a
	// dual-stack socket.
	fs.StringVar(&c.ListenAddress, "atunnel-listen-address", ":443", "Address for actor ingress HTTPS")
	fs.StringVar(&c.ConnectListenAddress, "atunnel-connect-listen-address", ":8443", "Address for actor ingress mTLS CONNECT")
	fs.StringVar(&c.CredentialBundle, "atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Worker Pod credential bundle used by atunnel for inbound serving and outbound mTLS")
	fs.StringVar(&c.TrustBundle, "atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Pod identity trust bundle used for router clients and the node-local atelet")
	fs.StringVar(&c.ClientIdentity, "atunnel-client-identity", installdefaults.RouterSPIFFEID(installdefaults.SystemNamespace), "SPIFFE identity allowed to call actor ingress HTTPS")
	fs.StringVar(&c.BrokerIdentity, "atunnel-broker-identity", installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "SPIFFE identity the node-local atelet must present on the credential broker connection. Override when atelet runs outside the default namespace.")
	fs.StringVar(&c.EgressListenAddress, "atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	fs.StringVar(&c.EgressTrustBundle, "atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "Service DNS trust bundle for the remote egress gateway")
	fs.StringVar(&c.DNSEgressPolicy, "dns-egress-policy", "", "DNS egress policy enforcement mode (must be "+egresspolicy.DNSEgressPolicyEnforceInAtunnel+")")
	return c
}

// Tunnel is the atunnel state one worker shares across its actors.
type Tunnel struct {
	// Ingress proxies router traffic to the active actors.
	Ingress *ingress.Server
	// Egress tunnels the active actors' outbound TCP to the egress gateway.
	Egress *egress.Proxy
	// EgressPort is the local atunnel listener used as the target of the
	// actor network's transparent TCP redirect.
	EgressPort uint16
	// DNSRelay answers the sandbox's DNS from inside its own namespace.
	DNSRelay *dns.Relay

	cfg Config
	// ateletTLS is used to authenticate to atelet to fetch egress policies.
	ateletTLS *tls.Config
}

// Start builds the DNS relay and starts serving actor ingress, which proxies
// to upstream inside each sandbox. Egress binds only in sandbox namespaces, so
// nothing listens for it here.
func Start(ctx context.Context, cfg Config, upstream string) (*Tunnel, error) {
	if err := egresspolicy.ValidateDNSEgressPolicy(cfg.DNSEgressPolicy); err != nil {
		return nil, err
	}
	upstreamURL, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("while parsing atunnel upstream: %w", err)
	}

	dnsRelay, err := dns.NewRelay(resolvConfPath)
	if err != nil {
		return nil, fmt.Errorf("configuring DNS relay: %w", err)
	}
	ateletTLS, err := ateletdial.TLSConfig(cfg.CredentialBundle, cfg.TrustBundle, cfg.BrokerIdentity)
	if err != nil {
		return nil, fmt.Errorf("configuring the atelet connection: %w", err)
	}

	// Ingress
	ingressServer, err := ingress.NewServer(ingress.Config{
		CredentialBundlePath: cfg.CredentialBundle,
		TrustBundlePath:      cfg.TrustBundle,
		AllowedClientID:      cfg.ClientIdentity,
		Upstream:             upstreamURL,
	})
	if err != nil {
		return nil, fmt.Errorf("configuring ingress: %w", err)
	}
	if err := serve(ctx, "atunnel", cfg.ListenAddress, ingressServer.Serve); err != nil {
		return nil, err
	}
	if err := serve(ctx, "atunnel CONNECT", cfg.ConnectListenAddress, ingressServer.ServeConnect); err != nil {
		return nil, err
	}

	// Egress
	egressProxy, err := egress.New(linuxsys.TCPOriginalDestination)
	if err != nil {
		return nil, fmt.Errorf("configuring egress: %w", err)
	}
	egressPort, err := egress.PortFromListenAddr(cfg.EgressListenAddress)
	if err != nil {
		return nil, err
	}

	return &Tunnel{
			Ingress:    ingressServer,
			Egress:     egressProxy,
			EgressPort: egressPort,
			DNSRelay:   dnsRelay,
			cfg:        cfg,
			ateletTLS:  ateletTLS},
		nil
}

// serve listens on address and runs serveFn on it, exiting the process if it
// fails.
func serve(ctx context.Context, name, address string, serveFn func(context.Context, net.Listener) error) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("while opening %s listener: %w", name, err)
	}
	go func() {
		if err := serveFn(ctx, lis); err != nil {
			serverboot.Fatal(ctx, "Failed to serve "+name, err)
		}
	}()
	slog.InfoContext(ctx, name+" serving", slog.String("address", address))
	return nil
}

// ActorEgress is an actor's tunneled egress, ready to activate.
type ActorEgress struct {
	// client presents the actor certificate to the remote egress gateway.
	client *egress.Client
	// certificateSource owns the actor key and renews its certificate via atelet.
	certificateSource *egress.BrokerCertificateSource
	expiresAt         time.Time
}

// PrepareEgress mints the actor's certificate and builds its gateway client. A
// nil gateway means the actor has no tunneled egress and yields a nil result.
func (t *Tunnel) PrepareEgress(ctx context.Context, actor resources.ActorAttribution, gateway *ateworkerpb.EgressGateway) (*ActorEgress, error) {
	if gateway == nil {
		return nil, nil
	}
	if gateway.GetAddress() == "" {
		return nil, fmt.Errorf("egress gateway address is required")
	}
	serverName, _, err := net.SplitHostPort(gateway.GetAddress())
	if err != nil {
		return nil, fmt.Errorf("invalid egress gateway address %q: %w", gateway.GetAddress(), err)
	}
	certificateSource, err := egress.NewBrokerCertificateSource(egress.BrokerConfig{
		SocketPath:           nodepath.WorkerSupportSocket,
		CredentialBundlePath: t.cfg.CredentialBundle,
		TrustBundlePath:      t.cfg.TrustBundle,
		ActorAtespace:        actor.Ref.Atespace,
		ActorName:            actor.Ref.Name,
		ActorUID:             actor.UID,
		AteletSPIFFEID:       t.cfg.BrokerIdentity,
	})
	if err != nil {
		return nil, fmt.Errorf("while configuring actor certificate broker: %w", err)
	}
	// Mint before starting the workload so configured tunneled egress fails
	// closed. The source retains the private key for mTLS and renewal.
	expiresAt, err := certificateSource.MintWorkerCertificate(ctx)
	if err != nil {
		return nil, fmt.Errorf("while obtaining actor certificate: %w", err)
	}
	gatewayClient, err := egress.NewClient(egress.ClientConfig{
		GatewayAddress:       gateway.GetAddress(),
		ServerName:           serverName,
		GetClientCertificate: certificateSource.GetClientCertificate,
		TrustBundlePath:      t.cfg.EgressTrustBundle,
	})
	if err != nil {
		return nil, fmt.Errorf("while configuring actor egress client: %w", err)
	}
	return &ActorEgress{client: gatewayClient, certificateSource: certificateSource, expiresAt: expiresAt}, nil
}

// ActivateEgress starts tunneling the actor's outbound traffic. Call it before
// the workload starts, since a workload may need the network to become ready.
// A nil egress is a no-op.
func (t *Tunnel) ActivateEgress(actor resources.ActorAttribution, actorEgress *ActorEgress) error {
	if actorEgress == nil {
		return nil
	}
	if err := t.Egress.Activate(actor.UID, actorEgress.client, actorEgress.certificateSource, actorEgress.expiresAt); err != nil {
		return fmt.Errorf("while activating actor egress: %w", err)
	}
	return nil
}

// ActivateIngress starts admitting the actor's inbound traffic through dial.
// Call it once the workload is ready to serve.
func (t *Tunnel) ActivateIngress(actor resources.ActorAttribution, dial ingress.DialFunc) error {
	if err := t.Ingress.Activate(actor.Ref.Atespace, actor.Ref.Name, actor.UID, dial); err != nil {
		return fmt.Errorf("while activating actor ingress: %w", err)
	}
	return nil
}

// Deactivate stops admitting the actor's traffic and drains its active streams,
// before the actor network is torn down. It attempts both directions even if
// one fails.
func (t *Tunnel) Deactivate(ctx context.Context, actor resources.ActorAttribution) error {
	err := errors.Join(
		t.Ingress.Deactivate(ctx, actor.Ref.Atespace, actor.Ref.Name, actor.UID),
		t.Egress.Deactivate(ctx, actor.UID),
	)
	if err != nil {
		return fmt.Errorf("while deactivating actor networking: %w", err)
	}
	return nil
}
