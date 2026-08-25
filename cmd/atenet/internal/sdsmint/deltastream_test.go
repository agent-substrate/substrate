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

package sdsmint

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"testing"
	"testing/synctest"
	"time"

	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/metadata"
)

// fakeDeltaStream stands in for the gRPC stream Envoy would be on the other
// end of. Requests are fed in from a slice; responses are collected.
type fakeDeltaStream struct {
	ctx      context.Context
	requests chan *discovery.DeltaDiscoveryRequest
	sent     chan *discovery.DeltaDiscoveryResponse
}

func newFakeDeltaStream(ctx context.Context) *fakeDeltaStream {
	return &fakeDeltaStream{
		ctx:      ctx,
		requests: make(chan *discovery.DeltaDiscoveryRequest, 8),
		sent:     make(chan *discovery.DeltaDiscoveryResponse, 8),
	}
}

func (f *fakeDeltaStream) Send(resp *discovery.DeltaDiscoveryResponse) error {
	select {
	case f.sent <- resp:
		return nil
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}

func (f *fakeDeltaStream) Recv() (*discovery.DeltaDiscoveryRequest, error) {
	select {
	case req, ok := <-f.requests:
		if !ok {
			return nil, io.EOF
		}
		return req, nil
	case <-f.ctx.Done():
		return nil, io.EOF
	}
}

func (f *fakeDeltaStream) Context() context.Context     { return f.ctx }
func (f *fakeDeltaStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeDeltaStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeDeltaStream) SetTrailer(metadata.MD)       {}
func (f *fakeDeltaStream) SendMsg(any) error            { return nil }
func (f *fakeDeltaStream) RecvMsg(any) error            { return nil }

// respondWait bounds a wait for something the server should do promptly. Every
// caller runs inside a synctest bubble, so this is fake time: only a run that
// is already failing ever spends it.
const respondWait = time.Minute

// nextResponse waits for one response, failing the test if none arrives.
func (f *fakeDeltaStream) nextResponse(t *testing.T) *discovery.DeltaDiscoveryResponse {
	t.Helper()
	select {
	case resp := <-f.sent:
		return resp
	case <-time.After(respondWait):
		t.Fatal("timed out waiting for a DeltaDiscoveryResponse")
		return nil
	}
}

// quiet blocks until the server has nothing left to do, then fails if it sent
// anything. This is the negative assertion the whole file used to spell as a
// sleep: synctest.Wait returns once every goroutine is durably blocked, so
// anything the server meant to send is already in the channel by then.
func (f *fakeDeltaStream) quiet(t *testing.T, whileDoing string) {
	t.Helper()
	synctest.Wait()
	select {
	case resp := <-f.sent:
		t.Fatalf("server sent %v %s", resourceNames(resp), whileDoing)
	default:
	}
}

func testServer(t *testing.T, opts serverOptions) *server {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = quietLogger()
	}
	m := testMinter(t, minterOptions{TTL: defaultTTL})
	return newServer(m, opts)
}

// startServer runs DeltaSecrets against a fake stream and returns the stream
// plus a func that stops it and reports the server's error.
func startServer(t *testing.T, srv *server) (*fakeDeltaStream, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := newFakeDeltaStream(ctx)

	done := make(chan error, 1)
	go func() { done <- srv.DeltaSecrets(stream) }()

	return stream, func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(respondWait):
			t.Fatal("DeltaSecrets did not return after the stream was cancelled")
			return nil
		}
	}
}

func resourceNames(resp *discovery.DeltaDiscoveryResponse) []string {
	names := make([]string, 0, len(resp.GetResources()))
	for _, r := range resp.GetResources() {
		names = append(names, r.GetName())
	}
	return names
}

// unpackSecret pulls the Secret proto out of a delta Resource.
func unpackSecret(t *testing.T, res *discovery.Resource) *tlsv3.Secret {
	t.Helper()
	msg, err := res.GetResource().UnmarshalNew()
	if err != nil {
		t.Fatalf("unmarshalling resource %q: %v", res.GetName(), err)
	}
	secret, ok := msg.(*tlsv3.Secret)
	if !ok {
		t.Fatalf("resource %q is a %T, want *tlsv3.Secret", res.GetName(), msg)
	}
	return secret
}

// leafFromResource pulls the leaf x509 out of a delta Resource carrying a
// Secret, so a test can reason about the certificate Envoy would actually
// serve rather than just the xDS version string.
func leafFromResource(t *testing.T, res *discovery.Resource) *x509.Certificate {
	t.Helper()
	secret := unpackSecret(t, res)
	chain := secret.GetTlsCertificate().GetCertificateChain().GetInlineBytes()
	block, _ := pem.Decode(chain)
	if block == nil {
		t.Fatalf("resource %q: certificate chain is not PEM", res.GetName())
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("resource %q: parsing leaf: %v", res.GetName(), err)
	}
	return leaf
}

func TestDeltaSecretsMintsSubscribedName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}

		resp := stream.nextResponse(t)
		if resp.GetTypeUrl() != secretTypeURL {
			t.Errorf("type_url = %q, want %q", resp.GetTypeUrl(), secretTypeURL)
		}
		if resp.GetNonce() == "" {
			t.Error("response has no nonce; Envoy needs one to ACK")
		}
		if len(resp.GetResources()) != 1 {
			t.Fatalf("got %d resources, want 1", len(resp.GetResources()))
		}

		res := resp.GetResources()[0]
		if res.GetName() != "a.example" {
			t.Errorf("resource name = %q, want a.example", res.GetName())
		}
		if res.GetVersion() == "" {
			t.Error("resource has no version; delta xDS needs one per resource")
		}

		secret := unpackSecret(t, res)
		// This is the invariant the whole design rests on: Envoy matches the
		// response to its on-demand subscription by secret name, which is the SNI.
		if secret.GetName() != "a.example" {
			t.Errorf("secret name = %q, want it to equal the requested resource name", secret.GetName())
		}

		chain := secret.GetTlsCertificate().GetCertificateChain().GetInlineBytes()
		key := secret.GetTlsCertificate().GetPrivateKey().GetInlineBytes()
		if _, err := tls.X509KeyPair(chain, key); err != nil {
			t.Errorf("secret does not contain a usable TLS keypair: %v", err)
		}
	})
}

// TestDeltaSecretsStampsResourceTTL covers the only thing that gets a name
// re-minted. Envoy drops a resource when its TTL fires and re-subscribes on the
// next handshake; nothing on this side pushes. A resource sent without a ttl is
// held by Envoy for good, and the leaf inside it goes on being served long past
// its notAfter -- see poc/sdsmint/expiry, which measured both.
func TestDeltaSecretsStampsResourceTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}

		res := stream.nextResponse(t).GetResources()[0]
		ttl := res.GetTtl()
		if ttl == nil {
			t.Fatal("resource carries no ttl; Envoy would hold this secret forever and serve the leaf past its notAfter")
		}

		// The invariant, rather than the exact fraction: the secret has to be
		// dropped while the leaf it carries is still valid, so the handshake
		// that re-subscribes is never the one served an expired leaf.
		remaining := time.Until(leafFromResource(t, res).NotAfter)
		if ttl.AsDuration() >= remaining {
			t.Errorf("ttl %s is not shorter than the leaf's remaining validity %s; a handshake could land after the leaf expires but before Envoy drops it",
				ttl.AsDuration(), remaining)
		}
	})
}

func TestDeltaSecretsWithdrawsRefusedName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		// The minter refuses names, not destinations: "*.evil.test" is turned away
		// for being a wildcard rather than for being anyone in particular. What
		// this pins is the partial response -- one name refused must not cost the
		// other name in the same subscription its certificate.
		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"ok.allowed", "*.evil.test"},
		}

		resp := stream.nextResponse(t)

		if len(resp.GetResources()) != 1 || resp.GetResources()[0].GetName() != "ok.allowed" {
			t.Errorf("resources = %v, want just ok.allowed", resourceNames(resp))
		}
		// A server cannot NACK in xDS. Withdrawing the name is how it says "this
		// will not be issued", and per the Envoy docs it also cancels the
		// data-plane subscription for that name.
		if got := resp.GetRemovedResources(); len(got) != 1 || got[0] != "*.evil.test" {
			t.Errorf("removed_resources = %v, want [*.evil.test]", got)
		}
	})
}

func TestDeltaSecretsBareAckSendsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}
		first := stream.nextResponse(t)

		// Envoy's ACK carries the nonce and no subscriptions. Replying to it
		// would start an infinite ACK loop.
		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:       secretTypeURL,
			ResponseNonce: first.GetNonce(),
		}
		stream.quiet(t, "in reply to a bare ACK")
	})
}

// TestDeltaSecretsNeverPushesUnprompted pins the shape of the server after
// rotation and the idle sweep were both removed: a subscribe is answered once
// and then the stream is silent for the whole life of the leaf and beyond. The
// server never speaks first. Nothing re-mints a name in place, so a leaf
// expires under a live subscription and Envoy goes on serving it -- that is a
// known consequence of the removals, not an accident, and this is where it is
// written down.
func TestDeltaSecretsNeverPushesUnprompted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The TTL testServer's minter is built with.
		const ttl = defaultTTL
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}
		stream.nextResponse(t)

		// Well past the point where the leaf has expired. Free on a fake clock.
		time.Sleep(2 * ttl)
		stream.quiet(t, "after the only subscribed leaf had expired")
	})
}

// TestDeltaSecretsIgnoresUnsubscribe pins that the server holds no subscription
// set to unsubscribe from. Envoy volunteers an unsubscribe when the
// configuration referencing a secret goes away; there is nothing on this side
// to forget, so the request must be absorbed silently rather than answered or
// treated as an error that tears down the stream.
func TestDeltaSecretsIgnoresUnsubscribe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}
		stream.nextResponse(t)

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                  secretTypeURL,
			ResourceNamesUnsubscribe: []string{"a.example"},
		}
		stream.quiet(t, "in reply to an unsubscribe")

		// The stream is still usable afterwards, and a name it just unsubscribed
		// from is minted again on request like any other -- the server draws no
		// distinction, because it kept no record to draw one from.
		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}
		if names := resourceNames(stream.nextResponse(t)); len(names) != 1 || names[0] != "a.example" {
			t.Errorf("after an unsubscribe the server served %v, want [a.example]", names)
		}
	})
}

// TestDeltaSecretsReplayOnlyRequestIsSilent covers the other half of what handle
// ignores. A request carrying nothing but initial_resource_versions says what
// Envoy already holds; answering it would push leaves nobody asked for. The
// re-subscribe that accompanies a real reconnect is what prompts the re-mint,
// and it arrives as an ordinary subscribe.
func TestDeltaSecretsReplayOnlyRequestIsSilent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                 secretTypeURL,
			InitialResourceVersions: map[string]string{"resumed.example": "old-version"},
		}

		stream.quiet(t, "in reply to a replay-only request")
	})
}

func TestDeltaSecretsRejectsWrongTypeURL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream := newFakeDeltaStream(ctx)

		done := make(chan error, 1)
		go func() { done <- srv.DeltaSecrets(stream) }()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                "type.googleapis.com/envoy.config.cluster.v3.Cluster",
			ResourceNamesSubscribe: []string{"a.example"},
		}

		// Wait for the server to fail on its own rather than canceling, which
		// would race the context-done branch of the stream loop.
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("DeltaSecrets accepted a non-SDS type_url")
			}
		case <-time.After(respondWait):
			t.Fatal("DeltaSecrets did not reject a non-SDS type_url")
		}
	})
}

func TestDeltaSecretsSurvivesNack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}
		first := stream.nextResponse(t)

		// A NACK must not tear down the stream; Envoy would just reconnect and
		// we would lose every live subscription.
		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:       secretTypeURL,
			ResponseNonce: first.GetNonce(),
			ErrorDetail:   &rpcstatus.Status{Code: 3, Message: "bad certificate"},
		}
		stream.requests <- &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"b.example"},
		}

		resp := stream.nextResponse(t)
		if names := resourceNames(resp); len(names) != 1 || names[0] != "b.example" {
			t.Errorf("after a NACK the server served %v, want [b.example]", names)
		}
	})
}

// TestResubscribeIsMintedAgain is what is left of the refresh path. Envoy only
// re-subscribes to a name it has dropped, so a repeat subscribe is a request
// for a certificate and must be answered with a freshly minted one rather than
// suppressed as a duplicate. With rotation and the idle sweep both gone this is
// the only way a name ever gets a new leaf.
func TestResubscribeIsMintedAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := testServer(t, serverOptions{})
		stream, stop := startServer(t, srv)
		defer func() {
			if err := stop(); err != nil {
				t.Errorf("DeltaSecrets returned %v", err)
			}
		}()

		subscribe := &discovery.DeltaDiscoveryRequest{
			TypeUrl:                secretTypeURL,
			ResourceNamesSubscribe: []string{"a.example"},
		}

		stream.requests <- subscribe
		first := stream.nextResponse(t)
		if len(first.GetResources()) != 1 {
			t.Fatalf("initial response carried %d resources, want 1", len(first.GetResources()))
		}

		stream.requests <- subscribe
		second := stream.nextResponse(t)
		if len(second.GetResources()) != 1 {
			t.Fatalf("re-subscribe was answered with %d resources, want 1", len(second.GetResources()))
		}

		if got := second.GetResources()[0].GetName(); got != "a.example" {
			t.Fatalf("re-subscribe returned %q, want a.example", got)
		}
		leaf := leafFromResource(t, second.GetResources()[0])
		if err := leaf.VerifyHostname("a.example"); err != nil {
			t.Fatalf("re-minted leaf does not cover a.example: %v", err)
		}

		// A new leaf, not the first one handed back. The version is the serial,
		// so an unchanged version here would mean Envoy sees no update and goes
		// on serving whatever it already had.
		if v1, v2 := first.GetResources()[0].GetVersion(), second.GetResources()[0].GetVersion(); v1 == v2 {
			t.Errorf("re-subscribe returned the same version %s; the name was not minted again", v1)
		}
	})
}
