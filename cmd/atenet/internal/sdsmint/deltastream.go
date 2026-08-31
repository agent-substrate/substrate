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

// Delta SDS: the stateful, per-connection half of the server.
// - https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol#xds-protocol-delta
// - https://www.envoyproxy.io/docs/envoy/latest/configuration/security/secret
package sdsmint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	tlsv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tls/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	secretservice "github.com/envoyproxy/go-control-plane/envoy/service/secret/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// DeltaSecrets is what DELTA_GRPC drives. It is a long-lived loop that mints
// incrementally rather than serving a fixed snapshot.
func (s *server) DeltaSecrets(stream secretservice.SecretDiscoveryService_DeltaSecretsServer) error {
	ctx := stream.Context()

	// An arbitrary depth, and not a tuned one. One request yields at most one
	// response -- handleSubscribe batches every name in a request into a single
	// send -- and the producer signs a leaf per name before it queues anything,
	// which costs far more than handing a proto to gRPC's own buffered write
	// path. So the queue sits at 0 or 1 and any small number does the same job.
	// Only the extremes would change behavior: 0 makes every response a
	// synchronous handoff to sendLoop and stalls the select loop on each write,
	// which is what the buffer exists to avoid, and unbounded lets a wedged
	// stream accumulate every certificate it ever minted. Filling this is not a
	// failure either -- send blocks, with a ctx.Done escape, which is the stall
	// the buffer defers rather than prevents.
	const sendDepth = 8

	st := &deltaStream{
		srv:      s,
		stream:   stream,
		sendCh:   make(chan *discovery.DeltaDiscoveryResponse, sendDepth),
		sendErr:  make(chan error, 1),
		sendDone: make(chan struct{}),
	}

	go st.sendLoop(ctx)

	recvCh := make(chan *discovery.DeltaDiscoveryRequest)
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			select {
			case recvCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for send thread to finish before exiting.
	defer func() {
		close(st.sendCh)
		<-st.sendDone
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-st.sendErr:
			return err
		case err := <-recvErrCh:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case req := <-recvCh:
			if err := st.handle(ctx, req); err != nil {
				return err
			}
		}
	}
}

// deltaStream is the per-connection plumbing for a DeltaSecrets stream.
type deltaStream struct {
	srv    *server
	stream secretservice.SecretDiscoveryService_DeltaSecretsServer

	// sendCh queues responses for sendLoop.
	sendCh chan *discovery.DeltaDiscoveryResponse
	// sendErr carries the first send failure back to DeltaSecrets.
	sendErr chan error
	// sendDone closes once the loop has stopped touching the stream.
	sendDone chan struct{}
}

// handle applies one request from the client.
func (d *deltaStream) handle(ctx context.Context, req *discovery.DeltaDiscoveryRequest) error {
	if url := req.GetTypeUrl(); url != "" && url != secretTypeURL {
		return fmt.Errorf("unexpected type_url %q on the SDS stream", url)
	}

	// A request carrying error_detail is a NACK of whatever we last sent, and
	// only that: it brings no subscription changes to apply.
	if req.GetErrorDetail() != nil {
		d.logNACK(ctx, req)
		return nil
	}

	return d.handleSubscribe(ctx, req.GetResourceNamesSubscribe())
}

// logNACK records that Envoy rejected the last response. Nothing is resent: the
// server has no second thing to offer for the name, and a retry loop against a
// client that is rejecting on principle is worse than the failure.
func (d *deltaStream) logNACK(ctx context.Context, req *discovery.DeltaDiscoveryRequest) {
	ed := req.GetErrorDetail()
	d.srv.log.ErrorContext(ctx, "envoy NACKed an SDS response",
		slog.String("message", ed.GetMessage()),
		slog.Int("code", int(ed.GetCode())),
		slog.String("nonce", req.GetResponseNonce()),
	)
}

// handleSubscribe mints a leaf for every name the client asked for and sends
// the batch. A name this stream already holds is minted again rather than
// skipped. Envoy re-subscribes in two cases and both want a certificate: after
// a resource TTL dropped the secret and a handshake needs it back, and on the
// first request of a new stream, where it re-subscribes to everything it holds.
func (d *deltaStream) handleSubscribe(ctx context.Context, names []string) error {
	if len(names) == 0 {
		// A bare ACK, or an unsubscribe-only request. Nothing to send.
		return nil
	}

	var resources []*discovery.Resource
	var removed []string

	for _, name := range names {
		cert, err := d.srv.minter.certificate(ctx, name)
		if err != nil {
			// Refused. Tell Envoy the name does not exist; the paused
			// handshake for that SNI then fails, which is the intended
			// outcome for something that is not a hostname.
			removed = append(removed, name)
			continue
		}
		res, err := d.pack(name, cert)
		if err != nil {
			return err
		}
		resources = append(resources, res)
	}

	if len(resources) == 0 && len(removed) == 0 {
		return nil
	}
	return d.send(ctx, resources, removed)
}

// pack wraps a minted cert as a versioned delta Resource.
func (d *deltaStream) pack(name string, cert *MintedCert) (*discovery.Resource, error) {
	secret := toSecret(name, cert)
	body, err := anypb.New(secret)
	if err != nil {
		return nil, fmt.Errorf("marshalling secret for %q: %w", name, err)
	}
	// The serial changes on every mint, so it is a natural resource version:
	// every mint looks like a new version to Envoy.
	version := cert.Serial

	// Measured against this leaf's own notAfter.
	// --leaf-cert-ttl is refused below 2m, so the subtraction is positive on the
	// ordinary path. The floor is for the clamp, where notAfter is the CA's and
	// can be nearer than that.
	ttl := max(time.Until(cert.NotAfter)-time.Minute, time.Second)

	return &discovery.Resource{
		Name:     name,
		Version:  version,
		Resource: body,
		Ttl:      durationpb.New(ttl),
	}, nil
}

func (d *deltaStream) send(ctx context.Context, resources []*discovery.Resource, removed []string) error {
	resp := &discovery.DeltaDiscoveryResponse{
		TypeUrl:          secretTypeURL,
		Resources:        resources,
		RemovedResources: removed,
		Nonce:            d.srv.nextNonce(),
	}

	// The ctx.Done arm is the only way out if sendLoop has already exited on a
	// send failure while sendCh is full: nothing drains the channel after that,
	// and the loop that would notice sendErr is this same goroutine, parked here.
	// A bare channel send would block for the life of the process, since gRPC
	// cannot interrupt a stuck handler.
	select {
	case d.sendCh <- resp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendLoop drains sendCh onto the stream.
// It stops on the first send failure and hands it to sendErr, which is what
// DeltaSecrets returns; a full sendErr means a failure is already on its way
// back, so the second one is dropped rather than blocking the exit.
func (d *deltaStream) sendLoop(ctx context.Context) {
	defer close(d.sendDone)
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-d.sendCh:
			if !ok {
				return
			}
			if err := d.stream.Send(resp); err != nil {
				select {
				case d.sendErr <- err:
				default:
				}
				return
			}
		}
	}
}

// inlineBytes wraps PEM bytes as an inline Envoy DataSource. Leaf material is
// inlined rather than written to a path because it is per-connection and
// short-lived; putting it on a filesystem would only widen exposure.
func inlineBytes(b []byte) *corev3.DataSource {
	return &corev3.DataSource{
		Specifier: &corev3.DataSource_InlineBytes{InlineBytes: b},
	}
}

// toSecret packs a minted cert into the Secret proto Envoy expects back. The
// secret's name MUST equal the requested resource name (the SNI), or Envoy
// will not match the response to its subscription.
func toSecret(name string, c *MintedCert) *tlsv3.Secret {
	return &tlsv3.Secret{
		Name: name,
		Type: &tlsv3.Secret_TlsCertificate{
			TlsCertificate: &tlsv3.TlsCertificate{
				CertificateChain: inlineBytes(c.CertChainPEM),
				PrivateKey:       inlineBytes(c.PrivateKeyPEM),
			},
		},
	}
}
