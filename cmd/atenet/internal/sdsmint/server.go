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
	"log/slog"
	"strconv"
	"sync/atomic"

	secretservice "github.com/envoyproxy/go-control-plane/envoy/service/secret/v3"
)

// secretTypeURL is the xDS type URL for SDS resources.
const secretTypeURL = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret"

// server implements Envoy's Secret Discovery Service, minting a certificate
// per requested resource name.
//
// DeltaSecrets is the only method of that service this server implements.
// State-of-the-world SDS -- StreamSecrets and FetchSecrets -- is deliberately
// left to the embedded Unimplemented, so an Envoy configured with anything
// other than DELTA_GRPC fails immediately and visibly.
type server struct {
	secretservice.UnimplementedSecretDiscoveryServiceServer

	minter *minter
	log    *slog.Logger

	// nonce numbers the responses this server sends. Every xDS response needs
	// one: the client echoes it back as response_nonce, which is what makes a
	// later request recognizable as an ACK or NACK of a specific response
	// rather than a fresh subscription. A response that carries none cannot be
	// ACKed at all.
	//
	// One counter for the whole server rather than one per stream. A client
	// only ever compares a nonce against the last one it received on its own
	// stream, so the sequence being sparse there costs nothing, and this
	// server does not correlate them either -- an incoming response_nonce is
	// read only for the NACK log line in deltaStream.handle. Atomic because
	// streams are served concurrently.
	nonce atomic.Uint64
}

// serverOptions configures newServer.
type serverOptions struct {
	Logger *slog.Logger
}

// newServer builds an SDS server over m.
func newServer(m *minter, opts serverOptions) *server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &server{
		minter: m,
		log:    opts.Logger,
	}
}

// nextNonce returns the nonce to stamp on the next response. It increments
// before formatting, so the first response is "1" and none ever goes out with
// the empty nonce.
func (s *server) nextNonce() string {
	return strconv.FormatUint(s.nonce.Add(1), 10)
}
