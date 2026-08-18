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

// Command egressprobe drives the egress gateway's MITM leg from inside the
// cluster and reports the certificate it was served, so an e2e suite can assert
// on what sdsmint actually minted.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/credbundle"
)

var (
	listenAddress        = flag.String("listen", ":8080", "Address the probe's HTTP API listens on.")
	gatewayAddress       = flag.String("gateway-address", "atenet-egress.ate-system.svc:443", "host:port of the egress gateway's CONNECT front door.")
	credentialBundlePath = flag.String("credential-bundle", "/run/actor-identity/credential-bundle.pem", "PEM credential bundle presented as the client certificate to the gateway.")
	trustBundlePath      = flag.String("trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle used to verify the gateway's serving certificate.")
	handshakeTimeout     = flag.Duration("handshake-timeout", 20*time.Second, "Budget for one CONNECT plus inner handshake.")
)

// tunnelDestination is the CONNECT authority. atunnel takes this from
// SO_ORIGINAL_DST and rejects hostnames, so it must be a literal IP -- and the
// gateway routes every CONNECT to the MITM listener regardless of authority,
// which is why a documentation address that resolves nowhere is enough. The
// name being tested travels in the tunneled ClientHello, not here.
const tunnelDestination = "192.0.2.1:443"

// Stages a handshake can fail at, reported in handshakeResult.Stage. They name
// the hop, so a test can say which of the gateway's several ways of saying no
// it expected without matching on prose.
const (
	// stageClient is a local failure before anything was dialed: a bad flag, an
	// unreadable credential bundle. Never a statement about the gateway.
	stageClient = "client"
	// stageGatewayTLS is the front door's mTLS. Reaching here and failing means
	// the gateway refused the client certificate -- or presented one the probe
	// would not accept.
	stageGatewayTLS = "gateway_tls"
	// stageConnect is the CONNECT exchange. A failure here means the
	// certificate was accepted and the request was declined anyway, which is
	// where the ext_proc authorization check lives. ConnectStatus carries the
	// status code.
	stageConnect = "connect"
	// stageTunnel is any other failure opening the tunnel: DNS, TCP, a
	// truncated response.
	stageTunnel = "tunnel"
	// stageInnerHandshake is the tunneled TLS handshake -- the one sdsmint
	// serves. A failure here is the minter's, not the front door's.
	stageInnerHandshake = "inner_handshake"
)

// handshakeResult is the probe's response body.
type handshakeResult struct {
	SNI string `json:"sni"`
	// Credential is the bundle path this handshake presented, echoed back so a
	// test cannot mistake a result for one taken with a different identity.
	Credential string `json:"credential"`
	// OK reports whether the inner TLS handshake completed. A denied SNI is a
	// normal outcome, not a probe failure, so it comes back as OK=false with
	// the reason rather than as an HTTP error.
	OK bool `json:"ok"`
	// Stage is where a failed handshake stopped, one of the stage constants
	// above. Empty when OK.
	Stage string `json:"stage,omitempty"`
	// ConnectStatus is the status the gateway answered the CONNECT with, set
	// only when Stage is stageConnect and the gateway actually replied.
	ConnectStatus int    `json:"connect_status,omitempty"`
	Error         string `json:"error,omitempty"`
	// ChainPEM is the chain the gateway presented, leaf first.
	ChainPEM string `json:"chain_pem,omitempty"`
}

// handshake opens a tunnel through the gateway and completes an inner TLS
// handshake for the requested SNI, which is what makes Envoy ask sdsmint for a
// secret under that name.
func handshake(w http.ResponseWriter, r *http.Request) {
	sni := r.URL.Query().Get("sni")
	if sni == "" {
		http.Error(w, "missing sni query parameter", http.StatusBadRequest)
		return
	}

	// Which credential to present. The default is the actor identity the suite
	// minted; a test that is asserting how the gateway treats some OTHER
	// credential names its path here rather than needing a second pod, since
	// the choice is made per handshake and nothing is cached across them.
	credential := r.URL.Query().Get("credential-bundle")
	if credential == "" {
		credential = *credentialBundlePath
	}

	ctx, cancel := context.WithTimeout(r.Context(), *handshakeTimeout)
	defer cancel()

	result := handshakeResult{SNI: sni, Credential: credential}
	chain, stage, err := fetchChain(ctx, sni, credential)
	if err != nil {
		result.Stage = stage
		result.Error = err.Error()
		var rejected *atunnel.ConnectRejectedError
		if errors.As(err, &rejected) {
			result.ConnectStatus = rejected.StatusCode
		}
	} else {
		result.OK = true
		result.ChainPEM = encodeChain(chain)
	}
	writeJSON(w, result)
}

func encodeChain(chain []*x509.Certificate) string {
	var out []byte
	for _, cert := range chain {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})...)
	}
	return string(out)
}

// fetchChain opens a tunnel and completes the inner handshake, returning the
// chain it was served. On failure it also returns the stage that failed, since
// which hop said no is the assertion most callers are actually making.
func fetchChain(ctx context.Context, sni, credentialBundle string) ([]*x509.Certificate, string, error) {
	client, err := atunnel.NewClient(atunnel.ClientConfig{
		GatewayAddress:       *gatewayAddress,
		ServerName:           serverName(*gatewayAddress),
		GetClientCertificate: credbundle.ClientLoader(credentialBundle),
		TrustBundlePath:      *trustBundlePath,
	})
	if err != nil {
		return nil, stageClient, fmt.Errorf("building egress client: %w", err)
	}

	conn, err := client.DialContext(ctx, tunnelDestination)
	if err != nil {
		return nil, dialStage(err), fmt.Errorf("opening tunnel: %w", err)
	}
	defer conn.Close()

	//nolint:gosec // G402: verification is the caller's assertion; see the package comment.
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         sni,
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, stageInnerHandshake, fmt.Errorf("inner TLS handshake for %q: %w", sni, err)
	}
	defer tlsConn.Close()

	out := tlsConn.ConnectionState().PeerCertificates
	if len(out) == 0 {
		return nil, stageInnerHandshake, fmt.Errorf("handshake for %q completed with no peer certificates", sni)
	}
	return out, "", nil
}

// dialStage names the hop a DialContext failure stopped at. It reads atunnel's
// typed errors rather than its messages, so the mapping survives a reworded
// error.
func dialStage(err error) string {
	var rejected *atunnel.ConnectRejectedError
	switch {
	case errors.Is(err, atunnel.ErrGatewayHandshake):
		return stageGatewayTLS
	case errors.As(err, &rejected):
		return stageConnect
	default:
		return stageTunnel
	}
}

// serverName is the host half of a host:port address. It is written by hand
// rather than with net.SplitHostPort so that a malformed flag surfaces at
// handshake time with the address in the message, instead of at startup.
func serverName(address string) string {
	for i := len(address) - 1; i >= 0; i-- {
		if address[i] == ':' {
			return address[:i]
		}
	}
	return address
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("egressprobe: encoding response: %v", err)
	}
}

func main() {
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/handshake", handshake)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      2 * time.Minute,
	}
	log.Printf("egressprobe: listening on %s, gateway %s", *listenAddress, *gatewayAddress)
	log.Fatal(server.ListenAndServe())
}
