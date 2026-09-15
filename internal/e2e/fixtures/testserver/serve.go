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

package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

// protocolListener is one protocol the serve subcommand speaks, and the address
// it speaks it on. Adding a protocol to the shared origin is an entry in the
// list newServeCmd builds and the flag that supplies its address.
type protocolListener struct {
	// name appears in the logs and in the error a failed bind returns, which is
	// the whole diagnosis when a pod exits before it is ever probed.
	name    string
	address string
	// serve takes ownership of the bound listener and blocks. Returning at all
	// is a failure: none of these servers are ever asked to shut down.
	serve func(net.Listener) error
}

// newServeCmd is the shared origin the networking suite dials: one pod speaking
// every protocol those tests need, each on a port of its own.
//
// One pod rather than one per protocol because an origin is scaffolding — the
// tests are about what happens between the actor and here — and standing one up
// costs a namespace, an image push, a schedule and a readiness wait each time.
//
// A port per protocol rather than one port that sniffs what arrived, because
// sniffing is a behavior of its own: it delays the first byte and it can be
// wrong, and either would be indistinguishable from the tunnel under test
// misbehaving. The port a test dials is the protocol it gets.
//
// Which is also why this is a subcommand rather than a flag on `http`: the
// existing single-protocol subcommands stay exactly as narrow as they were, so
// a fixture that wants nothing but a gRPC listener still has one.
func newServeCmd() *cobra.Command {
	var httpAddress, grpcAddress string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve several protocols at once, each on its own port.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			protocols := []protocolListener{{
				// HTTP carries readiness and the websocket upgrade, which is
				// HTTP and so wants no port of its own.
				name:    "http",
				address: httpAddress,
				serve: func(listener net.Listener) error {
					server := &http.Server{
						Handler:           newServeHandler(),
						ReadHeaderTimeout: 10 * time.Second,
						WriteTimeout:      2 * time.Minute,
					}
					return server.Serve(listener)
				},
			}}
			if grpcAddress != "" {
				// Nothing but gRPC on this port: cleartext HTTP/2 with no HTTP
				// handler multiplexed in front of it, so a test can tell a
				// tunnel that mangled the frames from a server that answered.
				protocols = append(protocols, protocolListener{
					name:    "grpc",
					address: grpcAddress,
					serve:   newServer().Serve,
				})
			}
			return serveAll(protocols)
		},
	}
	cmd.Flags().StringVar(&httpAddress, "listen", ":8080", "Address the HTTP/1.1 listener binds, serving /healthz, /readyz and the /ws upgrade. This is the port kubelet probes.")
	cmd.Flags().StringVar(&grpcAddress, "grpc", "", "Address for a cleartext HTTP/2 gRPC listener. Empty serves no gRPC.")
	return cmd
}

// newServeHandler is the HTTP/1.1 side of the shared origin: readiness on both
// spellings the fixtures use, and the websocket upgrade.
func newServeHandler() http.Handler {
	mux := http.NewServeMux()
	ready := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	mux.HandleFunc("/healthz", ready)
	mux.HandleFunc("/readyz", ready)
	mux.HandleFunc("/ws", echoWebsocket)
	return mux
}

// serveAll binds every listener before serving any of them, and returns as soon
// as one stops.
//
// Binding first is what lets one readiness probe speak for several ports. A
// container gets one probe, kubelet aims it at the primary port, and a test
// that sees Ready goes straight on to dial the others — so a listener opened
// after the probe started answering is a connection refused, arriving as a
// failure of whatever sits between the test and here. Bound up front, a port
// that cannot be had is instead a process that never becomes ready at all, and
// says which port and why.
func serveAll(protocols []protocolListener) error {
	bound := make([]net.Listener, 0, len(protocols))
	defer func() {
		for _, listener := range bound {
			_ = listener.Close()
		}
	}()
	for _, protocol := range protocols {
		listener, err := net.Listen("tcp", protocol.address)
		if err != nil {
			return fmt.Errorf("listening for %s on %s: %w", protocol.name, protocol.address, err)
		}
		log.Printf("testserver serve: %s on %s", protocol.name, listener.Addr())
		bound = append(bound, listener)
	}

	stopped := make(chan error, len(protocols))
	for i, protocol := range protocols {
		go func() {
			stopped <- fmt.Errorf("the %s listener on %s stopped serving: %v", protocol.name, protocol.address, protocol.serve(bound[i]))
		}()
	}
	return <-stopped
}
