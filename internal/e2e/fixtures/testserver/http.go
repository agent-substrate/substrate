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
	"io"
	"log"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

// newHTTPHandler is the HTTP/1.1 origin an Actor's egress lands on. It exists so
// a test can assert the destination port is recovered from SO_ORIGINAL_DST
// rather than defaulted from the URL scheme: the actor fetches its /healthz on a
// non-standard port, and the gateway's access log is expected to carry that
// port. The optional body makes the same fixture useful for assertions about a
// response inside the egress tunnel.
func newHTTPHandler(body string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if body != "" {
			_, _ = io.WriteString(w, body)
		}
	})
	return mux
}

func newHTTPCmd() *cobra.Command {
	var (
		listenAddress string
		certFile      string
		keyFile       string
		body          string
	)
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve an HTTP/1.1 origin answering /healthz.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			server := &http.Server{
				Addr:              listenAddress,
				Handler:           newHTTPHandler(body),
				ReadHeaderTimeout: 10 * time.Second,
				WriteTimeout:      2 * time.Minute,
			}
			log.Printf("testserver http: listening on %s", listenAddress)
			return serveHTTP(server, certFile, keyFile)
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":8080", "Address the HTTP origin listens on.")
	cmd.Flags().StringVar(&certFile, "tls-cert", "", "PEM certificate file; enables HTTPS when paired with --tls-key.")
	cmd.Flags().StringVar(&keyFile, "tls-key", "", "PEM private key file; enables HTTPS when paired with --tls-cert.")
	cmd.Flags().StringVar(&body, "body", "", "Response body for /healthz.")
	return cmd
}
