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
	"log"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// newHTTPCmd is a plain HTTP/1.1 origin an Actor's egress lands on. It exists so
// a test can assert the destination port is recovered from SO_ORIGINAL_DST.
// It can also verify an injected Authorization header against a mounted token.
func newHTTPCmd() *cobra.Command {
	var listenAddress, authorizationFile string
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve a plain HTTP/1.1 origin answering /healthz.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			mux := http.NewServeMux()
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			if authorizationFile != "" {
				mux.HandleFunc("/credential", credentialHandler(authorizationFile))
			}

			server := &http.Server{
				Addr:              listenAddress,
				Handler:           mux,
				ReadHeaderTimeout: 10 * time.Second,
				WriteTimeout:      2 * time.Minute,
			}
			log.Printf("testserver http: listening on %s", listenAddress)
			return server.ListenAndServe()
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":8080", "Address the HTTP origin listens on.")
	cmd.Flags().StringVar(&authorizationFile, "authorization-file", "", "Enable /credential, requiring a Bearer token matching this file.")
	return cmd
}

func credentialHandler(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, err := os.ReadFile(path)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if len(token) == 0 || r.Header.Get("Authorization") != "Bearer "+string(token) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
