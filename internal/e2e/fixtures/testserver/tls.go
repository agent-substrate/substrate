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
	"crypto/tls"
	"fmt"
	"net/http"
)

// configureTLSServer loads the origin certificate and key and offers only
// HTTP/1.1 over TLS. Invalid credentials fail setup before the listener starts.
func configureTLSServer(server *http.Server, certFile, keyFile string) error {
	if certFile == "" && keyFile == "" {
		return nil
	}
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("--tls-cert and --tls-key must be provided together")
	}

	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("loading TLS certificate and key: %w", err)
	}
	server.TLSConfig = &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"http/1.1"},
	}
	return nil
}

func serveHTTP(server *http.Server, certFile, keyFile string) error {
	if certFile == "" && keyFile == "" {
		return server.ListenAndServe()
	}

	if err := configureTLSServer(server, certFile, keyFile); err != nil {
		return err
	}

	return server.ListenAndServeTLS("", "")
}
