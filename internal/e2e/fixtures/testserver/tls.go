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
	"log"
	"net"
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

// tlsListenAddress is where the http and websocket subcommands serve HTTPS
// when given a certificate, beside their cleartext --listen. It is a fixed port.
const tlsListenAddress = ":8443"

// serveHTTP serves server in cleartext on server.Addr and, when certFile and
// keyFile are set, over TLS on tlsListenAddress.
func serveHTTP(server *http.Server, certFile, keyFile string) error {
	if err := configureTLSServer(server, certFile, keyFile); err != nil {
		return err
	}
	cleartext, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	var encrypted net.Listener
	if server.TLSConfig != nil {
		if encrypted, err = net.Listen("tcp", tlsListenAddress); err != nil {
			_ = cleartext.Close()
			return err
		}
		log.Printf("testserver: serving HTTPS on %s", tlsListenAddress)
	}
	return serveListeners(server, cleartext, encrypted)
}

// serveListeners serves server in cleartext on cleartext and, unless encrypted
// is nil, over TLS on encrypted, returning when either stops. The caller binds
// both before this starts serving either, so a readiness probe that reaches one
// listener finds the other already listening.
func serveListeners(server *http.Server, cleartext, encrypted net.Listener) error {
	errs := make(chan error, 2)
	go func() { errs <- server.Serve(cleartext) }()
	if encrypted != nil {
		go func() { errs <- server.ServeTLS(encrypted, "", "") }()
	}
	return <-errs
}
