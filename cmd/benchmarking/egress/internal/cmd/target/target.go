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

// Package target is the endpoint every tunnel of the egress scale test leads
// to: the subcommand that answers each request with a small fixed body.
package target

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// targetOptions are the flags of the target subcommand.
type targetOptions struct {
	listen, listenPorts       string
	tlsListen, tlsListenPorts string
	tlsCertFile, tlsKeyFile   string
	responseBytes             int
}

// Command is the endpoint the benchmark-egress actors call: every request
// gets a 200 with a small fixed body, over plain HTTP and, when given a
// certificate, HTTPS. One Deployment of it sits behind all of the test's
// Services. --listen-ports and --tls-listen-ports give each Service its own
// backend port: Services on distinct ClusterIPs that share one backend port
// can hand the target two connections with the same 4-tuple.
func Command() *cobra.Command {
	var o targetOptions
	cmd := &cobra.Command{
		Use:   "target",
		Short: "Answer every request the actors' tunnels carry with a small fixed body",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTarget(cmd.Context(), o)
		},
	}
	cmd.Flags().StringVar(&o.listen, "listen", ":8080", "Address to serve plain HTTP on.")
	cmd.Flags().StringVar(&o.listenPorts, "listen-ports", "", "Also serve plain HTTP on each port of this range, A-B.")
	cmd.Flags().StringVar(&o.tlsListen, "tls-listen", "", "Address to serve HTTPS on; empty serves no HTTPS.")
	cmd.Flags().StringVar(&o.tlsListenPorts, "tls-listen-ports", "", "Also serve HTTPS on each port of this range, A-B.")
	cmd.Flags().StringVar(&o.tlsCertFile, "tls-cert-file", "", "PEM certificate to serve HTTPS with.")
	cmd.Flags().StringVar(&o.tlsKeyFile, "tls-key-file", "", "PEM private key of --tls-cert-file.")
	cmd.Flags().IntVar(&o.responseBytes, "response-bytes", 16, "Size of the body every request gets back.")
	return cmd
}

func newHandler(responseBytes int) http.Handler {
	body := bytes.Repeat([]byte("x"), responseBytes)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write(body)
	})
	return mux
}

// newTLSConfig serves the certificate in certFile with the key in keyFile.
func newTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading TLS certificate: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// portRange parses "A-B" or "A" into the ports A through B; "" is none.
func portRange(s string) ([]int, error) {
	if s == "" {
		return nil, nil
	}
	lo, hi, found := strings.Cut(s, "-")
	if !found {
		hi = lo
	}
	a, errA := strconv.Atoi(lo)
	b, errB := strconv.Atoi(hi)
	if errA != nil || errB != nil || a < 1 || b > 65535 || a > b {
		return nil, fmt.Errorf("%q is not a port or a port range A-B", s)
	}
	ports := make([]int, 0, b-a+1)
	for p := a; p <= b; p++ {
		ports = append(ports, p)
	}
	return ports, nil
}

// openListeners opens addr, when set, and every port in ports on all interfaces.
// On an error it closes what it opened.
func openListeners(addr, ports string) ([]net.Listener, error) {
	addrs, err := listenAddrs(addr, ports)
	if err != nil {
		return nil, err
	}
	return listenAll(addrs)
}

// listenAddrs is addr, when set, followed by every port of ports on the
// wildcard address.
func listenAddrs(addr, ports string) ([]string, error) {
	ps, err := portRange(ports)
	if err != nil {
		return nil, err
	}
	var addrs []string
	if addr != "" {
		addrs = append(addrs, addr)
	}
	for _, p := range ps {
		addrs = append(addrs, ":"+strconv.Itoa(p))
	}
	return addrs, nil
}

// listenAll listens on every address, or on none.
func listenAll(addrs []string) ([]net.Listener, error) {
	lns := make([]net.Listener, 0, len(addrs))
	for _, a := range addrs {
		ln, err := net.Listen("tcp", a)
		if err != nil {
			for _, l := range lns {
				l.Close()
			}
			return nil, fmt.Errorf("listening on %s: %w", a, err)
		}
		lns = append(lns, ln)
	}
	return lns, nil
}

// runTarget serves until ctx ends or a listener fails.
func runTarget(ctx context.Context, o targetOptions) error {
	handler := newHandler(o.responseBytes)
	plain := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	plainLns, err := openListeners(o.listen, o.listenPorts)
	if err != nil {
		return fmt.Errorf("opening HTTP listeners: %w", err)
	}
	servers := []*http.Server{plain}
	var tlsSrv *http.Server
	var tlsLns []net.Listener
	if o.tlsListen != "" || o.tlsListenPorts != "" {
		cfg, err := newTLSConfig(o.tlsCertFile, o.tlsKeyFile)
		if err != nil {
			return err
		}
		tlsSrv = &http.Server{Handler: handler, TLSConfig: cfg, ReadHeaderTimeout: 10 * time.Second}
		if tlsLns, err = openListeners(o.tlsListen, o.tlsListenPorts); err != nil {
			return fmt.Errorf("opening HTTPS listeners: %w", err)
		}
		servers = append(servers, tlsSrv)
	}
	slog.Info("benchmark-egress target serving", "http_ports", len(plainLns), "https_ports", len(tlsLns))

	// The first listener to fail stops them all, as the signal does.
	failed := make(chan error, len(plainLns)+len(tlsLns))
	var wg sync.WaitGroup
	serve := func(ln net.Listener, run func(net.Listener) error) {
		wg.Go(func() {
			if err := run(ln); !errors.Is(err, http.ErrServerClosed) {
				failed <- fmt.Errorf("serving on %s: %w", ln.Addr(), err)
			}
		})
	}
	for _, ln := range plainLns {
		serve(ln, plain.Serve)
	}
	for _, ln := range tlsLns {
		serve(ln, func(ln net.Listener) error { return tlsSrv.ServeTLS(ln, "", "") })
	}

	select {
	case <-ctx.Done():
	case err = <-failed:
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}
	wg.Wait()
	return err
}
