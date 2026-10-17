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

// Command target is the endpoint the egress-tunnels actors call: every request
// gets a 200 with a small fixed body, over plain HTTP and, when given a
// certificate, HTTPS. One Deployment of it sits behind all of the test's
// Services. --listen-ports and --tls-listen-ports give each Service its own
// backend port: Services on distinct ClusterIPs that share one backend port
// can hand the target two connections with the same 4-tuple.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

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

func main() {
	listen := flag.String("listen", ":8080", "Address to serve plain HTTP on.")
	listenPorts := flag.String("listen-ports", "", "Also serve plain HTTP on each port of this range, A-B.")
	tlsListen := flag.String("tls-listen", "", "Address to serve HTTPS on; empty serves no HTTPS.")
	tlsListenPorts := flag.String("tls-listen-ports", "", "Also serve HTTPS on each port of this range, A-B.")
	tlsCertFile := flag.String("tls-cert-file", "", "PEM certificate to serve HTTPS with.")
	tlsKeyFile := flag.String("tls-key-file", "", "PEM private key of --tls-cert-file.")
	responseBytes := flag.Int("response-bytes", 16, "Size of the body every request gets back.")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	handler := newHandler(*responseBytes)
	plain := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	plainLns, err := openListeners(*listen, *listenPorts)
	if err != nil {
		slog.Error("opening HTTP listeners", "err", err)
		os.Exit(1)
	}
	servers := []*http.Server{plain}
	var tlsSrv *http.Server
	var tlsLns []net.Listener
	if *tlsListen != "" || *tlsListenPorts != "" {
		cfg, err := newTLSConfig(*tlsCertFile, *tlsKeyFile)
		if err != nil {
			slog.Error("loading the TLS certificate", "err", err)
			os.Exit(1)
		}
		tlsSrv = &http.Server{Handler: handler, TLSConfig: cfg, ReadHeaderTimeout: 10 * time.Second}
		if tlsLns, err = openListeners(*tlsListen, *tlsListenPorts); err != nil {
			slog.Error("opening HTTPS listeners", "err", err)
			os.Exit(1)
		}
		servers = append(servers, tlsSrv)
	}
	slog.Info("egress-tunnels target serving", "http_ports", len(plainLns), "https_ports", len(tlsLns))

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
	if err != nil {
		slog.Error("serving", "err", err)
		os.Exit(1)
	}
}
