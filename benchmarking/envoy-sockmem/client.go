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
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/internal/atunnel/egress"
)

// Modes, the same for both kinds of Envoy, named from Envoy's side: the
// client is downstream and the origin upstream. The slow modes hold conns
// connections whose reader stalls after drain bytes; the others measure
// throughput over conns connections.
const (
	slowDownstream = "slow-downstream" // the origin sends without end, the client stalls
	slowUpstream   = "slow-upstream"   // the client sends without end, the origin stalls
	download       = "download"
	upload         = "upload"
)

// originTunnelIP is the destination the client's egress tunnels name. Nothing
// listens on it; the egress-like Envoy routes each port to the origin.
const originTunnelIP = "192.0.2.10"

type clientConfig struct {
	kind     string
	mode     string
	pkiDir   string
	conns    int
	drain    int64
	hold     time.Duration
	fill     time.Duration
	openRate float64
	// envoyCgroup is Envoy's cgroup directory, seen through its process's
	// root when the client shares its PID namespace.
	envoyCgroup string
	envoyAdmin  string
	originAdmin string
	label       string
}

// result is one client run, printed as a JSON line.
type result struct {
	Kind       string `json:"kind"`
	Mode       string `json:"mode"`
	Conns      int    `json:"conns"`
	Opened     int64  `json:"opened"`
	Errors     int64  `json:"errors"`
	FirstError string `json:"first_error,omitempty"`
	Before     sample `json:"before"`
	// Holding is the peak of each figure while the connections are held, or
	// the sample after streaming.
	Holding  sample  `json:"holding"`
	MBPerSec float64 `json:"mb_per_sec,omitempty"`
	// EnvoyCPUSecPerGB is Envoy's CPU time over the bytes moved.
	EnvoyCPUSecPerGB float64 `json:"envoy_cpu_sec_per_gb,omitempty"`
}

// sample is Envoy's memory at one instant: its container's cgroup, and the
// heap its allocator reports.
type sample struct {
	CgroupBytes     uint64  `json:"cgroup_bytes"`
	CgroupSockBytes uint64  `json:"cgroup_sock_bytes"`
	CgroupAnonBytes uint64  `json:"cgroup_anon_bytes"`
	CPUSeconds      float64 `json:"cpu_seconds"`
	HeapAllocated   uint64  `json:"heap_allocated"`
	HeapSize        uint64  `json:"heap_size"`
}

func (c clientConfig) sample(ctx context.Context) (sample, error) {
	var s sample
	read := func(name string) (string, error) {
		b, err := os.ReadFile(filepath.Join(c.envoyCgroup, name))
		return string(b), err
	}
	current, err := read("memory.current")
	if err != nil {
		return s, err
	}
	if s.CgroupBytes, err = strconv.ParseUint(strings.TrimSpace(current), 10, 64); err != nil {
		return s, fmt.Errorf("reading Envoy's memory.current: %w", err)
	}
	stat, err := read("memory.stat")
	if err != nil {
		return s, err
	}
	s.CgroupSockBytes, s.CgroupAnonBytes = statField(stat, "sock"), statField(stat, "anon")
	cpu, err := read("cpu.stat")
	if err != nil {
		return s, err
	}
	s.CPUSeconds = float64(statField(cpu, "usage_usec")) / 1e6

	// /memory reads the allocator now; the server.memory_* gauges are only
	// as fresh as the last stats flush.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.envoyAdmin+"/memory", nil)
	if err != nil {
		return s, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	var memory struct {
		Allocated string `json:"allocated"`
		HeapSize  string `json:"heap_size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&memory); err != nil {
		return s, fmt.Errorf("reading Envoy's /memory: %w", err)
	}
	s.HeapAllocated, _ = strconv.ParseUint(memory.Allocated, 10, 64)
	s.HeapSize, _ = strconv.ParseUint(memory.HeapSize, 10, 64)
	return s, nil
}

func statField(stat, field string) uint64 {
	scanner := bufio.NewScanner(strings.NewReader(stat))
	for scanner.Scan() {
		var name string
		var value uint64
		if _, err := fmt.Sscanf(scanner.Text(), "%s %d", &name, &value); err == nil && name == field {
			return value
		}
	}
	return 0
}

func runClient(ctx context.Context, c clientConfig) error {
	if c.mode == "ready" {
		// run-docker.sh polls this until Envoy serves.
		return getOK(ctx, c.envoyAdmin+"/ready")
	}
	caPath := filepath.Join(c.pkiDir, caFile)
	cert, err := tls.LoadX509KeyPair(filepath.Join(c.pkiDir, clientCertFile), filepath.Join(c.pkiDir, clientKeyFile))
	if err != nil {
		return err
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(caPEM)

	var open func(ctx context.Context) (net.Conn, error)
	var after func(net.Conn) error
	end := time.Now().Add(c.hold)
	switch c.kind {
	case "egress":
		client, err := egress.NewClient(egress.ClientConfig{
			GatewayAddress:       net.JoinHostPort(envoyHost, "443"),
			ServerName:           envoyHost,
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil },
			TrustBundlePath:      caPath,
		})
		if err != nil {
			return err
		}
		defer client.Close()
		port := map[string]int{slowDownstream: sourcePort, slowUpstream: stallPort, download: sourcePort, upload: discardPort}[c.mode]
		if port == 0 {
			return fmt.Errorf("unknown mode %q", c.mode)
		}
		// atunnel's client sends an IP destination, as an actor's
		// intercepted connection has; Envoy routes on it to the origin.
		destination := net.JoinHostPort(originTunnelIP, strconv.Itoa(port))
		open = func(ctx context.Context) (net.Conn, error) { return client.DialContext(ctx, destination) }
		switch c.mode {
		case slowDownstream:
			after = func(conn net.Conn) error { return readN(conn, conn, c.drain) }
		case slowUpstream:
			after = func(conn net.Conn) error {
				if err := readN(conn, conn, 1); err != nil {
					return err
				}
				go writeForever(conn, nil)
				return nil
			}
		}
	case "ingress":
		tlsConfig := &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
			RootCAs:      roots,
			ServerName:   envoyHost,
			NextProtos:   []string{"http/1.1"},
		}
		dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tlsConfig}
		header := "Host: actor\r\n"
		request := map[string]string{
			slowDownstream: "GET /stream HTTP/1.1\r\n" + header + "\r\n",
			download:       "GET /stream HTTP/1.1\r\n" + header + "\r\n",
			slowUpstream:   fmt.Sprintf("POST /stall?until=%d&drain=%d HTTP/1.1\r\n%sContent-Length: %d\r\n\r\n", end.Unix()+1, c.drain, header, int64(1)<<40),
			upload:         fmt.Sprintf("POST /discard HTTP/1.1\r\n%sContent-Length: %d\r\n\r\n", header, int64(1)<<40),
		}[c.mode]
		if request == "" {
			return fmt.Errorf("unknown mode %q", c.mode)
		}
		open = func(ctx context.Context) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(envoyHost, "8443"))
			if err != nil {
				return nil, err
			}
			if _, err := io.WriteString(conn, request); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		}
		switch c.mode {
		case slowDownstream:
			after = func(conn net.Conn) error { return readN(conn, conn, c.drain) }
		case slowUpstream:
			after = func(conn net.Conn) error {
				// The origin answers before it stalls.
				if err := readN(conn, conn, 1); err != nil {
					return err
				}
				go writeForever(conn, nil)
				return nil
			}
		}
	default:
		return fmt.Errorf("--kind must be ingress or egress, got %q", c.kind)
	}

	res := result{Kind: c.kind, Mode: c.mode, Conns: c.conns}
	if after != nil {
		if c.kind == "egress" && c.mode == slowUpstream {
			// The origin stops stalling as the hold ends, so nothing it
			// holds lingers into the next measurement. An ingress
			// slow-upstream request carries the same in its query.
			if err := post(ctx, fmt.Sprintf("%s/stall?until=%d&drain=%d", c.originAdmin, end.Unix()+1, c.drain)); err != nil {
				return fmt.Errorf("setting the origin's stall: %w", err)
			}
		}
		err = c.holdConns(ctx, &res, end, open, after)
	} else {
		err = c.stream(ctx, &res, open)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(labeled{Label: c.label, result: res})
}

// holdConns opens c.conns connections at c.openRate, holds them until end, and
// samples Envoy before the first open and, for the peak, from c.fill after the
// last until end.
func (c clientConfig) holdConns(ctx context.Context, res *result, end time.Time, open func(context.Context) (net.Conn, error), after func(net.Conn) error) error {
	var err error
	if res.Before, err = c.sample(ctx); err != nil {
		return err
	}
	var mu sync.Mutex
	var held []net.Conn
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	var opened, failed atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	interval := time.Duration(float64(time.Second) / c.openRate)
	for range c.conns {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
		wg.Go(func() {
			conn, err := open(ctx)
			if err == nil {
				if err = after(conn); err != nil {
					_ = conn.Close()
				}
			}
			if err != nil {
				failed.Add(1)
				firstErr.CompareAndSwap(nil, err.Error())
				return
			}
			opened.Add(1)
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		})
	}
	wg.Wait()
	res.Opened, res.Errors = opened.Load(), failed.Load()
	if e, ok := firstErr.Load().(string); ok {
		res.FirstError = e
	}
	// Queues behind slow readers can take seconds to fill, so sample every
	// second from c.fill after the last open until the hold ends, and keep
	// each figure's peak.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.fill):
	}
	for {
		s, err := c.sample(ctx)
		if err != nil {
			return err
		}
		res.Holding = peak(res.Holding, s)
		if time.Until(end) < time.Second {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
}

// peak is the larger of a and b, figure by figure.
func peak(a, b sample) sample {
	return sample{
		CgroupBytes:     max(a.CgroupBytes, b.CgroupBytes),
		CgroupSockBytes: max(a.CgroupSockBytes, b.CgroupSockBytes),
		CgroupAnonBytes: max(a.CgroupAnonBytes, b.CgroupAnonBytes),
		CPUSeconds:      max(a.CPUSeconds, b.CPUSeconds),
		HeapAllocated:   max(a.HeapAllocated, b.HeapAllocated),
		HeapSize:        max(a.HeapSize, b.HeapSize),
	}
}

// stream moves bytes as fast as it can over c.conns connections for c.hold,
// reading for the download modes and writing for the upload modes.
func (c clientConfig) stream(ctx context.Context, res *result, open func(context.Context) (net.Conn, error)) error {
	isUpload := c.mode == upload
	var moved, failed atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	conns := make([]net.Conn, 0, c.conns)
	for range c.conns {
		conn, err := open(ctx)
		if err != nil {
			return err
		}
		conns = append(conns, conn)
	}
	before, err := c.sample(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	deadline := start.Add(c.hold)
	for _, conn := range conns {
		wg.Go(func() {
			defer conn.Close()
			_ = conn.SetDeadline(deadline)
			buf := make([]byte, chunk)
			for {
				var n int
				var err error
				if isUpload {
					n, err = conn.Write(buf)
				} else {
					n, err = conn.Read(buf)
				}
				moved.Add(int64(n))
				if err != nil {
					if time.Now().Before(deadline) {
						failed.Add(1)
						firstErr.CompareAndSwap(nil, err.Error())
					}
					return
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	after, err := c.sample(ctx)
	if err != nil {
		return err
	}
	res.Before, res.Holding = before, after
	res.Opened, res.Errors = int64(len(conns)), failed.Load()
	if e, ok := firstErr.Load().(string); ok {
		res.FirstError = e
	}
	res.MBPerSec = float64(moved.Load()) / elapsed / 1e6
	if moved.Load() > 0 {
		res.EnvoyCPUSecPerGB = (after.CPUSeconds - before.CPUSeconds) / (float64(moved.Load()) / 1e9)
	}
	return nil
}

// readN reads n bytes, at least one, from r within 30s.
func readN(conn net.Conn, r io.Reader, n int64) error {
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.CopyN(io.Discard, r, max(1, n)); err != nil {
		return err
	}
	return conn.SetDeadline(time.Time{})
}

func getOK(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	return nil
}

func post(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s: HTTP %d", url, resp.StatusCode)
	}
	return nil
}
