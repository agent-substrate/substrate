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

package atepg

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"
)

// connectForPing connects to the shared container with a fresh schema that is
// dropped when the test ends. The returned config lets a test adjust the DSN
// or pool size before connecting.
func connectForPing(t *testing.T, schema string, adjust func(*ConnectConfig)) *Persistence {
	t.Helper()
	admin := requirePool(t)
	ctx := t.Context()
	drop := fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema)
	if _, err := admin.Exec(ctx, drop); err != nil {
		t.Fatalf("resetting PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), drop) })

	cfg := testConnectConfig(schema)
	adjust(&cfg)
	p, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	t.Cleanup(func() {
		p.Close()
		p.pool.Close()
	})
	return p
}

// A saturated request pool must not fail the readiness probe: the probe cannot
// tell a busy replica from an unreachable store if it borrows from that pool.
func TestPingIgnoresSaturatedRequestPool(t *testing.T) {
	p := connectForPing(t, "ping-saturated", func(cfg *ConnectConfig) { cfg.PoolMaxConns = 1 })
	ctx := t.Context()

	if p.probePool == p.pool {
		t.Fatal("probe pool is the request pool")
	}
	if got := p.probePool.Config().MaxConns; got != probePoolMaxConns {
		t.Errorf("probe pool MaxConns = %d, want %d", got, probePoolMaxConns)
	}
	if got := p.probePool.Config().MinConns; got != probePoolMinConns {
		t.Errorf("probe pool MinConns = %d, want %d", got, probePoolMinConns)
	}

	held, err := p.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquiring the only request connection: %v", err)
	}
	defer held.Release()

	budget := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(ctx, 500*time.Millisecond)
	}

	// The premise: the request pool really is exhausted, and a ping through it
	// blocks past the readiness budget.
	poolCtx, cancel := budget()
	defer cancel()
	if err := p.pool.Ping(poolCtx); err == nil {
		t.Fatal("request pool ping succeeded while every connection was held; the pool is not saturated")
	}

	probeCtx, cancel := budget()
	defer cancel()
	if err := p.Ping(probeCtx); err != nil {
		t.Errorf("Ping with a saturated request pool = %v, want nil", err)
	}
}

func TestPingFailsAfterClose(t *testing.T) {
	p := connectForPing(t, "ping-closed", func(*ConnectConfig) {})
	if err := p.Ping(t.Context()); err != nil {
		t.Fatalf("Ping before Close = %v, want nil", err)
	}
	p.Close()
	if err := p.Ping(t.Context()); err == nil {
		t.Error("Ping after Close succeeded, want an error from the closed probe pool")
	}
}

// Ping fails while PostgreSQL does not answer and passes again once it does,
// with no restart of the pool.
func TestPingFollowsStoreReachability(t *testing.T) {
	requirePool(t)
	dsn, err := url.Parse(containerDSN)
	if err != nil {
		t.Fatalf("parsing PostgreSQL DSN: %v", err)
	}
	proxy := newTCPProxy(t, dsn.Host)
	dsn.Host = proxy.addr
	p := connectForPing(t, "ping-reachability", func(cfg *ConnectConfig) { cfg.ReadWriteDSN = dsn.String() })

	ping := func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		return p.Ping(ctx)
	}
	if err := ping(); err != nil {
		t.Fatalf("Ping with the store reachable = %v, want nil", err)
	}
	proxy.stop()
	if err := ping(); err == nil {
		t.Error("Ping with the store unreachable succeeded, want an error")
	}
	proxy.start()
	if err := ping(); err != nil {
		t.Errorf("Ping after the store became reachable = %v, want nil", err)
	}
}

// tcpProxy forwards TCP connections to target until stopped, and start brings
// it back on the same address.
type tcpProxy struct {
	t      *testing.T
	target string
	addr   string

	mu    sync.Mutex
	ln    net.Listener
	conns []net.Conn
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	p := &tcpProxy{t: t, target: target, addr: "127.0.0.1:0"}
	p.start()
	t.Cleanup(p.stop)
	return p
}

func (p *tcpProxy) start() {
	p.t.Helper()
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		p.t.Fatalf("listening on %s: %v", p.addr, err)
	}
	p.mu.Lock()
	p.ln = ln
	p.addr = ln.Addr().String()
	p.mu.Unlock()
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			go p.forward(client)
		}
	}()
}

func (p *tcpProxy) forward(client net.Conn) {
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		client.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, client, upstream)
	p.mu.Unlock()
	go func() {
		io.Copy(upstream, client) //nolint:errcheck // Either side closing ends the copy.
		upstream.Close()
	}()
	io.Copy(client, upstream) //nolint:errcheck // Either side closing ends the copy.
	client.Close()
}

// stop refuses new connections and drops the open ones.
func (p *tcpProxy) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		p.ln.Close()
		p.ln = nil
	}
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}
