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

package dns

import (
	"errors"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
)

const (
	// defaultMaxUDPConnUses bounds how many queries a pooled UDP socket serves
	// before being closed and replaced with a new ephemeral port.
	defaultMaxUDPConnUses = 16
	// defaultMaxIdleUDPConns bounds how many idle UDP sockets remain pooled
	// after a concurrent burst subsides.
	defaultMaxIdleUDPConns = 8
)

type udpPoolEntry struct {
	uses int
	idle bool
}

// udpConnPool manages outbound UDP sockets bound to ephemeral ports and
// discards each socket after maxUses queries.
//
// listenPacket must return a comparable net.PacketConn implementation (such as
// *net.UDPConn).
type udpConnPool struct {
	listenPacket func(network, address string) (net.PacketConn, error)
	onNewConn    func(net.PacketConn)
	maxUses      int
	maxIdle      int

	mu      sync.Mutex
	idle    []net.PacketConn
	entries map[net.PacketConn]*udpPoolEntry
	closed  bool
}

func newUDPConnPool(listenPacket func(network, address string) (net.PacketConn, error)) *udpConnPool {
	if listenPacket == nil {
		listenPacket = net.ListenPacket
	}
	return &udpConnPool{
		listenPacket: listenPacket,
		maxUses:      defaultMaxUDPConnUses,
		maxIdle:      defaultMaxIdleUDPConns,
		entries:      make(map[net.PacketConn]*udpPoolEntry),
	}
}

// Get returns a random available PacketConn from the pool, or opens a new one
// on an ephemeral port if none is available.
func (p *udpConnPool) Get() (net.PacketConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, net.ErrClosed
	}

	if len(p.idle) > 0 {
		idx := rand.IntN(len(p.idle))
		conn := p.idle[idx]
		last := len(p.idle) - 1
		p.idle[idx] = p.idle[last]
		p.idle[last] = nil
		p.idle = p.idle[:last]
		entry := p.entries[conn]
		entry.idle = false
		entry.uses++
		return conn, nil
	}

	conn, err := p.listenPacket("udp", ":0")
	if err != nil {
		return nil, err
	}
	p.entries[conn] = &udpPoolEntry{uses: 1}
	if p.onNewConn != nil {
		p.onNewConn(conn)
	}
	return conn, nil
}

// Put returns conn to the pool, or closes and discards it once it reaches
// maxUses uses or the idle pool is full.
func (p *udpConnPool) Put(conn net.PacketConn) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	entry, ok := p.entries[conn]
	if !ok || entry.idle {
		p.mu.Unlock()
		return
	}
	maxUses := p.maxUses
	if maxUses <= 0 {
		maxUses = defaultMaxUDPConnUses
	}
	maxIdle := p.maxIdle
	if maxIdle <= 0 {
		maxIdle = defaultMaxIdleUDPConns
	}
	if p.closed || entry.uses >= maxUses || len(p.idle) >= maxIdle {
		delete(p.entries, conn)
		p.mu.Unlock()
		_ = conn.Close()
		return
	}
	entry.idle = true
	p.idle = append(p.idle, conn)
	p.mu.Unlock()
}

// Discard removes conn from the pool and closes it.
func (p *udpConnPool) Discard(conn net.PacketConn) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	if entry, ok := p.entries[conn]; ok {
		delete(p.entries, conn)
		if entry.idle {
			p.idle = slices.DeleteFunc(p.idle, func(c net.PacketConn) bool { return c == conn })
		}
	}
	p.mu.Unlock()

	_ = conn.Close()
}

// Close marks the pool closed and closes all idle and in-flight sockets.
func (p *udpConnPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	conns := make([]net.PacketConn, 0, len(p.entries))
	for conn := range p.entries {
		conns = append(conns, conn)
	}
	clear(p.entries)
	p.idle = nil
	p.mu.Unlock()

	var errs error
	for _, conn := range conns {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}
