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

package load

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/report"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/sink"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/credbundle"
)

// Mode is a kind of traffic the generator sends.
type Mode string

const (
	// IngressH1 sends HTTPS requests over HTTP/1.1 to atunnel's ingress
	// listener, as the router does.
	IngressH1 Mode = "ingress-h1"
	// IngressH2 is IngressH1 over HTTP/2.
	IngressH2 Mode = "ingress-h2"
	// Connect opens a TLS connection and a CONNECT tunnel to atunnel's CONNECT
	// listener per unit and sends one request through it.
	Connect Mode = "connect"
	// EgressHTTP sends plain HTTP to atunnel's egress listener, as an actor's
	// redirected connections arrive there.
	EgressHTTP Mode = "egress-http"
	// EgressIdle opens connections to the egress listener, completes one
	// request on each, and holds them open.
	EgressIdle Mode = "egress-idle"
	// EgressStream writes bytes as fast as it can over a few connections to
	// the egress discard listener.
	EgressStream Mode = "egress-stream"
	// EgressStreamDown reads as fast as it can over a few connections from
	// the gateway's endless writer.
	EgressStreamDown Mode = "egress-stream-down"
	// ConnectStream reads as fast as it can over a few CONNECT tunnels to
	// actors writing without end; ConnectStreamUp writes into them instead.
	ConnectStream   Mode = "connect-stream"
	ConnectStreamUp Mode = "connect-stream-up"
	// EgressSlowRead holds egress connections the gateway streams into
	// without end, reading one byte from each and then nothing.
	EgressSlowRead Mode = "egress-slow-read"
	// EgressSlowWrite holds egress connections the gateway never reads,
	// writing into each without end.
	EgressSlowWrite Mode = "egress-slow-write"
	// ConnectSlowRead holds CONNECT tunnels to an actor streaming without
	// end, reading one byte from each and then nothing.
	ConnectSlowRead Mode = "connect-slow-read"
	// ConnectSlowWrite holds CONNECT tunnels to an actor that never reads,
	// writing into each without end.
	ConnectSlowWrite Mode = "connect-slow-write"
)

// holds reports whether a mode holds connections rather than sending units at
// a rate.
func holds(m Mode) bool {
	switch m {
	case EgressIdle, EgressSlowRead, EgressSlowWrite, ConnectSlowRead, ConnectSlowWrite:
		return true
	}
	return false
}

// Targets is where the generator sends traffic and with what credentials.
type Targets struct {
	Ingress       string
	Connect       string
	Egress        string
	EgressDiscard string
	// EgressSource and EgressStall reach the gateway's endless writer and
	// its reader that never reads.
	EgressSource string
	EgressStall  string
	// ServerName is the name atunnel's ingress certificate is checked for.
	ServerName string
	// RouterBundle is the router client certificate; CA verifies atunnel.
	RouterBundle string
	CA           string
	// Actors are atespace/name values, cycled through the routing header.
	Actors []string
	// ActorPort is the port a CONNECT names.
	ActorPort int
}

// options configure one run of a mode.
type options struct {
	mode     Mode
	rate     float64
	duration time.Duration
	warmup   time.Duration
	workers  int
	// connections caps pooled connections for the HTTP modes, and is the
	// count to hold for EgressIdle and to stream over for EgressStream.
	connections int
	// newConnectionPerRequest turns keep-alive off for the HTTP modes.
	newConnectionPerRequest bool
	payloadBytes            int
	// drainBytes is what a slow-peer mode's stalled reader reads at full
	// speed before it stalls.
	drainBytes int64
	// whileHolding is called by EgressIdle with every connection still open,
	// once the opens are done and before they are closed.
	whileHolding func(context.Context)
}

// run is one execution of a mode.
type run struct {
	opts      options
	targets   Targets
	latency   samples
	completed atomic.Uint64
	errors    atomic.Uint64
	bytes     atomic.Uint64
	nextActor atomic.Uint64

	samplesMu    sync.Mutex
	errorSamples []string
}

const maxErrorSamples = 5

func (r *run) fail(err error) {
	r.errors.Add(1)
	r.samplesMu.Lock()
	defer r.samplesMu.Unlock()
	if len(r.errorSamples) < maxErrorSamples {
		r.errorSamples = append(r.errorSamples, err.Error())
	}
}

// done records a completed unit if it falls in the measured window.
func (r *run) done(u unit, n int64) {
	if !u.measured {
		return
	}
	r.latency.record(time.Since(u.due))
	r.completed.Add(1)
	r.bytes.Add(uint64(n))
}

func (r *run) actor() string {
	return r.targets.Actors[(r.nextActor.Add(1)-1)%uint64(len(r.targets.Actors))]
}

// execute runs opts against targets and summarizes what it saw. It returns an
// error only when the run could not start.
func execute(ctx context.Context, targets Targets, opts options) (report.Load, error) {
	r := &run{opts: opts, targets: targets}
	p := &pacer{rate: opts.rate, duration: opts.duration, warmup: opts.warmup, workers: opts.workers, maxPending: 10000}
	var err error
	switch opts.mode {
	case IngressH1, IngressH2:
		err = r.ingress(ctx, p)
	case Connect:
		err = r.connect(ctx, p)
	case EgressHTTP:
		err = r.egressHTTP(ctx, p)
	case EgressIdle:
		err = r.egressIdle(ctx, p)
	case EgressStream:
		err = r.egressStream(ctx)
	case EgressStreamDown:
		err = r.egressStreamDown(ctx)
	case ConnectStream, ConnectStreamUp:
		err = r.connectStream(ctx, opts.mode == ConnectStreamUp)
	case EgressSlowRead:
		err = r.egressSlow(ctx, p, r.targets.EgressSource, false)
	case EgressSlowWrite:
		err = r.egressSlow(ctx, p, r.targets.EgressStall, true)
	case ConnectSlowRead, ConnectSlowWrite:
		err = r.connectSlow(ctx, p)
	default:
		err = fmt.Errorf("unknown mode %q", opts.mode)
	}
	if err != nil {
		return report.Load{}, err
	}
	measured := (opts.duration - opts.warmup).Seconds()
	l := report.Load{
		Offered:         opts.rate,
		MeasuredSeconds: measured,
		Scheduled:       p.scheduled.Load(),
		Skipped:         p.skipped.Load(),
		Completed:       r.completed.Load(),
		Errors:          r.errors.Load(),
		Latency:         r.latency.percentiles(),
		SendLag:         p.lag.percentiles(),
		Bytes:           r.bytes.Load(),
		ErrorSamples:    r.errorSamples,
	}
	if measured > 0 {
		l.Achieved = float64(l.Completed) / measured
		l.MBytesPerSec = float64(l.Bytes) / measured / 1e6
	}
	return l, nil
}

// routerTLS is the client TLS configuration the router presents to atunnel.
func (r *run) routerTLS(nextProtos ...string) (*tls.Config, error) {
	cert, err := credbundle.Parse(r.targets.RouterBundle)
	if err != nil {
		return nil, err
	}
	roots, err := credbundle.ParsePool(r.targets.CA)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{*cert},
		RootCAs:      roots,
		ServerName:   r.targets.ServerName,
		NextProtos:   nextProtos,
	}, nil
}

func (r *run) transport(tlsConfig *tls.Config) *http.Transport {
	return &http.Transport{
		TLSClientConfig:     tlsConfig,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		MaxIdleConns:        r.opts.connections,
		MaxIdleConnsPerHost: r.opts.connections,
		MaxConnsPerHost:     r.opts.connections,
		DisableKeepAlives:   r.opts.newConnectionPerRequest,
		IdleConnTimeout:     90 * time.Second,
	}
}

// get sends one GET and drains the response.
func (r *run) get(ctx context.Context, client *http.Client, u unit, url, actor string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		r.fail(err)
		return
	}
	if actor != "" {
		req.Header.Set(atenet.TargetActorHeader, actor)
	}
	resp, err := client.Do(req)
	if err != nil {
		r.fail(err)
		return
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		r.fail(fmt.Errorf("HTTP %d", resp.StatusCode))
		return
	}
	r.done(u, n)
}

func (r *run) ingress(ctx context.Context, p *pacer) error {
	proto := "http/1.1"
	if r.opts.mode == IngressH2 {
		proto = "h2"
	}
	tlsConfig, err := r.routerTLS(proto)
	if err != nil {
		return err
	}
	transport := r.transport(tlsConfig)
	transport.ForceAttemptHTTP2 = r.opts.mode == IngressH2
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	url := "https://" + r.targets.Ingress + "/"
	p.run(ctx, func(ctx context.Context, u unit) {
		r.get(ctx, client, u, url, r.actor())
	})
	return nil
}

// connect costs a TLS handshake, a CONNECT, and atunnel's dial to the actor per
// unit, as a new router tunnel would.
func (r *run) connect(ctx context.Context, p *pacer) error {
	tlsConfig, err := r.routerTLS("http/1.1")
	if err != nil {
		return err
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tlsConfig}
	authority := net.JoinHostPort("actor", strconv.Itoa(r.targets.ActorPort))
	p.run(ctx, func(ctx context.Context, u unit) {
		n, err := r.connectOnce(ctx, dialer, authority)
		if err != nil {
			r.fail(err)
			return
		}
		r.done(u, n)
	})
	return nil
}

// openTunnel opens a TLS connection to atunnel's CONNECT listener and a
// tunnel through it to the next actor. The connection's deadline is 30s
// from now.
func (r *run) openTunnel(ctx context.Context, dialer *tls.Dialer, authority string) (net.Conn, *bufio.Reader, error) {
	conn, err := dialer.DialContext(ctx, "tcp", r.targets.Connect)
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s: %s\r\n\r\n",
		authority, authority, atenet.TargetActorHeader, r.actor()); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("CONNECT %d", resp.StatusCode)
	}
	return conn, reader, nil
}

func (r *run) connectOnce(ctx context.Context, dialer *tls.Dialer, authority string) (int64, error) {
	conn, reader, err := r.openTunnel(ctx, dialer, authority)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: actor\r\nConnection: close\r\n\r\n"); err != nil {
		return 0, err
	}
	inner, err := http.ReadResponse(reader, nil)
	if err != nil {
		return 0, err
	}
	defer inner.Body.Close()
	n, _ := io.Copy(io.Discard, inner.Body)
	if inner.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d inside tunnel", inner.StatusCode)
	}
	return n, nil
}

func (r *run) egressHTTP(ctx context.Context, p *pacer) error {
	transport := r.transport(nil)
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	url := "http://" + r.targets.Egress + "/"
	p.run(ctx, func(ctx context.Context, u unit) {
		r.get(ctx, client, u, url, "")
	})
	return nil
}

// opener opens one connection to hold and returns it with the bytes it read.
type opener func(ctx context.Context) (net.Conn, int64, error)

// egressIdle holds connections that each completed one request, so each
// tunnel exists end to end and is then idle.
func (r *run) egressIdle(ctx context.Context, p *pacer) error {
	request := "GET / HTTP/1.1\r\nHost: " + r.targets.Egress + "\r\n\r\n"
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return r.hold(ctx, p, time.Now().Add(r.opts.duration), func(ctx context.Context) (net.Conn, int64, error) {
		conn, err := dialer.DialContext(ctx, "tcp", r.targets.Egress)
		if err != nil {
			return nil, 0, err
		}
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		resp, err := func() (*http.Response, error) {
			if _, err := io.WriteString(conn, request); err != nil {
				return nil, err
			}
			return http.ReadResponse(bufio.NewReader(conn), nil)
		}()
		if err != nil {
			_ = conn.Close()
			return nil, 0, err
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		_ = conn.SetDeadline(time.Time{})
		return conn, n, nil
	})
}

// slowChunk is what a slow-peer mode writes at a time.
const slowChunk = 64 << 10

// The slow-peer modes' stalled readers first read drainBytes at full speed,
// so the kernel has grown the queues behind them, then nothing until the
// hold ends. The ingress actor learns both from its request's query; the
// gateway from the runner, through its admin endpoint.

// egressSlow holds egress connections to destination. With write, the
// gateway stalls, as the runner set it to, and the actor writes into each
// without end; without, the gateway streams and the actor reads drainBytes,
// then nothing.
func (r *run) egressSlow(ctx context.Context, p *pacer, destination string, write bool) error {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	end := time.Now().Add(r.opts.duration)
	return r.hold(ctx, p, end, func(ctx context.Context) (net.Conn, int64, error) {
		conn, err := dialer.DialContext(ctx, "tcp", destination)
		if err != nil {
			return nil, 0, err
		}
		if write {
			// The gateway answers one byte, then stalls.
			if err := readN(conn, conn, 1); err != nil {
				_ = conn.Close()
				return nil, 0, err
			}
			go writeUntilClosed(conn)
			return conn, 1, nil
		}
		if err := readN(conn, conn, max(1, r.opts.drainBytes)); err != nil {
			_ = conn.Close()
			return nil, 0, err
		}
		return conn, max(1, r.opts.drainBytes), nil
	})
}

// connectSlow holds CONNECT tunnels to the actors. For ConnectSlowRead the
// actor streams into each and the generator reads drainBytes, then nothing;
// for ConnectSlowWrite the generator writes without end and the actor stalls.
func (r *run) connectSlow(ctx context.Context, p *pacer) error {
	tlsConfig, err := r.routerTLS("http/1.1")
	if err != nil {
		return err
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tlsConfig}
	authority := net.JoinHostPort("actor", strconv.Itoa(r.targets.ActorPort))
	end := time.Now().Add(r.opts.duration)
	write := r.opts.mode == ConnectSlowWrite
	request := "GET " + sink.StreamPath + " HTTP/1.1\r\nHost: actor\r\n\r\n"
	want := max(1, r.opts.drainBytes)
	if write {
		request = fmt.Sprintf("POST %s?until=%d&drain=%d HTTP/1.1\r\nHost: actor\r\nContent-Length: %d\r\n\r\n",
			sink.StallPath, end.Unix()+1, r.opts.drainBytes, int64(1)<<40)
		// The actor answers before it stalls.
		want = 1
	}
	return r.hold(ctx, p, end, func(ctx context.Context) (net.Conn, int64, error) {
		conn, reader, err := r.openTunnel(ctx, dialer, authority)
		if err != nil {
			return nil, 0, err
		}
		if _, err := io.WriteString(conn, request); err != nil {
			_ = conn.Close()
			return nil, 0, err
		}
		if err := readN(conn, reader, want); err != nil {
			_ = conn.Close()
			return nil, 0, err
		}
		if write {
			go writeUntilClosed(conn)
		}
		return conn, want, nil
	})
}

// readN reads n bytes from r within 30s, then clears conn's deadline.
func readN(conn net.Conn, r io.Reader, n int64) error {
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.CopyN(io.Discard, r, n); err != nil {
		return err
	}
	return conn.SetDeadline(time.Time{})
}

// writeUntilClosed writes to conn until a write fails, as when the hold ends
// and closes it.
func writeUntilClosed(conn net.Conn) {
	chunk := make([]byte, slowChunk)
	for {
		if _, err := conn.Write(chunk); err != nil {
			return
		}
	}
}

// hold opens up to connections connections with open at the pacer's rate and
// holds them until end. whileHolding runs as soon as every open has finished,
// while the connections are still held.
func (r *run) hold(ctx context.Context, p *pacer, end time.Time, open opener) error {
	var mu sync.Mutex
	var held []net.Conn
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	}()
	var opened, finished atomic.Int64
	allOpened := make(chan struct{})
	finish := func() {
		if finished.Add(1) == int64(r.opts.connections) {
			close(allOpened)
		}
	}
	hookDone := make(chan struct{})
	go func() {
		defer close(hookDone)
		select {
		case <-allOpened:
		case <-ctx.Done():
			return
		case <-time.After(time.Until(end)):
		}
		if r.opts.whileHolding != nil {
			r.opts.whileHolding(ctx)
		}
	}()
	p.run(ctx, func(ctx context.Context, u unit) {
		if opened.Add(1) > int64(r.opts.connections) {
			return
		}
		defer finish()
		conn, n, err := open(ctx)
		if err != nil {
			r.fail(err)
			return
		}
		r.done(u, n)
		mu.Lock()
		held = append(held, conn)
		mu.Unlock()
	})
	// Hold for whatever is left of the duration, and until the hook is done.
	select {
	case <-ctx.Done():
	case <-time.After(time.Until(end)):
	}
	<-hookDone
	return nil
}

// egressStream writes payloads over connections connections for the duration.
// Each write is a unit, so latency is per write.
func (r *run) egressStream(ctx context.Context) error {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return r.stream(ctx, true, func(ctx context.Context) (net.Conn, io.Reader, error) {
		conn, err := dialer.DialContext(ctx, "tcp", r.targets.EgressDiscard)
		return conn, conn, err
	})
}

// egressStreamDown reads what the gateway sends without end over connections
// connections for the duration. Each read is a unit.
func (r *run) egressStreamDown(ctx context.Context) error {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return r.stream(ctx, false, func(ctx context.Context) (net.Conn, io.Reader, error) {
		conn, err := dialer.DialContext(ctx, "tcp", r.targets.EgressSource)
		return conn, conn, err
	})
}

// connectStream reads from, or writes to, the actors through CONNECT tunnels
// over connections connections for the duration.
func (r *run) connectStream(ctx context.Context, upload bool) error {
	tlsConfig, err := r.routerTLS("http/1.1")
	if err != nil {
		return err
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tlsConfig}
	authority := net.JoinHostPort("actor", strconv.Itoa(r.targets.ActorPort))
	request := "GET " + sink.StreamPath + " HTTP/1.1\r\nHost: actor\r\n\r\n"
	if upload {
		request = fmt.Sprintf("POST %s HTTP/1.1\r\nHost: actor\r\nContent-Length: %d\r\n\r\n", sink.DiscardPath, int64(1)<<40)
	}
	return r.stream(ctx, upload, func(ctx context.Context) (net.Conn, io.Reader, error) {
		conn, reader, err := r.openTunnel(ctx, dialer, authority)
		if err != nil {
			return nil, nil, err
		}
		_ = conn.SetDeadline(time.Time{})
		if _, err := io.WriteString(conn, request); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		return conn, reader, nil
	})
}

// stream opens connections connections with open and, for the duration,
// writes payloads into each, or reads from each what it returns. Each write
// or read is a unit.
func (r *run) stream(ctx context.Context, upload bool, open func(context.Context) (net.Conn, io.Reader, error)) error {
	buf := make([]byte, r.opts.payloadBytes)
	start := time.Now()
	end, measureFrom := start.Add(r.opts.duration), start.Add(r.opts.warmup)
	var wg sync.WaitGroup
	for range r.opts.connections {
		wg.Go(func() {
			conn, reader, err := open(ctx)
			if err != nil {
				r.fail(err)
				return
			}
			defer conn.Close()
			for time.Now().Before(end) && ctx.Err() == nil {
				due := time.Now()
				var n int
				if upload {
					_ = conn.SetWriteDeadline(due.Add(10 * time.Second))
					n, err = conn.Write(buf)
				} else {
					_ = conn.SetReadDeadline(due.Add(10 * time.Second))
					n, err = reader.Read(buf)
				}
				if err != nil {
					r.fail(err)
					return
				}
				r.done(unit{due: due, measured: due.After(measureFrom)}, int64(n))
			}
		})
	}
	wg.Wait()
	return nil
}
