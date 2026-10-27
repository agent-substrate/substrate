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
)

// Targets is where the generator sends traffic and with what credentials.
type Targets struct {
	Ingress       string
	Connect       string
	Egress        string
	EgressDiscard string
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

func (r *run) connectOnce(ctx context.Context, dialer *tls.Dialer, authority string) (int64, error) {
	conn, err := dialer.DialContext(ctx, "tcp", r.targets.Connect)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s: %s\r\n\r\n",
		authority, authority, atenet.TargetActorHeader, r.actor()); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("CONNECT %d", resp.StatusCode)
	}
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

// egressIdle opens up to connections connections at the pacer's rate,
// completes one request on each so its tunnel exists end to end, and holds
// them until the duration is over. whileHolding runs as soon as every open
// has finished, while the connections are still held.
func (r *run) egressIdle(ctx context.Context, p *pacer) error {
	var mu sync.Mutex
	var held []net.Conn
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	}()
	end := time.Now().Add(r.opts.duration)
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
	request := "GET / HTTP/1.1\r\nHost: " + r.targets.Egress + "\r\n\r\n"
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	p.run(ctx, func(ctx context.Context, u unit) {
		if opened.Add(1) > int64(r.opts.connections) {
			return
		}
		defer finish()
		conn, err := dialer.DialContext(ctx, "tcp", r.targets.Egress)
		if err != nil {
			r.fail(err)
			return
		}
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		resp, err := func() (*http.Response, error) {
			if _, err := io.WriteString(conn, request); err != nil {
				return nil, err
			}
			return http.ReadResponse(bufio.NewReader(conn), nil)
		}()
		if err != nil {
			r.fail(err)
			_ = conn.Close()
			return
		}
		n, _ := io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		_ = conn.SetDeadline(time.Time{})
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
	payload := make([]byte, r.opts.payloadBytes)
	start := time.Now()
	end, measureFrom := start.Add(r.opts.duration), start.Add(r.opts.warmup)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var wg sync.WaitGroup
	for range r.opts.connections {
		wg.Go(func() {
			conn, err := dialer.DialContext(ctx, "tcp", r.targets.EgressDiscard)
			if err != nil {
				r.fail(err)
				return
			}
			defer conn.Close()
			for time.Now().Before(end) && ctx.Err() == nil {
				due := time.Now()
				_ = conn.SetWriteDeadline(due.Add(10 * time.Second))
				if _, err := conn.Write(payload); err != nil {
					r.fail(err)
					return
				}
				r.done(unit{due: due, measured: due.After(measureFrom)}, int64(len(payload)))
			}
		})
	}
	wg.Wait()
	return nil
}
