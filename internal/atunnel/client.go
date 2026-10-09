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

package atunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// TODO(liorlieberman): support/use CONNECT on Ingress as well.
// ClientConfig configures an egress CONNECT client.
type ClientConfig struct {
	GatewayAddress       string
	ServerName           string
	GetClientCertificate func(*tls.CertificateRequestInfo) (*tls.Certificate, error)
	TrustBundlePath      string
}

// DialFunc dials a network address. It matches net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// ErrGatewayHandshake reports that the gateway's front door refused the
// connection at TLS: it rejected the client certificate, or its own
// certificate did not verify.
var ErrGatewayHandshake = errors.New("atunnel: egress gateway TLS handshake")

// ConnectRejectedError reports a CONNECT the gateway answered with a non-2xx
// status. The caller authenticated successfully and the request was declined
// anyway, which is what an authorization denial looks like from here. The
// status code is carried separately from the message because it is the part a
// caller can act on.
type ConnectRejectedError struct {
	StatusCode int
	// Status is the full status line, e.g. "403 Forbidden".
	Status string
	// Message is the response body, or the status text when the body is empty.
	Message string
}

func (e *ConnectRejectedError) Error() string {
	return fmt.Sprintf("atunnel: egress gateway rejected CONNECT with %s: %s", e.Status, e.Message)
}

// ClientOption customizes a Client beyond its configuration. Production
// callers need none of these.
type ClientOption func(*Client)

// WithDialer overrides how the client reaches the egress gateway. It exists so
// tests can substitute a transport; by default a net.Dialer is used.
func WithDialer(dial DialFunc) ClientOption {
	return func(c *Client) {
		if dial != nil {
			c.dialContext = dial
		}
	}
}

// Client opens actor egress streams through an mTLS-authenticated gateway.
type Client struct {
	gatewayAddress string
	tlsConfig      *tls.Config
	dialContext    DialFunc

	mu      sync.Mutex
	h2Conns []*h2ClientConn
	dialing *dialCall
}

// Client implements egressDialer.
var _ egressDialer = (*Client)(nil)

// NewClient creates an egress CONNECT client and validates its TLS material.
func NewClient(cfg ClientConfig, opts ...ClientOption) (*Client, error) {
	if _, _, err := net.SplitHostPort(cfg.GatewayAddress); err != nil {
		return nil, fmt.Errorf("atunnel: invalid egress gateway address %q: %w", cfg.GatewayAddress, err)
	}
	if cfg.ServerName == "" {
		return nil, fmt.Errorf("atunnel: egress gateway server name is required")
	}
	if cfg.GetClientCertificate == nil {
		return nil, fmt.Errorf("atunnel: client certificate source is required")
	}
	if cfg.TrustBundlePath == "" {
		return nil, fmt.Errorf("atunnel: trust bundle path is required")
	}
	trustPEM, err := os.ReadFile(cfg.TrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("atunnel: reading trust bundle: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("atunnel: trust bundle %q contains no certificates", cfg.TrustBundlePath)
	}

	client := &Client{
		gatewayAddress: cfg.GatewayAddress,
		dialContext:    (&net.Dialer{}).DialContext,
		tlsConfig: &tls.Config{
			MinVersion:           tls.VersionTLS12,
			RootCAs:              rootCAs,
			ServerName:           cfg.ServerName,
			GetClientCertificate: cfg.GetClientCertificate,
			NextProtos:           []string{"h2", "http/1.1"},
		},
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// DialContext opens a CONNECT tunnel to destination. destination becomes the
// request authority, so it must include an explicit port.
func (c *Client) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	if err := validateDestination(destination); err != nil {
		return nil, err
	}
	for {
		h2Conn, wasVerified, h1Conn, err := c.acquireConn(ctx)
		if err != nil {
			return nil, err
		}
		if h1Conn != nil {
			return c.connectH1(h1Conn, destination)
		}
		conn, retry, err := c.connectH2(ctx, h2Conn, wasVerified, destination)
		if retry {
			continue
		}
		return conn, err
	}
}

type dialCall struct {
	done chan struct{}

	mu      sync.Mutex
	waiters int
	cancel  context.CancelFunc
	h2Conn  *h2ClientConn
	h1Conn  *tls.Conn
	err     error
}

func (c *Client) acquireConn(ctx context.Context) (*h2ClientConn, bool, *tls.Conn, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, nil, fmt.Errorf("atunnel: connecting to egress gateway: %w", err)
		}

		// First check if there is an existing H/2 connection with available stream capacity.
		c.mu.Lock()
		keep := c.h2Conns[:0]
		var reserved *h2ClientConn
		var wasVerified bool
		for _, h2Conn := range c.h2Conns {
			if h2Conn.isClosed() {
				continue
			}
			if reserved == nil {
				if err := h2Conn.cc.Reserve(); err == nil {
					reserved = h2Conn
					wasVerified = h2Conn.verified.Load()
					keep = append(keep, h2Conn)
					continue
				}
				if h2Conn.isClosed() || h2Conn.cc.InFlight() == 0 {
					continue
				}
			}
			keep = append(keep, h2Conn)
		}
		c.h2Conns = keep
		if reserved != nil {
			c.mu.Unlock()
			return reserved, wasVerified, nil, nil
		}

		// If there are no H/2 connections with idle capacity, start a new connection.
		// Dial new connection if there are no in progress connections already .
		call := c.dialing
		if call == nil {
			dialCtx, cancel := context.WithCancel(context.Background())
			call = &dialCall{
				done:    make(chan struct{}),
				waiters: 1,
				cancel:  cancel,
			}
			c.dialing = call
			c.mu.Unlock()
			go c.runDial(dialCtx, call)
		} else {
			call.mu.Lock()
			call.waiters++
			call.mu.Unlock()
			c.mu.Unlock()
		}

		select {
		case <-call.done:
			call.mu.Lock()
			call.waiters--
			if call.h1Conn != nil {
				conn := call.h1Conn
				call.h1Conn = nil
				call.mu.Unlock()
				return nil, false, conn, nil
			}
			h2Conn := call.h2Conn
			err := call.err
			call.mu.Unlock()

			if err != nil {
				if ctx.Err() != nil {
					return nil, false, nil, fmt.Errorf("atunnel: connecting to egress gateway: %w", ctx.Err())
				}
				return nil, false, nil, err
			}
			if h2Conn != nil {
				wasVerified := h2Conn.verified.Load()
				if err := h2Conn.cc.Reserve(); err == nil {
					return h2Conn, wasVerified, nil, nil
				}
				if !wasVerified && h2Conn.isClosed() {
					c.removeH2Conn(h2Conn)
					return nil, false, nil, connectExchangeError("reading CONNECT response", h2Conn.connErr(nil))
				}
			}
		case <-ctx.Done():
			c.mu.Lock()
			call.mu.Lock()
			call.waiters--
			var orphan *tls.Conn
			if call.waiters == 0 {
				if c.dialing == call {
					c.dialing = nil
				}
				call.cancel()
				if call.h1Conn != nil {
					orphan = call.h1Conn
					call.h1Conn = nil
				}
			}
			call.mu.Unlock()
			c.mu.Unlock()
			if orphan != nil {
				_ = orphan.Close()
			}
			return nil, false, nil, fmt.Errorf("atunnel: connecting to egress gateway: %w", ctx.Err())
		}
	}
}

// Dial egress gateway and see what protocol was offered by egress gateway.
func (c *Client) runDial(dialCtx context.Context, call *dialCall) {
	defer call.cancel()

	var (
		h2Conn *h2ClientConn
		h1Conn *tls.Conn
		err    error
	)
	rawConn, dialErr := c.dialContext(dialCtx, "tcp", c.gatewayAddress)
	if dialErr != nil {
		err = fmt.Errorf("atunnel: connecting to egress gateway: %w", dialErr)
	} else {
		tlsConn := tls.Client(rawConn, c.tlsConfig.Clone())
		if hsErr := tlsConn.HandshakeContext(dialCtx); hsErr != nil {
			_ = rawConn.Close()
			err = fmt.Errorf("%w: %w", ErrGatewayHandshake, hsErr)
		} else if tlsConn.ConnectionState().NegotiatedProtocol == "h2" {
			h2Conn, err = c.newH2ClientConn(dialCtx, tlsConn)
		} else {
			h1Conn = tlsConn
		}
	}

	c.mu.Lock()
	call.mu.Lock()
	if c.dialing == call {
		c.dialing = nil
	}
	if call.waiters == 0 {
		call.mu.Unlock()
		c.mu.Unlock()
		if h2Conn != nil {
			_ = h2Conn.cc.Close()
		}
		if h1Conn != nil {
			_ = h1Conn.Close()
		}
		close(call.done)
		return
	}
	if h2Conn != nil {
		c.h2Conns = append(c.h2Conns, h2Conn)
		call.h2Conn = h2Conn
	}
	call.h1Conn = h1Conn
	call.err = err
	close(call.done)
	call.mu.Unlock()
	c.mu.Unlock()
}

func (c *Client) newH2ClientConn(ctx context.Context, tlsConn *tls.Conn) (*h2ClientConn, error) {
	raw := &trackedTLSConn{Conn: tlsConn}
	var protocols http.Protocols
	protocols.SetHTTP2(true)
	tr := &http.Transport{
		Protocols:          &protocols,
		DisableCompression: true,
		DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
			return raw, nil
		},
	}
	cc, err := tr.NewClientConn(ctx, "https", c.gatewayAddress)
	if err != nil {
		_ = raw.Close()
		connErr := raw.firstErr()
		if connErr == nil {
			connErr = err
		}
		return nil, connectExchangeError("writing CONNECT request", connErr)
	}
	return &h2ClientConn{
		cc:         cc,
		raw:        raw,
		localAddr:  tlsConn.LocalAddr(),
		remoteAddr: tlsConn.RemoteAddr(),
	}, nil
}

func (c *Client) removeH2Conn(target *h2ClientConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, h2Conn := range c.h2Conns {
		if h2Conn == target {
			c.h2Conns = append(c.h2Conns[:i], c.h2Conns[i+1:]...)
			return
		}
	}
}

func (c *Client) connectH1(tlsConn *tls.Conn, destination string) (net.Conn, error) {
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: destination},
		Host:   destination,
	}
	if err := req.Write(tlsConn); err != nil {
		_ = tlsConn.Close()
		return nil, connectExchangeError("writing CONNECT request", err)
	}

	reader := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = tlsConn.Close()
		return nil, connectExchangeError("reading CONNECT response", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		_ = tlsConn.Close()
		return nil, newConnectRejectedError(resp, body)
	}

	return &bufferedConn{Conn: tlsConn, reader: reader}, nil
}

func (c *Client) connectH2(ctx context.Context, h2Conn *h2ClientConn, wasVerified bool, destination string) (net.Conn, bool, error) {
	streamCtx, streamCancel := context.WithCancel(context.Background())
	reqBody := newH2RequestBody()
	traceCtx := httptrace.WithClientTrace(streamCtx, &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			reqBody.onWroteRequest(info.Err)
		},
	})
	stopCancel := context.AfterFunc(ctx, streamCancel)

	req := (&http.Request{
		Method:        http.MethodConnect,
		URL:           &url.URL{Host: destination},
		Host:          destination,
		Header:        make(http.Header),
		Body:          reqBody,
		ContentLength: -1,
	}).WithContext(traceCtx)

	resp, err := h2Conn.cc.RoundTrip(req)
	if !stopCancel() {
		reqBody.abort()
		streamCancel()
		if err == nil {
			_ = resp.Body.Close()
		}
		if !wasVerified && h2Conn.isClosed() {
			c.removeH2Conn(h2Conn)
			_ = h2Conn.cc.Close()
		}
		return nil, false, connectExchangeError("reading CONNECT response", ctx.Err())
	}
	if err != nil {
		reqBody.abort()
		streamCancel()
		if h2Conn.isClosed() {
			c.removeH2Conn(h2Conn)
			if h2Conn.cc.InFlight() == 0 {
				_ = h2Conn.cc.Close()
			}
		}
		if wasVerified && ctx.Err() == nil && (h2Conn.isClosed() || h2Conn.cc.Available() == 0) {
			c.removeH2Conn(h2Conn)
			return nil, true, nil
		}
		return nil, false, connectExchangeError("reading CONNECT response", h2Conn.connErr(err))
	}

	h2Conn.verified.Store(true)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		reqBody.abort()
		_ = resp.Body.Close()
		streamCancel()
		return nil, false, newConnectRejectedError(resp, body)
	}

	return newH2StreamConn(h2Conn, reqBody, resp.Body, streamCancel), false, nil
}

func newConnectRejectedError(resp *http.Response, body []byte) *ConnectRejectedError {
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(resp.StatusCode)
	}
	return &ConnectRejectedError{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Message:    message,
	}
}

type h2ClientConn struct {
	cc         *http.ClientConn
	raw        *trackedTLSConn
	localAddr  net.Addr
	remoteAddr net.Addr
	verified   atomic.Bool
}

func (h *h2ClientConn) isClosed() bool {
	return h.cc.Err() != nil || h.raw.firstErr() != nil
}

func (h *h2ClientConn) connErr(fallback error) error {
	if err := h.raw.firstErr(); err != nil {
		return err
	}
	return fallback
}

type trackedTLSConn struct {
	*tls.Conn
	mu     sync.Mutex
	closed bool
	err    error
}

func (c *trackedTLSConn) recordErr(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	if !c.closed && c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
}

func (c *trackedTLSConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		c.recordErr(err)
	}
	return n, err
}

func (c *trackedTLSConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil {
		c.recordErr(err)
	}
	return n, err
}

func (c *trackedTLSConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	if nc := c.Conn.NetConn(); nc != nil {
		t := time.AfterFunc(250*time.Millisecond, func() { _ = nc.Close() })
		defer t.Stop()
	}
	return c.Conn.Close()
}

func (c *trackedTLSConn) firstErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

type readSlot struct {
	buf   []byte
	resCh chan readResult
}

type readResult struct {
	n   int
	err error
}

type h2RequestBody struct {
	readyCh chan readSlot
	abortCh chan struct{}
	doneCh  chan struct{}

	abortOnce sync.Once
	doneOnce  sync.Once

	errMu    sync.Mutex
	writeErr error

	// Guarded by h2StreamConn.writeMu.
	pendingSlot *readSlot
	eofSent     bool
}

func newH2RequestBody() *h2RequestBody {
	return &h2RequestBody{
		readyCh: make(chan readSlot),
		abortCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

func (b *h2RequestBody) Read(buf []byte) (int, error) {
	resCh := make(chan readResult, 1)
	select {
	case b.readyCh <- readSlot{buf: buf, resCh: resCh}:
	case <-b.abortCh:
		return 0, io.EOF
	}
	select {
	case res := <-resCh:
		return res.n, res.err
	case <-b.abortCh:
		return 0, io.EOF
	}
}

func (b *h2RequestBody) Close() error {
	b.abort()
	b.finish(net.ErrClosed)
	return nil
}

func (b *h2RequestBody) onWroteRequest(err error) {
	b.finish(err)
}

func (b *h2RequestBody) finish(err error) {
	b.doneOnce.Do(func() {
		b.errMu.Lock()
		b.writeErr = err
		b.errMu.Unlock()
		close(b.doneCh)
	})
}

func (b *h2RequestBody) abort() {
	b.abortOnce.Do(func() {
		close(b.abortCh)
	})
}

func (b *h2RequestBody) err() error {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	if b.writeErr != nil {
		return b.writeErr
	}
	return net.ErrClosed
}

func (b *h2RequestBody) writeError() error {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	return b.writeErr
}

type readOutcome struct {
	buf []byte
	err error
}

type h2StreamConn struct {
	h2Conn   *h2ClientConn
	reqBody  *h2RequestBody
	respBody io.ReadCloser
	cancel   context.CancelFunc

	readMu       sync.Mutex
	readOnce     sync.Once
	readReqCh    chan int
	readResCh    chan readOutcome
	readInFlight bool
	readPending  []byte
	readErr      error

	writeMu sync.Mutex

	readDeadline  streamDeadline
	writeDeadline streamDeadline

	closeOnce sync.Once
	closedCh  chan struct{}
}

func newH2StreamConn(h2Conn *h2ClientConn, reqBody *h2RequestBody, respBody io.ReadCloser, cancel context.CancelFunc) *h2StreamConn {
	return &h2StreamConn{
		h2Conn:        h2Conn,
		reqBody:       reqBody,
		respBody:      respBody,
		cancel:        cancel,
		readReqCh:     make(chan int, 1),
		readResCh:     make(chan readOutcome, 1),
		readDeadline:  makeStreamDeadline(),
		writeDeadline: makeStreamDeadline(),
		closedCh:      make(chan struct{}),
	}
}

func (c *h2StreamConn) startReader() {
	c.readOnce.Do(func() {
		go func() {
			for {
				var size int
				select {
				case size = <-c.readReqCh:
				case <-c.closedCh:
					return
				}
				buf := make([]byte, size)
				n, err := c.respBody.Read(buf)
				c.readResCh <- readOutcome{buf: buf[:n], err: err}
				if err != nil {
					return
				}
			}
		}()
	})
}

func (c *h2StreamConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	if isClosedChan(c.closedCh) {
		return 0, net.ErrClosed
	}
	if isClosedChan(c.readDeadline.wait()) {
		return 0, os.ErrDeadlineExceeded
	}
	if len(c.readPending) > 0 {
		n := copy(p, c.readPending)
		c.readPending = c.readPending[n:]
		if len(c.readPending) == 0 && c.readErr != nil {
			err := c.readErr
			c.readErr = nil
			return n, err
		}
		return n, nil
	}
	if c.readErr != nil {
		return 0, c.readErr
	}
	if len(p) == 0 {
		return 0, nil
	}

	c.startReader()
	if !c.readInFlight {
		c.readInFlight = true
		c.readReqCh <- len(p)
	}

	select {
	case out := <-c.readResCh:
		c.readInFlight = false
		c.readErr = out.err
		n := copy(p, out.buf)
		c.readPending = out.buf[n:]
		if len(c.readPending) > 0 {
			return n, nil
		}
		return n, c.readErr
	case <-c.readDeadline.wait():
		return 0, os.ErrDeadlineExceeded
	case <-c.closedCh:
		return 0, net.ErrClosed
	}
}

func (c *h2StreamConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if isClosedChan(c.closedCh) {
		return 0, net.ErrClosed
	}
	if isClosedChan(c.writeDeadline.wait()) {
		return 0, os.ErrDeadlineExceeded
	}
	if c.reqBody.eofSent {
		return 0, net.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}

	var total int
	for len(p) > 0 {
		slot, err := c.acquireWriteSlot()
		if err != nil {
			return total, err
		}
		n := copy(slot.buf, p)
		p = p[n:]
		total += n
		slot.resCh <- readResult{n: n}

		select {
		case nextSlot := <-c.reqBody.readyCh:
			c.reqBody.pendingSlot = &nextSlot
		case <-c.reqBody.doneCh:
			return total, c.reqBody.err()
		case <-c.closedCh:
			return total, net.ErrClosed
		case <-c.writeDeadline.wait():
			return total, os.ErrDeadlineExceeded
		}
	}
	return total, nil
}

func (c *h2StreamConn) acquireWriteSlot() (readSlot, error) {
	if c.reqBody.pendingSlot != nil {
		slot := *c.reqBody.pendingSlot
		c.reqBody.pendingSlot = nil
		return slot, nil
	}
	select {
	case slot := <-c.reqBody.readyCh:
		return slot, nil
	case <-c.reqBody.doneCh:
		return readSlot{}, c.reqBody.err()
	case <-c.closedCh:
		return readSlot{}, net.ErrClosed
	case <-c.writeDeadline.wait():
		return readSlot{}, os.ErrDeadlineExceeded
	}
}

func (c *h2StreamConn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if isClosedChan(c.closedCh) {
		return net.ErrClosed
	}
	if c.reqBody.eofSent {
		return nil
	}
	var slot readSlot
	if c.reqBody.pendingSlot != nil {
		slot = *c.reqBody.pendingSlot
		c.reqBody.pendingSlot = nil
	} else {
		select {
		case slot = <-c.reqBody.readyCh:
		case <-c.reqBody.doneCh:
			c.reqBody.eofSent = true
			return c.reqBody.writeError()
		case <-c.closedCh:
			return net.ErrClosed
		}
	}
	c.reqBody.eofSent = true
	slot.resCh <- readResult{n: 0, err: io.EOF}
	select {
	case <-c.reqBody.doneCh:
		return c.reqBody.writeError()
	case <-c.closedCh:
		return nil
	}
}

func (c *h2StreamConn) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		if c.writeMu.TryLock() {
			if !c.reqBody.eofSent && c.reqBody.pendingSlot != nil {
				slot := *c.reqBody.pendingSlot
				c.reqBody.pendingSlot = nil
				c.reqBody.eofSent = true
				slot.resCh <- readResult{n: 0, err: io.EOF}
				select {
				case <-c.reqBody.doneCh:
				case <-time.After(100 * time.Millisecond):
				}
			}
			c.writeMu.Unlock()
		}
		close(c.closedCh)
		c.reqBody.abort()
		closeErr = c.respBody.Close()
		c.cancel()
	})
	return closeErr
}

func (c *h2StreamConn) LocalAddr() net.Addr {
	return c.h2Conn.localAddr
}

func (c *h2StreamConn) RemoteAddr() net.Addr {
	return c.h2Conn.remoteAddr
}

func (c *h2StreamConn) SetDeadline(t time.Time) error {
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *h2StreamConn) SetReadDeadline(t time.Time) error {
	c.readDeadline.set(t)
	return nil
}

func (c *h2StreamConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.set(t)
	return nil
}

type streamDeadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{}
}

func makeStreamDeadline() streamDeadline {
	return streamDeadline{cancel: make(chan struct{})}
}

func (d *streamDeadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel
	}
	d.timer = nil

	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}

	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		d.timer = time.AfterFunc(dur, func() {
			close(d.cancel)
		})
		return
	}

	if !closed {
		close(d.cancel)
	}
}

func (d *streamDeadline) wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// connectExchangeError wraps a failure that happened after the TLS handshake
// returned but before the gateway answered CONNECT, naming the front door as
// the cause when the connection was torn down rather than answered.
func connectExchangeError(op string, err error) error {
	if gatewayHungUp(err) {
		return fmt.Errorf("%w: %s: %w", ErrGatewayHandshake, op, err)
	}
	return fmt.Errorf("atunnel: %s: %w", op, err)
}

// gatewayHungUp reports whether err is the peer tearing the connection down,
// as opposed to a local failure such as a context deadline or a malformed
// response.
func gatewayHungUp(err error) bool {
	// A TLS alert from the peer -- "unknown certificate authority" is the one
	// a wrong client CA produces. crypto/tls reports an alert as a net.OpError
	// whose Op is "remote error" and whose Err is an unexported alert type, so
	// the Op is the only part of it that can be matched without matching on
	// the message text this package deliberately avoids.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "remote error" {
		return true
	}
	// Or the gateway closed or reset instead of alerting -- including a reset
	// that lands while the CONNECT request is still going out, which is what
	// makes the alert-versus-EPIPE outcome a race rather than a distinction.
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET)
}

func validateDestination(destination string) error {
	host, port, err := net.SplitHostPort(destination)
	if err != nil {
		return fmt.Errorf("atunnel: invalid egress destination %q: %w", destination, err)
	}
	if host == "" {
		return fmt.Errorf("atunnel: invalid egress destination %q: host is empty", destination)
	}
	if net.ParseIP(host) == nil {
		return fmt.Errorf("atunnel: invalid egress destination %q: host must be an IP address", destination)
	}
	if _, ok := ParsePort(port); !ok {
		return fmt.Errorf("atunnel: invalid egress destination %q: port must be between 1 and 65535", destination)
	}
	return nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *bufferedConn) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return nil
}
