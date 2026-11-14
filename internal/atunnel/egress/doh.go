// Copyright 2026 The Agent Substrate Authors
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

package egress

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"

	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
)

// maxDNSMessageSize is the largest DNS message, set by the two-byte length
// that frames a message over TCP.
const maxDNSMessageSize = 65535

// ErrEgressNotActive reports that the actor has no usable egress client: none
// was activated, it was deactivated, or its certificate expired.
var ErrEgressNotActive = errors.New("egress: actor egress is not active")

// ExchangeDNS sends a DNS query in wire format to the gateway as an RFC 8484
// GET and returns the DNS response. A response other than 200 with a DNS
// message body is an error.
func (c *Client) ExchangeDNS(ctx context.Context, query []byte) ([]byte, error) {
	target := &url.URL{
		Scheme:   "https",
		Host:     c.gatewayAddress,
		Path:     protocol.DoHPath,
		RawQuery: protocol.DoHQueryParam + "=" + base64.RawURLEncoding.EncodeToString(query),
	}
	for {
		h2Conn, wasVerified, h1Conn, err := c.acquireConn(ctx)
		if err != nil {
			return nil, err
		}
		if h1Conn != nil {
			return exchangeDNSH1(ctx, h1Conn, target)
		}
		msg, retry, err := c.exchangeDNSH2(ctx, h2Conn, wasVerified, target)
		if retry {
			continue
		}
		return msg, err
	}
}

func newDNSRequest(ctx context.Context, target *url.URL) *http.Request {
	return (&http.Request{
		Method: http.MethodGet,
		URL:    target,
		Host:   target.Host,
		Header: http.Header{"Accept": {protocol.DoHMediaType}},
	}).WithContext(ctx)
}

// exchangeDNSH2 sends the query as one stream on a pooled connection. It
// reports retry when the connection died before the request was answered, as
// connectH2 does.
func (c *Client) exchangeDNSH2(ctx context.Context, h2Conn *h2ClientConn, wasVerified bool, target *url.URL) ([]byte, bool, error) {
	resp, err := h2Conn.cc.RoundTrip(newDNSRequest(ctx, target))
	if err != nil {
		if h2Conn.isClosed() {
			c.removeH2Conn(h2Conn)
			if h2Conn.cc.InFlight() == 0 {
				_ = h2Conn.cc.Close()
			}
		}
		if (wasVerified || h2Conn.retired.Load()) && ctx.Err() == nil && (h2Conn.isClosed() || h2Conn.cc.Available() == 0) {
			c.removeH2Conn(h2Conn)
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("egress: sending DNS query to egress gateway: %w", h2Conn.connErr(err))
	}
	h2Conn.verified.Store(true)
	msg, err := readDNSResponse(resp)
	return msg, false, err
}

// exchangeDNSH1 sends the query on a gateway connection that did not negotiate
// HTTP/2. The connection carries this one request.
func exchangeDNSH1(ctx context.Context, tlsConn *tls.Conn, target *url.URL) ([]byte, error) {
	defer tlsConn.Close()
	stop := context.AfterFunc(ctx, func() { _ = tlsConn.SetDeadline(time.Now()) })
	defer stop()

	req := newDNSRequest(ctx, target)
	req.Close = true
	if err := req.Write(tlsConn); err != nil {
		return nil, fmt.Errorf("egress: sending DNS query to egress gateway: %w", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(tlsConn), req)
	if err != nil {
		return nil, fmt.Errorf("egress: reading DNS response from egress gateway: %w", err)
	}
	return readDNSResponse(resp)
}

func readDNSResponse(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, fmt.Errorf("egress: gateway answered DNS query with %s: %s", resp.Status, body)
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != protocol.DoHMediaType {
		return nil, fmt.Errorf("egress: gateway answered DNS query with content type %q, want %s", resp.Header.Get("Content-Type"), protocol.DoHMediaType)
	}
	msg, err := io.ReadAll(io.LimitReader(resp.Body, maxDNSMessageSize+1))
	if err != nil {
		return nil, fmt.Errorf("egress: reading DNS response from egress gateway: %w", err)
	}
	if len(msg) > maxDNSMessageSize {
		return nil, fmt.Errorf("egress: gateway DNS response is larger than %d bytes", maxDNSMessageSize)
	}
	return msg, nil
}

// ExchangeDNS sends a DNS query to the gateway with the actor's egress client.
// It returns ErrEgressNotActive under the conditions that make handle drop a
// connection.
func (e *Proxy) ExchangeDNS(ctx context.Context, actorUID string, query []byte) ([]byte, error) {
	e.mu.Lock()
	active := e.active[actorUID]
	if active == nil || active.ctx.Err() != nil || active.dialer == nil || time.Now().Compare(active.expiresAt) >= 0 {
		e.mu.Unlock()
		return nil, ErrEgressNotActive
	}
	active.wg.Add(1)
	dialer := active.dialer
	e.mu.Unlock()
	defer active.wg.Done()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(active.ctx, cancel)
	defer stop()
	return dialer.ExchangeDNS(ctx, query)
}
