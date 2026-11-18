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
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
	"go.ate.dev/substrate/internal/resources"
)

// dnsDecisionHeader tells atunnel what the gateway did with a DNS query. The
// DNS response carries the same outcome; the header is for logs.
const dnsDecisionHeader = "x-ate-dns-decision"

// Values of dnsDecisionHeader.
const (
	dnsDecisionAllowed = "allowed"
	dnsDecisionBlocked = "blocked"
	dnsDecisionError   = "error"
)

const (
	// dnsForwardTimeout bounds resolving one query. It is below the egress
	// listener's ext_proc message_timeout, so the actor gets a SERVFAIL and
	// not the 504 Envoy sends when the callout times out.
	dnsForwardTimeout = 4 * time.Second
	// dnsAttemptTimeout bounds one upstream when another remains.
	dnsAttemptTimeout = time.Second
)

// isDNSQuery reports whether the request is an RFC 8484 GET.
func isDNSQuery(md *extproc.RequestMetadata) bool {
	path, _, _ := strings.Cut(md.Path, "?")
	return md.Method == http.MethodGet && path == protocol.DoHPath
}

// handleDNSQuery answers a DNS query atunnel sent for an actor. A name the
// actor's EgressPolicy names in any rule is resolved; any other name, and
// every name for an actor without a policy, is answered NXDOMAIN. Both are
// HTTP 200 with a DNS message, as RFC 8484 section 4.2.1 asks.
func (h *Handler) handleDNSQuery(ctx context.Context, md *extproc.RequestMetadata, leg string) (extproc.Result, error) {
	ref, err := h.authenticateActor(ctx, md, "DNS")
	if err != nil {
		return extproc.Result{}, err
	}
	query, hdr, q, err := parseDNSQuery(md.Path)
	if err != nil {
		slog.WarnContext(ctx, "egress denied: invalid DNS query", slog.Any("actor", ref), slog.Any("err", err))
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_BadRequest, "invalid DNS query")
	}

	start := time.Now()
	decision, resp, err := h.answerDNSQuery(ctx, leg, ref, query, hdr, q)
	if err != nil {
		return extproc.Result{}, err
	}
	attrs := []any{
		slog.Any("actor", ref),
		slog.String("name", q.Name.String()),
		slog.String("type", q.Type.String()),
		slog.String("decision", decision),
		slog.Duration("elapsed", time.Since(start)),
	}
	// An allowed query is the common case and is logged at Debug.
	if decision == dnsDecisionAllowed {
		slog.DebugContext(ctx, "egress DNS query", attrs...)
	} else {
		slog.InfoContext(ctx, "egress DNS query", attrs...)
	}
	return extproc.Result{
		Immediate: extproc.ImmediateResponseWithBody(envoy_type.StatusCode_OK, protocol.DoHMediaType, resp,
			map[string]string{dnsDecisionHeader: decision}),
	}, nil
}

// answerDNSQuery returns the decision on query and the DNS response for it.
// An error is a client-facing denial: the policy could not be read.
func (h *Handler) answerDNSQuery(ctx context.Context, leg string, ref resources.ActorRef, query []byte, hdr dnsmessage.Header, q dnsmessage.Question) (string, []byte, error) {
	policy, err := h.lookupPolicy(ctx, leg, ref)
	var denial *extproc.ReqError
	switch {
	case errors.As(err, &denial) && denial.StatusCode == int(envoy_type.StatusCode_Forbidden):
		// No policy, or one with no rules: nothing resolves.
		return dnsDecisionBlocked, blockedDNSResponse(query, hdr, q), nil
	case err != nil:
		return "", nil, err
	case !policy.AllowsHostname(q.Name.String()):
		return dnsDecisionBlocked, blockedDNSResponse(query, hdr, q), nil
	}

	resp, err := h.dns.exchange(ctx, query)
	if err != nil {
		slog.WarnContext(ctx, "egress DNS query could not be resolved", slog.Any("actor", ref), slog.String("name", q.Name.String()), slog.Any("err", err))
		return dnsDecisionError, protocol.ErrorPacket(hdr, dnsmessage.RCodeServerFailure, &q), nil
	}
	return dnsDecisionAllowed, resp, nil
}

// parseDNSQuery decodes the DNS query in the dns parameter of path and checks
// that it is a standard query with one question.
func parseDNSQuery(path string) ([]byte, dnsmessage.Header, dnsmessage.Question, error) {
	fail := func(err error) ([]byte, dnsmessage.Header, dnsmessage.Question, error) {
		return nil, dnsmessage.Header{}, dnsmessage.Question{}, err
	}
	u, err := url.ParseRequestURI(path)
	if err != nil {
		return fail(err)
	}
	query, err := base64.RawURLEncoding.DecodeString(u.Query().Get(protocol.DoHQueryParam))
	if err != nil {
		return fail(fmt.Errorf("decoding the %s parameter: %w", protocol.DoHQueryParam, err))
	}
	if !protocol.RequestSanityCheck(query) || protocol.QDCount(query) != 1 {
		return fail(errors.New("not a DNS query with one question"))
	}
	var p dnsmessage.Parser
	hdr, err := p.Start(query)
	if err != nil {
		return fail(err)
	}
	if hdr.OpCode != 0 {
		return fail(fmt.Errorf("opcode %d is not QUERY", hdr.OpCode))
	}
	q, err := p.Question()
	if err != nil {
		return fail(err)
	}
	return query, hdr, q, nil
}

// blockedEDE is an EDNS OPT record that carries an Extended DNS Error with
// info-code 15, Blocked (RFC 8914 section 4.16).
var blockedEDE = []byte{
	0x00,       // root owner name
	0x00, 0x29, // TYPE OPT
	0x04, 0xd0, // CLASS: 1232 byte UDP payload size
	0x00, 0x00, 0x00, 0x00, // extended RCODE and flags
	0x00, 0x06, // RDLENGTH
	0x00, 0x0f, // OPTION-CODE: Extended DNS Error
	0x00, 0x02, // OPTION-LENGTH
	0x00, 0x0f, // INFO-CODE: Blocked
}

// blockedDNSResponse returns the response to a query that policy does not
// allow: an NXDOMAIN that resolvers do not cache. A query that carries EDNS
// also gets an Extended DNS Error saying the name is blocked.
func blockedDNSResponse(query []byte, hdr dnsmessage.Header, q dnsmessage.Question) []byte {
	resp := protocol.NXDomainPacket(hdr, q)
	if !protocol.HasEDNS(query) {
		return resp
	}
	resp = append(resp, blockedEDE...)
	resp[11]++ // ARCOUNT
	return resp
}

// dnsForwarder resolves DNS queries with upstream resolvers.
type dnsForwarder struct {
	// upstreams are tried in order, each "host:port".
	upstreams []string
	dialer    net.Dialer
}

// exchange sends query to the upstreams and returns the first usable response.
// It moves to the next upstream when one does not answer, or answers SERVFAIL,
// NOTIMP or REFUSED.
func (f *dnsForwarder) exchange(ctx context.Context, query []byte) ([]byte, error) {
	if len(f.upstreams) == 0 {
		return nil, errors.New("no DNS upstream is configured")
	}
	ctx, cancel := context.WithTimeout(ctx, dnsForwardTimeout)
	defer cancel()

	// atunnel sends ID 0. A random ID keeps the 16 bits of spoofing
	// protection on the upstream exchange.
	clientID := protocol.TxnID(query)
	out := bytes.Clone(query)
	protocol.SetTxnID(out, uint16(rand.Uint32()))

	var (
		resp []byte
		errs error
	)
	for i, upstream := range f.upstreams {
		last := i == len(f.upstreams)-1
		attemptCtx, cancelAttempt := ctx, context.CancelFunc(func() {})
		if !last {
			attemptCtx, cancelAttempt = context.WithTimeout(ctx, dnsAttemptTimeout)
		}
		answer, err := f.exchangeWith(attemptCtx, upstream, out)
		cancelAttempt()
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("%s: %w", upstream, err))
			continue
		}
		resp = answer
		if last || !isFailoverRCode(dnsmessage.RCode(answer[3]&0x0f)) {
			break
		}
	}
	if resp == nil {
		return nil, errs
	}
	protocol.SetTxnID(resp, clientID)
	return resp, nil
}

func isFailoverRCode(rcode dnsmessage.RCode) bool {
	switch rcode {
	case dnsmessage.RCodeServerFailure, dnsmessage.RCodeNotImplemented, dnsmessage.RCodeRefused:
		return true
	}
	return false
}

// exchangeWith asks one upstream over UDP, and again over TCP when the UDP
// response is truncated.
func (f *dnsForwarder) exchangeWith(ctx context.Context, upstream string, query []byte) ([]byte, error) {
	resp, err := f.exchangeOver(ctx, "udp", upstream, query)
	if err != nil {
		return nil, err
	}
	const truncated = 0x02
	if resp[2]&truncated == 0 {
		return resp, nil
	}
	return f.exchangeOver(ctx, "tcp", upstream, query)
}

func (f *dnsForwarder) exchangeOver(ctx context.Context, network, upstream string, query []byte) ([]byte, error) {
	conn, err := f.dialer.DialContext(ctx, network, upstream)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	if network == "tcp" {
		if err := protocol.WriteTCPFrame(conn, query); err != nil {
			return nil, err
		}
		resp, err := protocol.ReadTCPFrame(conn)
		if err != nil {
			return nil, err
		}
		if !isResponseTo(resp, query) {
			return nil, errors.New("upstream sent a response to a different query")
		}
		return resp, nil
	}

	if _, err := conn.Write(query); err != nil {
		return nil, err
	}
	buf := make([]byte, protocol.UDPPayloadSize(query))
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}
		// Anything else on this socket is stale or spoofed. Keep waiting.
		if isResponseTo(buf[:n], query) {
			return buf[:n], nil
		}
	}
}

// isResponseTo reports whether resp is a DNS response with query's ID.
func isResponseTo(resp, query []byte) bool {
	return protocol.ResponseSanityCheck(resp) && protocol.TxnID(resp) == protocol.TxnID(query)
}
