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
	"net"
	"sync/atomic"
	"testing"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
)

const testActorSPIFFE = "spiffe://substrate-actor.local/worker-for-actor/foo/bar"

// testDNSQuery builds an A query for name with ID 0, as atunnel sends it.
func testDNSQuery(t *testing.T, name string, edns bool) []byte {
	t.Helper()
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{RecursionDesired: true})
	if err := b.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := b.Question(dnsmessage.Question{Name: dnsmessage.MustNewName(name), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}); err != nil {
		t.Fatal(err)
	}
	if edns {
		if err := b.StartAdditionals(); err != nil {
			t.Fatal(err)
		}
		var opt dnsmessage.ResourceHeader
		if err := opt.SetEDNS0(1232, dnsmessage.RCodeSuccess, false); err != nil {
			t.Fatal(err)
		}
		if err := b.OPTResource(opt, dnsmessage.OPTResource{}); err != nil {
			t.Fatal(err)
		}
	}
	msg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

// testDNSAnswer echoes query as a response with rcode and flags OR'd into the
// third header byte.
func testDNSAnswer(query []byte, rcode dnsmessage.RCode, flags byte) []byte {
	resp := bytes.Clone(query)
	resp[2] |= 0x80 | flags
	resp[3] = resp[3]&0xf0 | byte(rcode)
	return resp
}

// dnsQueryMetadata is the GET atunnel sends for query, as the egress listener
// hands it to ext_proc.
func dnsQueryMetadata(xfcc string, query []byte) *extproc.RequestMetadata {
	return dnsPathMetadata(xfcc, protocol.DoHPath+"?"+protocol.DoHQueryParam+"="+base64.RawURLEncoding.EncodeToString(query))
}

func dnsPathMetadata(xfcc, path string) *extproc.RequestMetadata {
	md := egressMetadata(xfcc)
	md.Method = "GET"
	md.Headers[":method"] = "GET"
	md.Path = path
	md.Headers[":path"] = path
	return md
}

// testDNSResolver is a UDP and TCP resolver on one loopback port. respond
// returns the answer to a query read over network.
type testDNSResolver struct {
	address string
	asked   atomic.Int32
}

func newTestDNSResolver(t *testing.T, respond func(network string, query []byte) []byte) *testDNSResolver {
	t.Helper()
	r := &testDNSResolver{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pc, err := net.ListenPacket("udp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close(); _ = pc.Close() })
	r.address = lis.Addr().String()

	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			r.asked.Add(1)
			_, _ = pc.WriteTo(respond("udp", bytes.Clone(buf[:n])), from)
		}
	}()
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				query, err := protocol.ReadTCPFrame(conn)
				if err != nil {
					return
				}
				r.asked.Add(1)
				_ = protocol.WriteTCPFrame(conn, respond("tcp", query))
			}()
		}
	}()
	return r
}

// immediate returns the direct reply in res.
func immediate(t *testing.T, res extproc.Result) *extprocv3.ImmediateResponse {
	t.Helper()
	if res.Immediate == nil {
		t.Fatal("handler returned no immediate response")
	}
	return res.Immediate.GetImmediateResponse()
}

func responseHeader(resp *extprocv3.ImmediateResponse, key string) string {
	for _, h := range resp.GetHeaders().GetSetHeaders() {
		if h.GetHeader().GetKey() == key {
			return string(h.GetHeader().GetRawValue())
		}
	}
	return ""
}

func TestHandleDNSQuery(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	xfcc := xfccHeader(ca.issueActorCert(t, testActorSPIFFE, actorCertOptions{}))

	allowed := testDNSQuery(t, "api.example.com.", false)
	blocked := testDNSQuery(t, "secret.attacker.example.", false)
	blockedEDNS := testDNSQuery(t, "secret.attacker.example.", true)
	nxdomain := func(query []byte) []byte {
		var p dnsmessage.Parser
		hdr, _ := p.Start(query)
		q, _ := p.Question()
		return protocol.NXDomainPacket(hdr, q)
	}

	tests := []struct {
		name   string
		policy *ateapi.EgressPolicy
		query  []byte
		// upstreamDown points the handler at a closed port.
		upstreamDown bool
		wantDecision string
		wantBody     []byte
		wantAsked    int32
	}{
		{
			name:         "name in the policy is resolved",
			policy:       httpsPolicy("api.example.com"),
			query:        allowed,
			wantDecision: dnsDecisionAllowed,
			wantBody:     testDNSAnswer(allowed, dnsmessage.RCodeSuccess, 0),
			wantAsked:    1,
		},
		{
			name:         "name in a passthrough rule is resolved whatever its ports",
			policy:       passthroughPolicy(ports(5432), "*.example.com"),
			query:        allowed,
			wantDecision: dnsDecisionAllowed,
			wantBody:     testDNSAnswer(allowed, dnsmessage.RCodeSuccess, 0),
			wantAsked:    1,
		},
		{
			name:         "name outside the policy is NXDOMAIN",
			policy:       httpsPolicy("api.example.com"),
			query:        blocked,
			wantDecision: dnsDecisionBlocked,
			wantBody:     nxdomain(blocked),
		},
		{
			name:         "blocked EDNS query carries the Blocked extended error",
			policy:       httpsPolicy("api.example.com"),
			query:        blockedEDNS,
			wantDecision: dnsDecisionBlocked,
			wantBody: func() []byte {
				resp := append(nxdomain(blockedEDNS), blockedEDE...)
				resp[11] = 1
				return resp
			}(),
		},
		{
			name:         "actor without a policy resolves nothing",
			query:        allowed,
			wantDecision: dnsDecisionBlocked,
			wantBody:     nxdomain(allowed),
		},
		{
			name:         "unreachable resolver is SERVFAIL",
			policy:       httpsPolicy("api.example.com"),
			query:        allowed,
			upstreamDown: true,
			wantDecision: dnsDecisionError,
			wantBody: func() []byte {
				var p dnsmessage.Parser
				hdr, _ := p.Start(allowed)
				q, _ := p.Question()
				return protocol.ErrorPacket(hdr, dnsmessage.RCodeServerFailure, &q)
			}(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolver := newTestDNSResolver(t, func(_ string, query []byte) []byte {
				if protocol.TxnID(query) == 0 {
					t.Error("query reached the resolver with ID 0")
				}
				return testDNSAnswer(query, dnsmessage.RCodeSuccess, 0)
			})
			upstream := resolver.address
			if tc.upstreamDown {
				closed, err := net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				upstream = closed.LocalAddr().String()
				_ = closed.Close()
			}
			h := New(&egressMockClient{actor: runningActor(), policy: tc.policy}, ca.loadRoots, 0, nil,
				WithDNSUpstreams([]string{upstream}))

			res, err := h.HandleRequestHeaders(context.Background(), dnsQueryMetadata(xfcc, tc.query))
			if err != nil {
				t.Fatal(err)
			}
			resp := immediate(t, res)
			if got := resp.GetStatus().GetCode(); got != envoy_type.StatusCode_OK {
				t.Errorf("status = %v, want 200", got)
			}
			if got := responseHeader(resp, "content-type"); got != protocol.DoHMediaType {
				t.Errorf("content-type = %q, want %s", got, protocol.DoHMediaType)
			}
			if got := responseHeader(resp, dnsDecisionHeader); got != tc.wantDecision {
				t.Errorf("%s = %q, want %q", dnsDecisionHeader, got, tc.wantDecision)
			}
			if diff := cmp.Diff(tc.wantBody, resp.GetBody()); diff != "" {
				t.Errorf("DNS response mismatch (-want +got):\n%s", diff)
			}
			if got := resolver.asked.Load(); got != tc.wantAsked {
				t.Errorf("resolver was asked %d times, want %d", got, tc.wantAsked)
			}
		})
	}
}

// A DNS query gets the identity checks a CONNECT gets, and a request that is
// not a DNS query is refused over HTTP.
func TestHandleDNSQueryRefusals(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	otherCA := newTestCA(t, "another-ca")
	xfcc := xfccHeader(ca.issueActorCert(t, testActorSPIFFE, actorCertOptions{}))
	query := testDNSQuery(t, "api.example.com.", false)
	suspended := runningActor()
	suspended.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED

	tests := []struct {
		name  string
		actor *ateapi.Actor
		md    *extproc.RequestMetadata
		want  envoy_type.StatusCode
	}{
		{
			name:  "certificate from another CA",
			actor: runningActor(),
			md:    dnsQueryMetadata(xfccHeader(otherCA.issueActorCert(t, testActorSPIFFE, actorCertOptions{})), query),
			want:  envoy_type.StatusCode_Forbidden,
		},
		{
			name:  "actor is not placed on a worker",
			actor: suspended,
			md:    dnsQueryMetadata(xfcc, query),
			want:  envoy_type.StatusCode_Forbidden,
		},
		{
			name:  "dns parameter is missing",
			actor: runningActor(),
			md:    dnsPathMetadata(xfcc, protocol.DoHPath),
			want:  envoy_type.StatusCode_BadRequest,
		},
		{
			name:  "dns parameter is not base64url",
			actor: runningActor(),
			md:    dnsPathMetadata(xfcc, protocol.DoHPath+"?dns=!!!"),
			want:  envoy_type.StatusCode_BadRequest,
		},
		{
			name:  "message is a response",
			actor: runningActor(),
			md:    dnsQueryMetadata(xfcc, testDNSAnswer(query, dnsmessage.RCodeSuccess, 0)),
			want:  envoy_type.StatusCode_BadRequest,
		},
		{
			name:  "GET on another path",
			actor: runningActor(),
			md:    dnsPathMetadata(xfcc, "/other"),
			want:  envoy_type.StatusCode_MethodNotAllowed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := egressHandler(ca.loadRoots, tc.actor, nil)
			_, err := h.HandleRequestHeaders(context.Background(), tc.md)
			wantStatus(t, err, tc.want)
		})
	}
}

func TestDNSForwarder(t *testing.T) {
	query := testDNSQuery(t, "api.example.com.", false)
	servfail := func(_ string, q []byte) []byte { return testDNSAnswer(q, dnsmessage.RCodeServerFailure, 0) }
	answer := func(_ string, q []byte) []byte { return testDNSAnswer(q, dnsmessage.RCodeSuccess, 0) }
	// Truncated over UDP, whole over TCP.
	truncating := func(network string, q []byte) []byte {
		if network == "udp" {
			return testDNSAnswer(q, dnsmessage.RCodeSuccess, 0x02)
		}
		return testDNSAnswer(q, dnsmessage.RCodeSuccess, 0)
	}

	tests := []struct {
		name      string
		upstreams []func(string, []byte) []byte
		want      []byte
		wantAsked []int32
	}{
		{
			name:      "truncated UDP answer is fetched over TCP",
			upstreams: []func(string, []byte) []byte{truncating},
			want:      testDNSAnswer(query, dnsmessage.RCodeSuccess, 0),
			wantAsked: []int32{2},
		},
		{
			name:      "SERVFAIL moves on to the next resolver",
			upstreams: []func(string, []byte) []byte{servfail, answer},
			want:      testDNSAnswer(query, dnsmessage.RCodeSuccess, 0),
			wantAsked: []int32{1, 1},
		},
		{
			name:      "SERVFAIL from the last resolver is returned",
			upstreams: []func(string, []byte) []byte{servfail},
			want:      testDNSAnswer(query, dnsmessage.RCodeServerFailure, 0),
			wantAsked: []int32{1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &dnsForwarder{}
			var resolvers []*testDNSResolver
			for _, respond := range tc.upstreams {
				r := newTestDNSResolver(t, respond)
				resolvers = append(resolvers, r)
				f.upstreams = append(f.upstreams, r.address)
			}
			got, err := f.exchange(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("response mismatch (-want +got):\n%s", diff)
			}
			for i, r := range resolvers {
				if asked := r.asked.Load(); asked != tc.wantAsked[i] {
					t.Errorf("resolver %d was asked %d times, want %d", i, asked, tc.wantAsked[i])
				}
			}
		})
	}
}
