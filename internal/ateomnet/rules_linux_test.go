//go:build linux

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

package ateomnet

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/nftables/expr"
)

// TestActorNftablesRuleExprs pins the expressions installed into the inet actor
// table. The nfproto guard in front of every IPv4 match is what makes those
// matches safe there -- without it the payload load reads an IPv4 offset out of
// an IPv6 header -- and no other test in the package can see it: an IPv4-only
// datapath still behaves correctly with the guard removed.
//
// The wants are spelled out as literal bytes rather than built from the same
// helpers as the code, so they pin the wire encoding and not just its spelling.
func TestActorNftablesRuleExprs(t *testing.T) {
	// meta nfproto ipv4; ip saddr 169.254.17.2
	actorSourceIsIPv4 := []expr.Any{
		&expr.Meta{Key: expr.MetaKeyNFPROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{2}},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 12, Len: 4},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{169, 254, 17, 2}},
	}

	// meta nfproto ipv6; ip6 saddr fd00:169:254::2
	actorSourceIsIPv6 := []expr.Any{
		&expr.Meta{Key: expr.MetaKeyNFPROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{10}},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 8, Len: 16},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{
			0xfd, 0x00, 0x01, 0x69, 0x02, 0x54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x02,
		}},
	}

	// meta l4proto tcp; redirect to :15001
	tcpRedirectTo15001 := []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{6}},
		&expr.Immediate{Register: 1, Data: []byte{0x3a, 0x99}},
		&expr.Redir{RegisterProtoMin: 1},
	}

	tests := []struct {
		name string
		got  []expr.Any
		want []expr.Any
	}{{
		name: "IPv4 source match guards the payload load with nfproto",
		got:  ipv4SourceEqual(ActorVethIP),
		want: actorSourceIsIPv4,
	}, {
		name: "IPv6 source match guards the payload load with nfproto",
		got:  ipv6SourceEqual(ActorVethIPv6IP),
		want: actorSourceIsIPv6,
	}, {
		name: "egress redirect matches actor IPv4 TCP and redirects to the port",
		got:  ActorEgressRedirectRule(nil, nil, 15001).Exprs,
		want: append(append([]expr.Any{}, actorSourceIsIPv4...), tcpRedirectTo15001...),
	}, {
		name: "egress redirect matches actor IPv6 TCP and redirects to the port",
		got:  ActorIPv6EgressRedirectRule(nil, nil, 15001).Exprs,
		want: append(append([]expr.Any{}, actorSourceIsIPv6...), tcpRedirectTo15001...),
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !reflect.DeepEqual(test.got, test.want) {
				t.Errorf("rule exprs mismatch:\ngot:\n%s\nwant:\n%s", formatExprs(test.got), formatExprs(test.want))
			}
		})
	}
}

// TestActorEgressRedirectRuleDisabled covers the zero port: no rule at all, so
// actor egress stays on the masquerade path instead of being redirected to a
// listener that is not there.
func TestActorEgressRedirectRuleDisabled(t *testing.T) {
	if rule := ActorEgressRedirectRule(nil, nil, 0); rule != nil {
		t.Errorf("ActorEgressRedirectRule(0) = %v, want nil", rule.Exprs)
	}
	if rule := ActorIPv6EgressRedirectRule(nil, nil, 0); rule != nil {
		t.Errorf("ActorIPv6EgressRedirectRule(0) = %v, want nil", rule.Exprs)
	}
}

func formatExprs(exprs []expr.Any) string {
	var s string
	for _, e := range exprs {
		s += fmt.Sprintf("  %T%+v\n", e, e)
	}
	return s
}
