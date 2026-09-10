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

// TestActorNftablesRuleExprs pins the wire encoding of the expressions
// installed into the inet actor table, notably the nfproto guard in front of
// every IPv4 match.
func TestActorNftablesRuleExprs(t *testing.T) {
	// meta nfproto ipv4; ip saddr 169.254.17.2
	actorSourceIsIPv4 := []expr.Any{
		&expr.Meta{Key: expr.MetaKeyNFPROTO, Register: 1},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{2}},
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 12, Len: 4},
		&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{169, 254, 17, 2}},
	}

	tests := []struct {
		name string
		got  []expr.Any
		want []expr.Any
	}{{
		name: "source match guards the payload load with nfproto",
		got:  ipv4SourceEqual(ActorVethIP),
		want: actorSourceIsIPv4,
	}, {
		name: "egress redirect matches actor IPv4 TCP and redirects to the port",
		got:  ActorEgressRedirectRule(nil, nil, 15001).Exprs,
		want: append(append([]expr.Any{}, actorSourceIsIPv4...),
			// meta l4proto tcp; redirect to :15001
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{6}},
			&expr.Immediate{Register: 1, Data: []byte{0x3a, 0x99}},
			&expr.Redir{RegisterProtoMin: 1},
		),
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
}

func formatExprs(exprs []expr.Any) string {
	var s string
	for _, e := range exprs {
		s += fmt.Sprintf("  %T%+v\n", e, e)
	}
	return s
}
