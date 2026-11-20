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

package dns

import (
	"errors"
	"net/netip"

	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
	"go.ate.dev/substrate/internal/egresspolicy"
)

// fakeAddressTTL is how many seconds a resolver may cache a fake address. A
// name can lose its address when the actor's table fills up, so the TTL is
// short: a client that honors it soon asks again and gets the new address.
const fakeAddressTTL = 30

// withFakeAddress returns what the actor gets for the upstream response raw,
// whose header and question are hdr and q, with p positioned after q.
//
// The addresses in an answer to an A query are replaced with the fake address
// of the queried name, so a connection to it can later be tunneled to that
// name. Any other response is returned as it is.
//
// The egress policy plays no part here: which names the actor may reach is
// decided where the query is admitted, and by the gateway at connect time.
func (h *dnsHandler) withFakeAddress(raw []byte, hdr dnsmessage.Header, q dnsmessage.Question, p *dnsmessage.Parser) []byte {
	if h.fakeIPs == nil || hdr.RCode != dnsmessage.RCodeSuccess || q.Class != dnsmessage.ClassINET || q.Type != dnsmessage.TypeA {
		return raw
	}
	tunneled, err := resolvesToTunneledAddress(p)
	name, isHostname := egresspolicy.NormalizeHostname(q.Name.String())
	if err != nil || !tunneled || !isHostname {
		return raw
	}
	addr, ok := h.fakeIPs.Address(name)
	if !ok {
		// The actor gets the real addresses, and its connections to them are
		// tunneled by address, as they are without a table.
		return raw
	}
	return protocol.AddressPacket(hdr, q, addr.As4(), fakeAddressTTL)
}

// resolvesToTunneledAddress reports whether the answers p is about to read
// hold A records, all with addresses the actor's connections are tunneled
// for. A connection to a loopback or link-local address never leaves the
// sandbox, so a name that resolves to one has to keep resolving to it.
func resolvesToTunneledAddress(p *dnsmessage.Parser) (bool, error) {
	if err := p.SkipAllQuestions(); err != nil {
		return false, err
	}
	found := false
	for {
		answer, err := p.AnswerHeader()
		if errors.Is(err, dnsmessage.ErrSectionDone) {
			return found, nil
		}
		if err != nil {
			return false, err
		}
		if answer.Type != dnsmessage.TypeA {
			if err := p.SkipAnswer(); err != nil {
				return false, err
			}
			continue
		}
		record, err := p.AResource()
		if err != nil {
			return false, err
		}
		if addr := netip.AddrFrom4(record.A); addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsUnspecified() {
			return false, nil
		}
		found = true
	}
}
