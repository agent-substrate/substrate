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

// fakeAddressTTL is how many seconds a resolver may cache a fake address. The
// table keeps the address until the name is evicted, whatever the TTL. A short
// TTL makes a client that honors it pick up the new address soon after that.
const fakeAddressTTL = 30

// withFakeAddress returns what the actor gets for the upstream response raw,
// whose header and question are hdr and q, with p positioned after q.
//
// An answer to an A query gets the queried name's fake address in place of the
// real ones, so atunnel can tunnel a connection to it by name. Any other
// response is returned as it is.
//
// There is no policy check here: the relay checked the query before forwarding
// it, and the gateway checks the connection.
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
		// No fake address left: answer as without DNS correlation. The actor's
		// connections to the real addresses are tunneled by address.
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
