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
	"net"
	"net/netip"
	"strconv"

	"go.ate.dev/substrate/internal/atunnel/fakeip"
)

// namedDestination returns the destination to tunnel to for a connection the
// actor made to address, an "ip:port". A fake address becomes the name it was
// given to, so the gateway receives the name the actor looked up.
//
// Any other address is returned as it is. That includes a fake address that
// belongs to no name, such as one whose name lost it. The gateway then has
// only the SNI or Host to go on, as without fake addresses.
func namedDestination(names *fakeip.Table, address string) string {
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return address
	}
	name, ok := names.Name(addrPort.Addr())
	if !ok {
		return address
	}
	return net.JoinHostPort(name, strconv.Itoa(int(addrPort.Port())))
}
