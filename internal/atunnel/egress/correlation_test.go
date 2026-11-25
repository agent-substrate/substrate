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
	"context"
	"log/slog"
	"net"
	"net/netip"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/atunnel/fakeip"
)

// fakeAddress returns the fake address table gives name.
func fakeAddress(t *testing.T, table *fakeip.Table, name string) netip.Addr {
	t.Helper()
	addr, ok := table.Address(name)
	if !ok {
		t.Fatalf("no fake address left for %q", name)
	}
	return addr
}

// unusedFakeAddress returns a fake address that table gave to no name.
func unusedFakeAddress(t *testing.T, table *fakeip.Table, used netip.Addr) netip.Addr {
	t.Helper()
	unused := used.Next()
	if !fakeip.Contains(unused) {
		unused = used.Prev()
	}
	if name, ok := table.Name(unused); ok {
		t.Fatalf("%s belongs to %q, want a fake address with no name", unused, name)
	}
	return unused
}

func TestNamedDestination(t *testing.T) {
	names := fakeip.New(slog.Default())
	fake := fakeAddress(t, names, "db.example.com")
	unused := net.JoinHostPort(unusedFakeAddress(t, names, fake).String(), "5432")

	tests := []struct {
		name    string
		address string
		want    string
	}{
		{name: "fake address", address: net.JoinHostPort(fake.String(), "5432"), want: "db.example.com:5432"},
		{name: "fake address that belongs to no name", address: unused, want: unused},
		{name: "real address", address: "93.184.216.34:443", want: "93.184.216.34:443"},
		{name: "IPv6 address", address: "[2001:db8::1]:443", want: "[2001:db8::1]:443"},
		{name: "not an address", address: "db.example.com:5432", want: "db.example.com:5432"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := namedDestination(names, tc.address); got != tc.want {
				t.Errorf("namedDestination(%s) = %q, want %q", tc.address, got, tc.want)
			}
		})
	}
}

func TestEgressTunnelsFakeAddressToItsName(t *testing.T) {
	names := fakeip.New(slog.Default())
	fakeAddr := fakeAddress(t, names, "db.example.com")
	fake := net.JoinHostPort(fakeAddr.String(), "5432")
	unused := net.JoinHostPort(unusedFakeAddress(t, names, fakeAddr).String(), "5432")

	tests := []struct {
		name string
		// names is the table the actor's listener is bound with.
		names *fakeip.Table
		// dialed is the address the actor connected to.
		dialed string
		// wantTunnels are the destinations the proxy tries, in order.
		wantTunnels []string
	}{
		{name: "fake address", names: names, dialed: fake, wantTunnels: []string{"db.example.com:5432"}},
		{name: "fake address that belongs to no name", names: names, dialed: unused, wantTunnels: []string{unused}},
		{name: "real address", names: names, dialed: "93.184.216.34:443", wantTunnels: []string{"93.184.216.34:443"}},
		{name: "address in the fake range without a table", dialed: fake, wantTunnels: []string{fake}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			egress, err := New(func(net.Conn) (string, error) { return tc.dialed, nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = egress.Deactivate(context.Background(), testActorUID) })
			if _, err := egress.Bind(testActorUID, tc.names); err != nil {
				t.Fatal(err)
			}
			tunnels := make(chan string, 2)
			upstream, gateway := net.Pipe()
			defer gateway.Close()
			dialer := tunnelDialerFunc(func(_ context.Context, destination string) (net.Conn, error) {
				tunnels <- destination
				if _, _, err := net.SplitHostPort(destination); err != nil {
					return nil, err
				}
				return upstream, nil
			})
			if err := egress.Activate(testActorUID, dialer, fakeActorCertificateSource{}, time.Now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}

			actor, proxy := net.Pipe()
			defer actor.Close()
			egress.handle(proxy, egress.active[testActorUID])

			for _, want := range tc.wantTunnels {
				if got := receiveWithin(t, tunnels, "tunnel to open"); got != want {
					t.Fatalf("tunnel opened to %q, want %q", got, want)
				}
			}
			// The actor's connection closes when the proxy is done with it, so
			// one more attempt would be in the channel by then.
			_ = gateway.Close()
			if err := actor.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			_, _ = actor.Read(make([]byte, 1))
			select {
			case got := <-tunnels:
				t.Fatalf("one more tunnel opened to %q, want none", got)
			default:
			}
		})
	}
}
