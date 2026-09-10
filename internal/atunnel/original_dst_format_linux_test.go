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

package atunnel

import (
	"encoding/binary"
	"net"
	"testing"
)

// networkOrderPort produces the raw field value the kernel leaves in
// RawSockaddrInet4.Port and RawSockaddrInet6.Port: a uint16 whose in-memory
// bytes are the port in network order, which on a little-endian host is not
// the port's numeric value.
func networkOrderPort(port uint16) uint16 {
	return binary.NativeEndian.Uint16(binary.BigEndian.AppendUint16(nil, port))
}

func TestFormatOriginalDestination(t *testing.T) {
	tests := []struct {
		name    string
		ip      []byte
		port    uint16
		want    string
		wantErr bool
	}{
		{name: "IPv4", ip: []byte{198, 18, 0, 1}, port: 443, want: "198.18.0.1:443"},
		{
			name: "IPv6 is bracketed",
			ip:   net.ParseIP("fd00:198:18::1").To16(),
			port: 443,
			want: "[fd00:198:18::1]:443",
		},
		{
			name: "v4-mapped IPv6 renders as IPv4",
			ip:   net.ParseIP("::ffff:198.18.0.1").To16(),
			port: 8080,
			want: "198.18.0.1:8080",
		},
		{name: "high port is not sign-extended", ip: []byte{198, 18, 0, 1}, port: 65535, want: "198.18.0.1:65535"},
		{name: "port zero is rejected", ip: []byte{198, 18, 0, 1}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := formatOriginalDestination(test.ip, networkOrderPort(test.port))
			if test.wantErr {
				if err == nil {
					t.Fatalf("formatOriginalDestination() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("formatOriginalDestination() error = %v", err)
			}
			if got != test.want {
				t.Errorf("formatOriginalDestination() = %q, want %q", got, test.want)
			}
		})
	}
}
