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

package router

import (
	"context"
	"errors"
	"net"
	"net/http"
)

// Loopback addresses to try when re-contacting Envoy.
var loopbackIPs = []string{"127.0.0.1", "::1"}

// newLoopbackTransport returns a clone of http.DefaultTransport whose dials to
// "localhost" go through dialLoopback.
func newLoopbackTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = dialLoopback(&net.Dialer{})
	return t
}

// dialLoopback provides special handling to try both IPv6 and IPv4 when
// connecting to "localhost".
func dialLoopback(d *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil || host != "localhost" {
			return d.DialContext(ctx, network, addr)
		}
		var errs []error
		for _, ip := range loopbackIPs {
			conn, err := d.DialContext(ctx, network, net.JoinHostPort(ip, port))
			if err == nil {
				return conn, nil
			}
			errs = append(errs, err)
		}
		return nil, errors.Join(errs...)
	}
}
