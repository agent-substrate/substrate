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

package extproc

import (
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

// AuthorityHeader is the HTTP/2 pseudo-header carrying the request authority.
// Exported because the direction handlers rewrite it when they pick an
// upstream.
const AuthorityHeader = ":authority"

// RequestMetadata is the request the mux hands to a direction handler: the
// HTTP headers Envoy sent, flattened and lowercased, with the pseudo-headers
// every handler needs pulled out.
type RequestMetadata struct {
	// Headers holds every header, keyed by lowercased name.
	Headers map[string]string
	Path    string
	Host    string
	Method  string
}

func NewRequestMetadata(headers []*corev3.HeaderValue) *RequestMetadata {
	headersMap := make(map[string]string)
	var path string
	var host string
	var method string

	for _, h := range headers {
		k := strings.ToLower(h.Key)
		val := h.Value
		if val == "" && len(h.RawValue) > 0 {
			val = string(h.RawValue)
		}

		headersMap[k] = val
		if k == ":path" {
			path = val
		}
		if k == AuthorityHeader || k == "host" {
			host = val
		}
		if k == ":method" {
			method = val
		}
	}

	return &RequestMetadata{
		Headers: headersMap,
		Path:    path,
		Host:    host,
		Method:  method,
	}
}

// Header returns the value of a header by name, case-insensitively, or "" when
// it was not sent.
func (m *RequestMetadata) Header(name string) string {
	return m.Headers[strings.ToLower(name)]
}
