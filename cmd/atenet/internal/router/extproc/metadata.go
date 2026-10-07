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
	"strconv"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

// AuthorityHeader is the HTTP/2 pseudo-header carrying the request authority.
// Exported because the direction handlers rewrite it when they pick an
// upstream.
const AuthorityHeader = ":authority"

// RequestMetadata is the request the mux hands to a direction handler: the
// HTTP headers the dataplane sent, flattened and lowercased, with the
// pseudo-headers every handler needs pulled out.
type RequestMetadata struct {
	// Headers holds every header, keyed by lowercased name.
	Headers map[string][]string
	Path    string
	Host    string
	Method  string

	// Attributes is the CEL request_attributes Envoy attached to this
	// callback, keyed by the ext_proc filter name that requested them (see
	// Attribute). Ingress reads its resolved actor authority from here, since
	// filter state -- unlike dynamic metadata -- survives the
	// connect_terminate -> main_internal internal-listener hop that CONNECT
	// requests take.
	Attributes map[string]*structpb.Struct
}

func NewRequestMetadata(headers []*corev3.HeaderValue, attributes map[string]*structpb.Struct) *RequestMetadata {
	headersMap := make(map[string][]string)
	var path string
	var host string
	var method string

	for _, h := range headers {
		k := strings.ToLower(h.Key)
		val := h.Value
		if val == "" && len(h.RawValue) > 0 {
			val = string(h.RawValue)
		}

		headersMap[k] = append(headersMap[k], val)
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
		Headers:    headersMap,
		Path:       path,
		Host:       host,
		Method:     method,
		Attributes: attributes,
	}
}

// Header returns the value of a header by name, case-insensitively, or "" when
// it was not sent. When multiple values were sent, the last one is returned.
func (m *RequestMetadata) Header(name string) string {
	if m == nil {
		return ""
	}
	vals := m.Headers[strings.ToLower(name)]
	if len(vals) == 0 {
		return ""
	}
	return vals[len(vals)-1]
}

// HeaderValues returns all values sent for a header by name, case-insensitively,
// in wire order, or nil when it was not sent.
func (m *RequestMetadata) HeaderValues(name string) []string {
	if m == nil {
		return nil
	}
	return m.Headers[strings.ToLower(name)]
}

// Get returns the value associated with the passed key, implementing propagation.TextMapCarrier.
func (m *RequestMetadata) Get(key string) string {
	return m.Header(key)
}

// Set stores the key-value pair, implementing propagation.TextMapCarrier.
func (m *RequestMetadata) Set(key, val string) {
	if m == nil {
		return
	}
	if m.Headers == nil {
		m.Headers = make(map[string][]string)
	}
	m.Headers[strings.ToLower(key)] = []string{val}
}

// Keys lists the keys stored in this carrier, implementing propagation.TextMapCarrier.
func (m *RequestMetadata) Keys() []string {
	if m == nil || m.Headers == nil {
		return nil
	}
	keys := make([]string, 0, len(m.Headers))
	for k := range m.Headers {
		keys = append(keys, k)
	}
	return keys
}

// Attribute returns the named CEL request_attributes value, scanning every
// filter's entries so callers don't need to hardcode which one reported it,
// or "" if none did. A number (Envoy sends int fields as one) renders as its
// decimal digits.
func (m *RequestMetadata) Attribute(name string) string {
	for _, attrs := range m.Attributes {
		if v, ok := attrs.GetFields()[name]; ok {
			if n, isNumber := v.GetKind().(*structpb.Value_NumberValue); isNumber {
				return strconv.FormatFloat(n.NumberValue, 'f', -1, 64)
			}
			return v.GetStringValue()
		}
	}
	return ""
}
