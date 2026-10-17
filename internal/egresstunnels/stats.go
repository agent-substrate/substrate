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

package egresstunnels

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Stats is what one actor's loop measured since it started. GET /stats and
// POST /stop return it as JSON.
type Stats struct {
	// Scheme is the scheme the loop requested endpoints over.
	Scheme  string        `json:"scheme,omitempty"`
	Elapsed time.Duration `json:"elapsed"`
	// Requests counts every request the loop finished, failed ones included.
	Requests  int64 `json:"requests"`
	Successes int64 `json:"successes"`
	// NewConns counts requests that went out on a connection opened for them
	// rather than on a kept-alive one. A tunnel the gateway reset shows up
	// here rather than in Errors: net/http redials, and retries a GET that
	// failed before any of the response arrived.
	NewConns int64 `json:"newConns"`
	// Opened counts endpoints whose first request has completed, so a reader
	// can tell the opening from the steady state.
	Opened int64 `json:"opened"`
	// OpeningDone is Elapsed at the moment the last endpoint's first request
	// completed; zero until every endpoint has opened. Merge keeps the latest.
	OpeningDone time.Duration `json:"openingDone"`
	// Rounds is the number of requests the least-visited endpoint has
	// completed; Merge sums it, so per-actor values say whether each actor is
	// still making progress.
	Rounds int64 `json:"rounds"`
	// LongestGap is the longest time between the starts of two consecutive
	// requests on one endpoint: how quiet the loop has left a tunnel. Each
	// endpoint is requested once per period, C times the interval, so with
	// prompt endpoints every gap is one period; a request that outlasts the
	// period delays only that endpoint's next request, so a longer gap means a
	// request on that tunnel took about that long. Keep the period under the
	// gateway's stream idle timeout.
	LongestGap time.Duration `json:"longestGap"`
	// Errors counts failed requests by Classify's class.
	Errors map[string]int64 `json:"errors,omitempty"`
	// Endpoints holds per-endpoint counts, indexed by endpoint number.
	Endpoints []Endpoint `json:"endpoints,omitempty"`
	// Opening holds the end-to-end time of each endpoint's first request, when
	// it succeeded: the cost of opening a tunnel as the client sees it.
	Opening Histogram `json:"opening"`
	// Latency holds the end-to-end time of successful requests after an
	// endpoint's first, so handshakes do not weigh on the steady state.
	Latency Histogram `json:"latency"`
	// DNS holds lookup times, recorded only for requests that did a lookup.
	DNS Histogram `json:"dns"`
	// TLS holds handshake times, recorded only for handshakes that completed.
	TLS Histogram `json:"tls"`
}

// Endpoint holds the counts for one endpoint.
type Endpoint struct {
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
	// LongestGap is Stats.LongestGap for this endpoint alone, so a quiet
	// tunnel can be named.
	LongestGap time.Duration `json:"longestGap"`
}

// Merge adds o's counts to s. Elapsed becomes the longer of the two, since
// merged loops run side by side rather than one after another. A histogram
// of o that Histogram.Merge refuses fails the merge and leaves s as it was.
func (s *Stats) Merge(o *Stats) error {
	opening, latency, dns, tls := s.Opening.clone(), s.Latency.clone(), s.DNS.clone(), s.TLS.clone()
	for _, h := range []struct {
		name string
		into *Histogram
		from Histogram
	}{{"opening", &opening, o.Opening}, {"latency", &latency, o.Latency}, {"dns", &dns, o.DNS}, {"tls", &tls, o.TLS}} {
		if err := h.into.Merge(h.from); err != nil {
			return fmt.Errorf("%s: %w", h.name, err)
		}
	}
	s.Opening, s.Latency, s.DNS, s.TLS = opening, latency, dns, tls
	s.Scheme = cmp.Or(s.Scheme, o.Scheme)
	s.Elapsed = max(s.Elapsed, o.Elapsed)
	s.Requests += o.Requests
	s.Successes += o.Successes
	s.NewConns += o.NewConns
	s.Opened += o.Opened
	s.OpeningDone = max(s.OpeningDone, o.OpeningDone)
	s.Rounds += o.Rounds
	s.LongestGap = max(s.LongestGap, o.LongestGap)
	for class, n := range o.Errors {
		if s.Errors == nil {
			s.Errors = make(map[string]int64)
		}
		s.Errors[class] += n
	}
	if len(o.Endpoints) > len(s.Endpoints) {
		s.Endpoints = append(s.Endpoints, make([]Endpoint, len(o.Endpoints)-len(s.Endpoints))...)
	}
	for i, e := range o.Endpoints {
		s.Endpoints[i].Requests += e.Requests
		s.Endpoints[i].Errors += e.Errors
		s.Endpoints[i].LongestGap = max(s.Endpoints[i].LongestGap, e.LongestGap)
	}
	return nil
}

// Clone returns a deep copy of s.
func (s *Stats) Clone() *Stats {
	c := *s
	c.Errors = maps.Clone(s.Errors)
	c.Endpoints = slices.Clone(s.Endpoints)
	c.Opening = s.Opening.clone()
	c.Latency = s.Latency.clone()
	c.DNS = s.DNS.clone()
	c.TLS = s.TLS.clone()
	return &c
}

// maxOtherLen bounds the message kept for an error Classify does not
// recognize, so one odd error cannot flood the report.
const maxOtherLen = 80

// Classify names the outcome of a request in a few low-cardinality classes.
// It returns "" for a success: no error and HTTP 200.
func Classify(err error, status int) string {
	if err == nil {
		if status == 200 {
			return ""
		}
		return fmt.Sprintf("HTTP %d", status)
	}
	var netErr net.Error
	var dnsErr *net.DNSError
	var opErr *net.OpError
	switch {
	case errors.As(err, &dnsErr):
		if dnsErr.IsNotFound {
			return "dns: not found"
		}
		if dnsErr.IsTimeout {
			return "dns: timeout"
		}
		return "dns: error"
	case errors.As(err, new(x509.UnknownAuthorityError)):
		return "tls: unknown authority"
	case errors.As(err, new(x509.HostnameError)):
		return "tls: hostname mismatch"
	case errors.As(err, new(x509.CertificateInvalidError)):
		return "tls: invalid certificate"
	case errors.As(err, new(tls.RecordHeaderError)),
		// net/http replaces the record error with this when the server
		// answered in plain HTTP, without wrapping it.
		strings.Contains(err.Error(), "server gave HTTP response to HTTPS client"):
		return "tls: not a TLS server"
	case errors.As(err, &opErr) && opErr.Op == "remote error":
		// crypto/tls reports an alert from the peer this way; its message,
		// such as "tls: handshake failure", names the alert.
		return "tls: peer alert: " + strings.TrimPrefix(opErr.Err.Error(), "tls: ")
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection reset"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "EOF"
	}
	// *url.Error repeats the method and URL, and *net.OpError the addresses
	// and the local port; both only add cardinality.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	if errors.As(err, &opErr) {
		return "net: " + opErr.Op + ": " + opErr.Err.Error()
	}
	msg := err.Error()
	if len(msg) > maxOtherLen {
		// Cut on a character boundary, or one message could yield two
		// classes depending on where its bytes fall.
		msg = strings.ToValidUTF8(msg[:maxOtherLen], "")
	}
	return "other: " + msg
}
