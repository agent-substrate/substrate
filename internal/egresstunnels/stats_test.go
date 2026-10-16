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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStatsMergeAndJSON(t *testing.T) {
	a := &Stats{
		Elapsed:     time.Minute,
		Requests:    10,
		Successes:   9,
		NewConns:    2,
		Opened:      2,
		OpeningDone: 3 * time.Second,
		Rounds:      1,
		LongestGap:  time.Second,
		Errors:      map[string]int64{"timeout": 1},
		Endpoints:   []Endpoint{{Requests: 5}, {Requests: 5, Errors: 1, LongestGap: 2 * time.Second}},
	}
	a.Latency.Record(time.Millisecond)
	a.Opening.Record(2 * time.Millisecond)
	b := &Stats{
		Scheme:      SchemeHTTPS,
		Elapsed:     2 * time.Minute,
		Requests:    6,
		Successes:   4,
		Opened:      3,
		OpeningDone: time.Second,
		Rounds:      4,
		LongestGap:  2 * time.Second,
		Errors:      map[string]int64{"timeout": 1, "HTTP 503": 1},
		Endpoints:   []Endpoint{{Requests: 2}, {Requests: 2, LongestGap: 3 * time.Second}, {Requests: 2, Errors: 2}},
	}
	b.DNS.Record(time.Millisecond)
	b.TLS.Record(3 * time.Millisecond)

	var got Stats
	if err := got.Merge(a); err != nil {
		t.Fatal(err)
	}
	if err := got.Merge(b); err != nil {
		t.Fatal(err)
	}
	if got.Elapsed != 2*time.Minute || got.Requests != 16 || got.Successes != 13 || got.NewConns != 2 || got.Opened != 5 ||
		got.OpeningDone != 3*time.Second || got.Rounds != 5 || got.LongestGap != 2*time.Second {
		t.Errorf("merged totals = %+v", got)
	}
	if got.Errors["timeout"] != 2 || got.Errors["HTTP 503"] != 1 {
		t.Errorf("merged errors = %v", got.Errors)
	}
	wantEndpoints := []Endpoint{{Requests: 7}, {Requests: 7, Errors: 1, LongestGap: 3 * time.Second}, {Requests: 2, Errors: 2}}
	if fmt.Sprint(got.Endpoints) != fmt.Sprint(wantEndpoints) {
		t.Errorf("merged endpoints = %v, want %v", got.Endpoints, wantEndpoints)
	}
	if got.Latency.Count != 1 || got.Opening.Count != 1 || got.DNS.Count != 1 || got.TLS.Count != 1 {
		t.Errorf("merged histograms: latency %d, opening %d, dns %d, tls %d samples; want 1 each", got.Latency.Count, got.Opening.Count, got.DNS.Count, got.TLS.Count)
	}
	if got.Scheme != SchemeHTTPS {
		t.Errorf("merged scheme = %q, want %q from the stats that set one", got.Scheme, SchemeHTTPS)
	}

	raw, err := json.Marshal(&got)
	if err != nil {
		t.Fatal(err)
	}
	var back Stats
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Latency.Quantile(0.5) != got.Latency.Quantile(0.5) || back.Opening.Count != 1 || back.TLS.Count != 1 || back.Scheme != SchemeHTTPS ||
		back.Requests != got.Requests || back.Rounds != 5 || back.OpeningDone != 3*time.Second || back.Errors["HTTP 503"] != 1 ||
		back.Endpoints[1].LongestGap != 3*time.Second {
		t.Errorf("JSON round trip lost data: %s", raw)
	}
}

func TestStatsMergeRefusesMismatchedHistograms(t *testing.T) {
	var got Stats
	got.Latency.Record(time.Millisecond)
	bad := &Stats{Requests: 5, Successes: 5}
	bad.TLS.Count = 5 // counts missing, as a report with omitted buckets has
	err := got.Merge(bad)
	if err == nil || !strings.Contains(err.Error(), "tls") {
		t.Fatalf("Merge() = %v, want an error naming the tls histogram", err)
	}
	if got.Requests != 0 || got.Latency.Count != 1 || got.TLS.Count != 0 {
		t.Errorf("a refused merge changed the receiver: %+v", got)
	}
}

func TestStatsCloneIsDeep(t *testing.T) {
	s := &Stats{Errors: map[string]int64{"EOF": 1}, Endpoints: []Endpoint{{Requests: 1, LongestGap: time.Second}}}
	s.Latency.Record(time.Millisecond)
	s.Opening.Record(time.Millisecond)
	s.TLS.Record(time.Millisecond)
	c := s.Clone()
	c.Errors["EOF"]++
	c.Endpoints[0].Requests++
	c.Endpoints[0].LongestGap++
	c.Latency.Record(time.Millisecond)
	c.Opening.Record(time.Millisecond)
	c.TLS.Record(time.Millisecond)
	if s.Errors["EOF"] != 1 || s.Endpoints[0].Requests != 1 || s.Endpoints[0].LongestGap != time.Second ||
		s.Latency.Count != 1 || s.Opening.Count != 1 || s.TLS.Count != 1 {
		t.Errorf("mutating the clone changed the original: %+v", s)
	}
	var sum int64
	for _, n := range s.Latency.Counts {
		sum += n
	}
	if sum != 1 {
		t.Errorf("original latency buckets hold %d samples after mutating the clone, want 1", sum)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	wrap := func(err error) error { return &url.Error{Op: "Get", URL: "http://x/", Err: err} }
	for _, tc := range []struct {
		name   string
		err    error
		status int
		want   string
	}{
		{"success", nil, 200, ""},
		{"http status", nil, 503, "HTTP 503"},
		{"deadline", wrap(context.DeadlineExceeded), 0, "timeout"},
		{"net timeout", wrap(&net.OpError{Op: "dial", Err: timeoutErr{}}), 0, "timeout"},
		{"refused", wrap(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), 0, "connection refused"},
		{"reset", wrap(&net.OpError{Op: "read", Err: syscall.ECONNRESET}), 0, "connection reset"},
		{"eof", wrap(io.EOF), 0, "EOF"},
		{"unexpected eof", wrap(io.ErrUnexpectedEOF), 0, "EOF"},
		{"dns not found", wrap(&net.DNSError{Err: "no such host", Name: "x", IsNotFound: true}), 0, "dns: not found"},
		{"dns other", wrap(&net.DNSError{Err: "server misbehaving", Name: "x"}), 0, "dns: error"},
		{"dns timeout", wrap(&net.DNSError{Err: "i/o timeout", Name: "x", IsTimeout: true}), 0, "dns: timeout"},
		{"long other is cut", wrap(errors.New(strings.Repeat("x", maxOtherLen+20))), 0, "other: " + strings.Repeat("x", maxOtherLen)},
		{"cut keeps whole characters", wrap(errors.New(strings.Repeat("a", maxOtherLen-1) + "é!")), 0, "other: " + strings.Repeat("a", maxOtherLen-1)},
		{"other strips url", wrap(errors.New("boom")), 0, "other: boom"},
		{"unknown authority", wrap(&tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}), 0, "tls: unknown authority"},
		{"hostname mismatch", wrap(&tls.CertificateVerificationError{Err: x509.HostnameError{Host: "x"}}), 0, "tls: hostname mismatch"},
		{"expired", wrap(&tls.CertificateVerificationError{Err: x509.CertificateInvalidError{Reason: x509.Expired}}), 0, "tls: invalid certificate"},
		{"not tls", wrap(tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}), 0, "tls: not a TLS server"},
		{"plain http answer", wrap(errors.New("http: server gave HTTP response to HTTPS client")), 0, "tls: not a TLS server"},
		{"peer alert", wrap(&net.OpError{Op: "remote error", Err: errors.New("tls: handshake failure")}), 0, "tls: peer alert: handshake failure"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.err, tc.status); got != tc.want {
				t.Errorf("Classify() = %q, want %q", got, tc.want)
			}
		})
	}

	long := Classify(errors.New(strings.Repeat("x", 500)), 0)
	if len(long) != len("other: ")+maxOtherLen {
		t.Errorf("unrecognized error not truncated: %d bytes", len(long))
	}
}

func TestClassifyDropsAddressesFromNetErrors(t *testing.T) {
	t.Parallel()
	err := &url.Error{Op: "Get", URL: "http://10.0.0.1:8080/", Err: &net.OpError{
		Op: "write", Net: "tcp",
		Source: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 51234},
		Addr:   &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 8080},
		Err:    syscall.EPIPE,
	}}
	got := Classify(err, 0)
	if want := "net: write: broken pipe"; got != want {
		t.Errorf("Classify(%v) = %q, want %q", err, got, want)
	}
}
