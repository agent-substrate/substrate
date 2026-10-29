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

package actor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	t.Parallel()
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
		{"net error without its addresses", &url.Error{Op: "Get", URL: "http://10.0.0.1:8080/", Err: &net.OpError{
			Op: "write", Net: "tcp",
			Source: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 51234},
			Addr:   &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 8080},
			Err:    syscall.EPIPE,
		}}, 0, "net: write: broken pipe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := classify(tc.err, tc.status); got != tc.want {
				t.Errorf("classify() = %q, want %q", got, tc.want)
			}
		})
	}
}
