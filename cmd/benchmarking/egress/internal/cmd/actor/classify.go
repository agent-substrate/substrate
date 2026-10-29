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
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"syscall"
)

// maxOtherLen bounds the message kept for an error classify does not
// recognize, so one odd error cannot flood the report.
const maxOtherLen = 80

// classify names the outcome of a request in a few low-cardinality classes.
// It returns "" for a success: no error and HTTP 200.
func classify(err error, status int) string {
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
