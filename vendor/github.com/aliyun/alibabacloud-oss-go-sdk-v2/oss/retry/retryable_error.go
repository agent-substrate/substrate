package retry

import (
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
)

type HTTPStatusCodeRetryable struct {
}

var retryErrorCodes = []int{
	401, // Unauthorized
	408, // Request Timeout
	429, // Rate exceeded.
}

func (*HTTPStatusCodeRetryable) IsErrorRetryable(err error) bool {
	var v interface{ HttpStatusCode() int }
	if errors.As(err, &v) {
		code := v.HttpStatusCode()
		if code >= 500 {
			return true
		}
		for _, e := range retryErrorCodes {
			if code == e {
				return true
			}
		}
	}
	return false
}

type ServiceErrorCodeRetryable struct {
}

var retryServiceErrorCodes = map[string]struct{}{
	"RequestTimeTooSkewed": {},
	"BadRequest":           {},
}

func (*ServiceErrorCodeRetryable) IsErrorRetryable(err error) bool {
	var v interface{ ErrorCode() string }
	if errors.As(err, &v) {
		if _, ok := retryServiceErrorCodes[v.ErrorCode()]; ok {
			return true
		}
	}
	return false
}

type ConnectionErrorRetryable struct{}

var retriableErrorStrings = []string{
	"connection reset",
	"connection refused",
	"use of closed network connection",
	"unexpected EOF reading trailer",
	"transport connection broken",
	"server closed idle connection",
	"bad record MAC",
	"stream error:",
	"tls: use of closed connection",
	"connection was forcibly closed",
	"broken pipe",
	"crc is inconsistent", // oss crc check error pattern
}

var retriableErrors = []error{
	io.EOF,
	io.ErrUnexpectedEOF,
}

func (c *ConnectionErrorRetryable) IsErrorRetryable(err error) bool {
	if err != nil {
		switch t := err.(type) {
		case *url.Error:
			if t.Err != nil {
				return c.IsErrorRetryable(t.Err)
			}
		case net.Error:
			if t.Temporary() || t.Timeout() {
				return true
			}
		}

		for _, retriableErr := range retriableErrors {
			if err == retriableErr {
				return true
			}
		}

		errString := err.Error()
		for _, phrase := range retriableErrorStrings {
			if strings.Contains(errString, phrase) {
				return true
			}
		}
	}
	return false
}
