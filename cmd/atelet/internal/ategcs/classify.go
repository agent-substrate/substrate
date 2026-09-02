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

package ategcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"

	"google.golang.org/api/googleapi"

	"github.com/agent-substrate/substrate/internal/ateerrors"
)

// tagTransientErr tags a backend failure of a retryable class — a 5xx/429/408
// response, connection trouble, a stream cut mid-transfer — with
// ateerrors.ReasonTransientObjectStorage: a fact for the RPC handler's call
// site, which alone decides whether the request retries. Errors already
// carrying a Reason, context errors, and local file-system errors pass
// through untouched.
func tagTransientErr(err error) error {
	if err == nil {
		return nil
	}
	// An error classified at the source (e.g. ReasonFailedGetExternalObject
	// for a missing object) keeps its classification.
	if _, ok := errors.AsType[ateerrors.Reason](err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if !isTransientBackendErr(err) {
		return err
	}
	return fmt.Errorf("%w: %w", ateerrors.ReasonTransientObjectStorage, err)
}

// isTransientBackendErr reports whether err is a backend failure of a class
// worth retrying.
func isTransientBackendErr(err error) bool {
	if gerr, ok := errors.AsType[*googleapi.Error](err); ok {
		return retryableHTTPStatus(gerr.Code)
	}
	if re, ok := errors.AsType[interface {
		error
		HTTPStatusCode() int
	}](err); ok {
		return retryableHTTPStatus(re.HTTPStatusCode())
	}
	// Skip network errors.
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return true
	}
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE)
}

func retryableHTTPStatus(code int) bool {
	return code == http.StatusTooManyRequests ||
		code == http.StatusRequestTimeout ||
		code >= http.StatusInternalServerError
}

// transientReader applies tagTransientErr to failures surfacing mid-stream from
// a reader handed to the caller, where they bypass the function-boundary
// tagging in this package.
type transientReader struct{ io.ReadCloser }

func (r transientReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		err = tagTransientErr(err)
	}
	return n, err
}
