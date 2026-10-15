// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectstoreplugin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/objectstorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// responseError is a storage service failure that reports its HTTP status,
// as a backend SDK's response error does.
type responseError struct {
	code int
	text string
}

func (e *responseError) Error() string       { return e.text }
func (e *responseError) HTTPStatusCode() int { return e.code }

// TestToStatusLogsUnrecognizedErrors checks that an error toStatus does not
// recognize, which callers see only as Internal, is logged at WARN with its
// HTTP status and Go type but not its text, and that recognized errors are not
// logged.
func TestToStatusLogsUnrecognizedErrors(t *testing.T) {
	const secret = "SECRET-SIG"
	for _, tc := range []struct {
		name     string
		err      error
		code     codes.Code
		wantLogs []string // empty: nothing is logged
	}{
		{"storage response error", fmt.Errorf("while reading gs://b/o?sig=%s: %w", secret, &responseError{412, "precondition failed for gs://b/o " + secret}), codes.Internal,
			[]string{`"level":"WARN"`, `"http_status":412`, `"error_type":"*objectstoreplugin.responseError"`}},
		{"other error", fmt.Errorf("while reading gs://b/o?sig=%s: %w", secret, errors.New("boom")), codes.Internal,
			[]string{`"level":"WARN"`, `"error_type":"*errors.errorString"`}},
		{"not found", fmt.Errorf("while reading: %w", objectstorage.ErrObjectNotFound), codes.NotFound, nil},
		{"canceled", context.Canceled, codes.Canceled, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			orig := slog.Default()
			t.Cleanup(func() { slog.SetDefault(orig) })
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))

			if got := status.Code(toStatus(tc.err)); got != tc.code {
				t.Errorf("toStatus(%v) code = %s, want %s", tc.err, got, tc.code)
			}
			logs := buf.String()
			if len(tc.wantLogs) == 0 && logs != "" {
				t.Errorf("logged a recognized error:\n%s", logs)
			}
			for _, want := range tc.wantLogs {
				if !strings.Contains(logs, want) {
					t.Errorf("logs missing %s:\n%s", want, logs)
				}
			}
			for _, leak := range []string{secret, "gs://b/o", "boom", "precondition failed"} {
				if strings.Contains(logs, leak) {
					t.Errorf("logs carry %q:\n%s", leak, logs)
				}
			}
		})
	}
}
