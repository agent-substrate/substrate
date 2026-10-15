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

package objectstorage

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
)

// TestDescribeGCSError checks what a GCS API error is described as: an XML API
// error's status, Code and Message but never its Details, and a JSON API
// error's status and reason but never its message, which names the caller and
// resource.
func TestDescribeGCSError(t *testing.T) {
	const secret = "caller@project.iam.gserviceaccount.com does not have storage.objects.get access to gvisor/releases/runsc"
	for _, tc := range []struct {
		name string
		err  error
		want ServiceError
	}{
		{
			// What GCS returned to the plugin's Workload Identity token for
			// gs://gvisor (#2613).
			name: "XML API error",
			err: &googleapi.Error{
				Code: 412,
				Body: "<?xml version='1.0' encoding='UTF-8'?><Error><Code>PreconditionFailed</Code><Message>The operation requires that Uniform Bucket Level Access be enabled.</Message><Details>" + secret + "</Details></Error>",
			},
			want: ServiceError{HTTPStatus: 412, Code: "PreconditionFailed", Message: "The operation requires that Uniform Bucket Level Access be enabled."},
		},
		{
			name: "XML API error, wrapped",
			err: fmt.Errorf("while opening gs://gvisor/releases/runsc: %w", &googleapi.Error{
				Code: 403,
				Body: "<Error><Code>AccessDenied</Code><Message>Access denied.</Message><Details>" + secret + "</Details></Error>",
			}),
			want: ServiceError{HTTPStatus: 403, Code: "AccessDenied", Message: "Access denied."},
		},
		{
			name: "JSON API error",
			err: &googleapi.Error{
				Code:    403,
				Message: secret,
				Errors:  []googleapi.ErrorItem{{Reason: "forbidden", Message: secret}},
				Body:    `{"error":{"code":403,"message":"` + secret + `"}}`,
			},
			want: ServiceError{HTTPStatus: 403, Code: "forbidden"},
		},
		{
			name: "body that is not an XML error",
			err:  &googleapi.Error{Code: 502, Body: "<html><body>Bad Gateway " + secret + "</body></html>"},
			want: ServiceError{HTTPStatus: 502},
		},
		{
			name: "empty body",
			err:  &googleapi.Error{Code: 500},
			want: ServiceError{HTTPStatus: 500},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := DescribeServiceError(tc.err)
			if !ok {
				t.Fatalf("DescribeServiceError(%v) found no service error", tc.err)
			}
			if got != tc.want {
				t.Errorf("DescribeServiceError = %+v, want %+v", got, tc.want)
			}
			if strings.Contains(got.Code+got.Message, "caller@") {
				t.Errorf("description %+v names the caller or resource", got)
			}
		})
	}

	t.Run("not a GCS error", func(t *testing.T) {
		if got, ok := describeGCSError(errors.New("boom")); ok {
			t.Errorf("describeGCSError(plain error) = %+v, true; want false", got)
		}
	})
}
