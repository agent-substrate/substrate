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

package ingress

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

func TestHandleRequestHeadersTargetPort(t *testing.T) {
	for _, tc := range []struct {
		name      string
		values    []string
		method    string
		authority string
		retained  string
		wantPort  string
		wantBad   bool
	}{
		{name: "default", wantPort: "80"},
		{name: "explicit", values: []string{"9090"}, wantPort: "9090"},
		{name: "minimum", values: []string{"1"}, wantPort: "1"},
		{name: "maximum", values: []string{"65535"}, wantPort: "65535"},
		{name: "leading zero", values: []string{"09090"}, wantPort: "9090"},
		{name: "empty", values: []string{""}, wantPort: "80"},
		{name: "zero", values: []string{"0"}, wantBad: true},
		{name: "negative", values: []string{"-1"}, wantBad: true},
		{name: "plus sign", values: []string{"+80"}, wantBad: true},
		{name: "out of range", values: []string{"65536"}, wantBad: true},
		{name: "overflow", values: []string{"18446744073709551616"}, wantBad: true},
		{name: "non decimal", values: []string{"0x50"}, wantBad: true},
		{name: "service name", values: []string{"http"}, wantBad: true},
		{name: "whitespace", values: []string{" 80"}, wantBad: true},
		{name: "comma", values: []string{"80,9090"}, wantBad: true},
		{name: "duplicate", values: []string{"9090", "9090"}, wantBad: true},
		{name: "different duplicates", values: []string{"80", "9090"}, wantBad: true},
		{name: "empty duplicate", values: []string{"", "9090"}, wantBad: true},
		{name: "CONNECT", method: "CONNECT", authority: "actor:9090", wantPort: "9090"},
		{name: "CONNECT matching", method: "CONNECT", authority: "actor:9090", values: []string{"09090"}, wantPort: "9090"},
		{name: "CONNECT conflict ignored", method: "CONNECT", authority: "actor:9090", values: []string{"80"}, wantPort: "9090"},
		{name: "CONNECT invalid header ignored", method: "CONNECT", authority: "actor:9090", values: []string{"invalid"}, wantPort: "9090"},
		{name: "CONNECT empty header ignored", method: "CONNECT", authority: "actor:9090", values: []string{""}, wantPort: "9090"},
		{name: "CONNECT duplicate header ignored", method: "CONNECT", authority: "actor:9090", values: []string{"80", "9090"}, wantPort: "9090"},
		{name: "tunneled", retained: "actor:9090", wantPort: "9090"},
		{name: "tunneled matching", retained: "actor:9090", values: []string{"9090"}, wantPort: "9090"},
		{name: "tunneled conflict ignored", retained: "actor:9090", values: []string{"80"}, wantPort: "9090"},
		{name: "tunneled invalid header ignored", retained: "actor:9090", values: []string{"65536"}, wantPort: "9090"},
		{name: "tunneled empty header ignored", retained: "actor:9090", values: []string{""}, wantPort: "9090"},
		{name: "tunneled duplicate header ignored", retained: "actor:9090", values: []string{"80", "9090"}, wantPort: "9090"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := &mockClient{resumeFn: func(context.Context, *ateapipb.ResumeActorRequest, ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				calls++
				return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{Status: &ateapipb.ActorStatus{
					WorkerAssignment: &ateapipb.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}},
				}}}, nil
			}}
			headers := []*corev3.HeaderValue{
				{Key: ":method", Value: tc.method},
				{Key: ":authority", Value: "app.example:8443"},
				{Key: ":path", Value: "/api%2Fitem?q=1"},
			}
			for _, value := range tc.values {
				headers = append(headers, &corev3.HeaderValue{Key: "Ate-Target-Port", Value: value})
			}
			md := requestMetadata("actor", "team-a", headers...)
			if tc.authority != "" {
				md.Host = tc.authority
			}
			if tc.retained != "" {
				md.Attributes = map[string]*structpb.Struct{"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
					extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue(tc.retained),
					extproc.TargetActorFilterStateAttribute:      structpb.NewStringValue("team-a/actor"),
				}}}
			}

			res, err := New(client, ParkedRequestConfig{}, nil).HandleRequestHeaders(t.Context(), md)
			if tc.wantBad {
				var reqErr *extproc.ReqError
				if !errors.As(err, &reqErr) || reqErr.StatusCode != http.StatusBadRequest {
					t.Fatalf("error = %v, want 400", err)
				}
				if calls != 0 {
					t.Errorf("ResumeActor called %d times for invalid port", calls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Errorf("ResumeActor called %d times, want 1", calls)
			}
			if got := dynamicMetadataPort(res.DynamicMetadata); got != tc.wantPort {
				t.Errorf("port = %q, want %q", got, tc.wantPort)
			}
			mutation := res.Response.GetResponse().GetHeaderMutation()
			if !slices.Contains(mutation.GetRemoveHeaders(), "ate-target-port") {
				t.Error("target-port header not removed")
			}
			for _, header := range mutation.GetSetHeaders() {
				if key := header.GetHeader().GetKey(); key == ":authority" || key == ":path" || key == ":method" {
					t.Errorf("application header %s rewritten", key)
				}
			}
		})
	}
}
