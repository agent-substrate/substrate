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

package extproc_test

import (
	"context"
	"io"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/egress"
	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
)

const websocketDenialBody = "WebSocket egress is not supported"

type websocketProcessStream struct {
	extprocv3.ExternalProcessor_ProcessServer
	ctx       context.Context
	requests  []*extprocv3.ProcessingRequest
	responses []*extprocv3.ProcessingResponse
}

func (s *websocketProcessStream) Context() context.Context { return s.ctx }

func (s *websocketProcessStream) Recv() (*extprocv3.ProcessingRequest, error) {
	if len(s.requests) == 0 {
		return nil, io.EOF
	}
	req := s.requests[0]
	s.requests = s.requests[1:]
	return req, nil
}

func (s *websocketProcessStream) Send(resp *extprocv3.ProcessingResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func TestProcessReturnsWebSocketDenialAsImmediateResponse(t *testing.T) {
	for _, leg := range []string{extproc.EgressCleartextFilterChainName, extproc.EgressTLSMITMFilterChainName} {
		t.Run(leg, func(t *testing.T) {
			request := &extprocv3.ProcessingRequest{
				Request: &extprocv3.ProcessingRequest_RequestHeaders{
					RequestHeaders: &extprocv3.HttpHeaders{
						Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{
							{Key: ":method", RawValue: []byte("GET")},
							{Key: ":authority", RawValue: []byte("api.example.com")},
							{Key: "connection", RawValue: []byte("keep-alive, Upgrade")},
							{Key: "upgrade", RawValue: []byte("websocket")},
						}},
					},
				},
				Attributes: map[string]*structpb.Struct{
					"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
						extproc.FilterChainNameAttribute: structpb.NewStringValue(leg),
					}},
				},
			}
			stream := &websocketProcessStream{ctx: context.Background(), requests: []*extprocv3.ProcessingRequest{request}}
			server := extproc.NewServer(0, nil, extproc.Handlers{
				extproc.DirectionEgress: egress.New(nil, nil, 0, nil, ""),
			})

			if err := server.Process(stream); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			if len(stream.responses) != 1 {
				t.Fatalf("responses = %d, want 1", len(stream.responses))
			}
			response := stream.responses[0].GetImmediateResponse()
			if response == nil {
				t.Fatalf("response = %v, want immediate denial", stream.responses[0])
			}
			if got := response.GetStatus().GetCode(); got != 403 {
				t.Errorf("status = %d, want 403", got)
			}
			if got := string(response.GetBody()); got != websocketDenialBody {
				t.Errorf("body = %q, want %q", got, websocketDenialBody)
			}
		})
	}
}
