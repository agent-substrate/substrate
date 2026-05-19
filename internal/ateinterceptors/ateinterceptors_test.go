// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package ateinterceptors

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatusErrorInterceptor(t *testing.T) {
	tests := []struct {
		name           string
		handlerErr     error
		wantCode       codes.Code
		wantMsg        string
		expectResponse bool
	}{
		{
			name:           "Success",
			handlerErr:     nil,
			expectResponse: true,
		},
		{
			name:       "StatusErrorInChain",
			handlerErr: fmt.Errorf("outer error: %w", status.Error(codes.NotFound, "actor not found")),
			wantCode:   codes.NotFound,
			wantMsg:    "actor not found",
		},
		{
			name:       "RawErrorFallback",
			handlerErr: errors.New("database connection failed"),
			wantCode:   codes.Internal,
			wantMsg:    "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				return "response", tt.handlerErr
			}

			resp, err := ServerUnaryInterceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)

			if tt.expectResponse {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if resp != "response" {
					t.Errorf("expected response 'response', got %v", resp)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error, got nil")
			}

			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected gRPC status error, got: %v", err)
			}

			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
			}

			if st.Message() != tt.wantMsg {
				t.Errorf("expected message %q, got %q", tt.wantMsg, st.Message())
			}
		})
	}
}
