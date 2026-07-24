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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
			wantMsg:    "internal server error: database connection failed",
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

// errorInfoOf returns the ErrorInfo detail carried by err, or nil if none.
func errorInfoOf(t *testing.T, err error) *epb.ErrorInfo {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) = _, false; want a status error", err)
	}
	for _, d := range st.Details() {
		if info, ok := d.(*epb.ErrorInfo); ok {
			return info
		}
	}
	return nil
}

// TestInternalServerUnaryInterceptorPreservesDetails verifies the interceptor
// returns structured errors (from NewGRPCError) intact — preserving the code and
// the ErrorInfo carrying the Reason — while collapsing plain errors to Internal
// with no ErrorInfo detail.
func TestInternalServerUnaryInterceptorPreservesDetails(t *testing.T) {
	tests := []struct {
		name          string
		handlerErr    error
		wantCode      codes.Code
		wantReason    string
		wantErrorInfo bool
	}{
		{
			name:          "structured error keeps code and reason",
			handlerErr:    ateerrors.NewGRPCError(context.Background(), codes.DataLoss, ateerrors.ReasonFaileSaveSnapshot, ateerrors.ActorCrashedMetadata(), errors.New("boom")),
			wantCode:      codes.DataLoss,
			wantReason:    string(ateerrors.ReasonFaileSaveSnapshot),
			wantErrorInfo: true,
		},
		{
			name:          "plain error collapses to Internal with no ErrorInfo",
			handlerErr:    errors.New("database connection failed"),
			wantCode:      codes.Internal,
			wantErrorInfo: false,
		},
	}

	interceptor := InternalServerUnaryInterceptor
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, tt.handlerErr
			}

			_, err := interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("code = %v, want %v", st.Code(), tt.wantCode)
			}

			info := errorInfoOf(t, err)
			if !tt.wantErrorInfo {
				if info != nil {
					t.Errorf("ErrorInfo = %v, want none", info)
				}
				return
			}
			if info == nil {
				t.Fatal("status is missing the ErrorInfo detail")
			}
			if got := info.GetReason(); got != tt.wantReason {
				t.Errorf("ErrorInfo.Reason = %q, want %q", got, tt.wantReason)
			}
		})
	}
}

type trailerStream struct {
	method   string
	trailers metadata.MD
}

func (s *trailerStream) Method() string                  { return s.method }
func (s *trailerStream) SetHeader(md metadata.MD) error  { return nil }
func (s *trailerStream) SendHeader(md metadata.MD) error { return nil }
func (s *trailerStream) SetTrailer(md metadata.MD) error {
	if s.trailers == nil {
		s.trailers = metadata.MD{}
	}
	for k, v := range md {
		s.trailers[k] = append(s.trailers[k], v...)
	}
	return nil
}

func TestServerUnaryInterceptorEmitsElapsedTrailer(t *testing.T) {
	const minHandlerDuration = 5 * time.Millisecond
	stream := &trailerStream{method: "/test.Service/Method"}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		time.Sleep(minHandlerDuration)
		return "response", nil
	}

	if _, err := ServerUnaryInterceptor(ctx, "request", &grpc.UnaryServerInfo{FullMethod: stream.method}, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vals := stream.trailers.Get(ServerElapsedTrailer)
	if len(vals) != 1 {
		t.Fatalf("expected one %s trailer, got %v", ServerElapsedTrailer, vals)
	}
	elapsedUs, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		t.Fatalf("could not parse %s as int64: %v", vals[0], err)
	}
	if got, min := time.Duration(elapsedUs)*time.Microsecond, minHandlerDuration; got < min {
		t.Errorf("trailer reported %s; expected at least %s (handler sleep)", got, min)
	}
}

func TestMaxDeadlineUnaryInterceptor_MaxDeadlineIsEnforced(t *testing.T) {
	const ceiling = 50 * time.Millisecond

	tests := []struct {
		name      string
		callerCtx func() (context.Context, context.CancelFunc)
	}{
		{
			name: "no caller deadline",
			callerCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
		{
			name: "caller deadline longer than ceiling",
			callerCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Hour)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interceptor := MaxDeadlineUnaryInterceptor(ceiling)

			callerCtx, cancel := tt.callerCtx()
			defer cancel()

			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				gotDeadline, ok := ctx.Deadline()
				if !ok {
					t.Fatalf("expected the ceiling deadline to be present")
				}
				if until := time.Until(gotDeadline); until > ceiling {
					t.Errorf("time until deadline = %v, want at most the %v ceiling", until, ceiling)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}

			_, err := interceptor(callerCtx, "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("err = %v, want context.DeadlineExceeded (should not have waited out the caller's own deadline)", err)
			}
		})
	}
}

func TestMaxDeadlineUnaryInterceptor_ShorterDeadlineIsPreserved(t *testing.T) {
	interceptor := MaxDeadlineUnaryInterceptor(time.Hour)

	callerCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	callerDeadline, _ := callerCtx.Deadline()

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		gotDeadline, ok := ctx.Deadline()
		if !ok {
			t.Fatalf("expected the caller's shorter deadline to be preserved")
		}
		if !gotDeadline.Equal(callerDeadline) {
			t.Errorf("deadline = %v, want caller's deadline %v (a ceiling above the caller's own deadline must not override it)", gotDeadline, callerDeadline)
		}
		return "response", nil
	}

	if _, err := interceptor(callerCtx, "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServerUnaryInterceptorRedactsEnvFromProtoRequestLogs(t *testing.T) {
	var log bytes.Buffer
	origLogger := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(origLogger)
	})
	slog.SetDefault(slog.New(slog.NewJSONHandler(&log, nil)))

	req := &ateletpb.RunRequest{
		Spec: &ateletpb.WorkloadSpec{
			Containers: []*ateletpb.Container{
				{
					Name: "main",
					Env: []*ateletpb.EnvEntry{
						{Name: "API_KEY", Value: "sk-secret"},
					},
				},
			},
		},
	}

	_, err := ServerUnaryInterceptor(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: "/atelet.AteomHerder/Run"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return &ateletpb.RunResponse{}, nil
	})
	if err != nil {
		t.Fatalf("ServerUnaryInterceptor failed: %v", err)
	}

	gotLog := log.String()
	if strings.Contains(gotLog, "sk-secret") || strings.Contains(gotLog, "API_KEY") {
		t.Fatalf("log contains env data: %s", gotLog)
	}
	if len(req.GetSpec().GetContainers()[0].GetEnv()) != 1 {
		t.Fatalf("interceptor mutated original request")
	}
}
