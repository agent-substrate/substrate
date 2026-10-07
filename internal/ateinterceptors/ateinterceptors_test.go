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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/protoredact"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
	"go.opentelemetry.io/otel/trace"
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
			name:       "WrappedStatusBecomesInternal",
			handlerErr: fmt.Errorf("outer error: %w", status.Error(codes.NotFound, "actor not found")),
			wantCode:   codes.Internal,
			wantMsg:    "internal server error",
		},
		{
			name:       "RawErrorFallback",
			handlerErr: errors.New("database connection failed"),
			wantCode:   codes.Internal,
			wantMsg:    "internal server error",
		},
		{
			name:       "APIErrorInChain",
			handlerErr: fmt.Errorf("workflow failed at step Load: %w", apierror.NotFound("actor not found")),
			wantCode:   codes.NotFound,
			wantMsg:    "actor not found",
		},
		{
			name:       "APIErrorWinsOverStatusItWraps",
			handlerErr: apierror.Internal("while calling atelet: %w", status.Error(codes.Unavailable, "atelet down")),
			wantCode:   codes.Internal,
			wantMsg:    "while calling atelet: rpc error: code = Unavailable desc = atelet down",
		},
		{
			name:       "ContextErrorKeepsItsCode",
			handlerErr: fmt.Errorf("while loading actor: %w", context.DeadlineExceeded),
			wantCode:   codes.DeadlineExceeded,
			wantMsg:    "while loading actor: context deadline exceeded",
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

func TestServerUnaryInterceptorUnexpectedErrorDiagnostics(t *testing.T) {
	const backendMessage = "database connection to 10.0.0.7:5432 failed"
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1},
		SpanID:  trace.SpanID{2},
	})
	for _, traced := range []bool{false, true} {
		t.Run(strconv.FormatBool(traced), func(t *testing.T) {
			log := captureDefaultLog(t)
			ctx := t.Context()
			wantMessage := "internal server error"
			if traced {
				ctx = trace.ContextWithSpanContext(ctx, sc)
				wantMessage += " (trace_id: " + sc.TraceID().String() + ")"
			}
			backendErr := fmt.Errorf("while loading actor: %w", errors.New(backendMessage))
			resp, err := ServerUnaryInterceptor(ctx, "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, func(context.Context, any) (any, error) {
				return nil, backendErr
			})
			st := status.Convert(err)
			if resp != nil || st.Code() != codes.Internal || st.Message() != wantMessage {
				t.Fatalf("response = %v, status = %v; want nil, Internal %q", resp, st, wantMessage)
			}
			var record struct {
				Error   string `json:"err"`
				TraceID string `json:"trace_id"`
			}
			if err := json.Unmarshal(log.Bytes(), &record); err != nil {
				t.Fatalf("decode log: %v", err)
			}
			if record.Error != backendErr.Error() {
				t.Errorf("logged error = %q, want %q", record.Error, backendErr)
			}
			if traced && record.TraceID != sc.TraceID().String() {
				t.Errorf("logged trace ID = %q, want %q", record.TraceID, sc.TraceID())
			}
		})
	}
}

// statusWithErrorInfo builds a status error carrying an AIP-193 ErrorInfo
// detail, standing in for a structured error built by an upstream service.
func statusWithErrorInfo(t *testing.T, code codes.Code, reason string, md map[string]string) error {
	t.Helper()
	st, err := status.New(code, "boom").WithDetails(&epb.ErrorInfo{
		Domain:   "substrate.dev",
		Reason:   reason,
		Metadata: md,
	})
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	return st.Err()
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

func TestInternalServerUnaryInterceptorCodes(t *testing.T) {
	tests := []struct {
		name       string
		handlerErr error
		wantCode   codes.Code
		wantMsg    string
	}{
		{
			name:       "apierror keeps its code",
			handlerErr: apierror.FailedPrecondition("snapshot is corrupt"),
			wantCode:   codes.FailedPrecondition,
			wantMsg:    "snapshot is corrupt",
		},
		{
			name:       "wrapped apierror keeps its code",
			handlerErr: fmt.Errorf("while restoring: %w", apierror.NotFound("actor not found")),
			wantCode:   codes.NotFound,
			wantMsg:    "actor not found",
		},
		{
			name:       "upstream status becomes Internal without its details",
			handlerErr: fmt.Errorf("while calling ateom: %w", statusWithErrorInfo(t, codes.DataLoss, "FAILED_SAVE_SNAPSHOT", nil)),
			wantCode:   codes.Internal,
			wantMsg:    "while calling ateom: rpc error: code = DataLoss desc = boom",
		},
		{
			name:       "wrapped upstream status becomes Internal",
			handlerErr: fmt.Errorf("while calling downstream: %w", status.Error(codes.Unavailable, "backend down")),
			wantCode:   codes.Internal,
			wantMsg:    "while calling downstream: rpc error: code = Unavailable desc = backend down",
		},
		{
			name:       "wrapped context error keeps the context's code",
			handlerErr: fmt.Errorf("gave up waiting for the actor's lock: %w", context.Canceled),
			wantCode:   codes.Canceled,
			wantMsg:    "gave up waiting for the actor's lock: context canceled",
		},
		{
			name:       "wrapped plain error becomes Internal",
			handlerErr: fmt.Errorf("while parsing manifest: %w", errors.New("bad json")),
			wantCode:   codes.Internal,
			wantMsg:    "while parsing manifest: bad json",
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
			if st.Message() != tt.wantMsg {
				t.Errorf("message = %q, want %q", st.Message(), tt.wantMsg)
			}
			if info := errorInfoOf(t, err); info != nil {
				t.Errorf("ErrorInfo = %v, want none", info)
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

// captureDefaultLog installs the handler stack the servers run with
// (serverboot.InitLogger: contextlogging over the JSON handler), since the
// interceptor relies on that handler for redaction.
func captureDefaultLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var log bytes.Buffer
	origLogger := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(origLogger)
	})
	serverboot.InitLoggerWithWriter(&log)
	return &log
}

func TestServerUnaryInterceptorRequestLogMasksEnvValues(t *testing.T) {
	log := captureDefaultLog(t)

	req := &ateletpb.RunRequest{
		Spec: &ateletpb.WorkloadSpec{
			Containers: []*ateletpb.Container{
				{
					Name: "main",
					Env: []*ateletpb.EnvEntry{
						{Name: "API_KEY", Value: "sk-secret"},
						{Name: "PLAIN", Value: "not-a-secret"},
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
	for _, secret := range []string{"sk-secret", "not-a-secret"} {
		if strings.Contains(gotLog, secret) {
			t.Fatalf("log contains env value %q: %s", secret, gotLog)
		}
	}
	// Names survive so the log still shows which variables were set.
	for _, want := range []string{`"name":"API_KEY"`, `"name":"PLAIN"`, `"value":"` + protoredact.Placeholder + `"`} {
		if !strings.Contains(gotLog, want) {
			t.Fatalf("log missing %s: %s", want, gotLog)
		}
	}
	if got := req.GetSpec().GetContainers()[0].GetEnv()[0].GetValue(); got != "sk-secret" {
		t.Fatalf("interceptor mutated original request: env value = %q", got)
	}
}

func TestServerUnaryInterceptorResponseLogMasksActorJWT(t *testing.T) {
	log := captureDefaultLog(t)

	const token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhY3RvciJ9.c2lnbmF0dXJl"
	resp := &ateapipb.MintActorJWTResponse{ActorJwt: token}

	got, err := ServerUnaryInterceptor(context.Background(), &ateapipb.MintActorJWTRequest{}, &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/MintActorJWT"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return resp, nil
	})
	if err != nil {
		t.Fatalf("ServerUnaryInterceptor failed: %v", err)
	}

	gotLog := log.String()
	if strings.Contains(gotLog, token) {
		t.Fatalf("log contains the actor JWT: %s", gotLog)
	}
	if !strings.Contains(gotLog, `"actor_jwt":"`+protoredact.Placeholder+`"`) {
		t.Fatalf("log does not show the redacted actor_jwt: %s", gotLog)
	}
	if got.(*ateapipb.MintActorJWTResponse).GetActorJwt() != token {
		t.Fatalf("interceptor mutated the response returned to the client")
	}
}

func TestInternalServerUnaryInterceptorLogMasksBytesFields(t *testing.T) {
	log := captureDefaultLog(t)

	resp := &credproviderpb.FetchSecretResponse{OpaqueBytes: []byte("hunter2-hunter2")}
	_, err := InternalServerUnaryInterceptor(context.Background(), &credproviderpb.FetchSecretRequest{Uri: "ate-secret://kubernetes.io/ns/name"}, &grpc.UnaryServerInfo{FullMethod: "/credprovider.CredentialProvider/FetchSecret"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return resp, nil
	})
	if err != nil {
		t.Fatalf("InternalServerUnaryInterceptor failed: %v", err)
	}

	gotLog := log.String()
	// encoding/json renders []byte as base64; check both forms are absent.
	for _, leak := range []string{"hunter2-hunter2", "aHVudGVyMi1odW50ZXIy"} {
		if strings.Contains(gotLog, leak) {
			t.Fatalf("log contains the fetched secret: %s", gotLog)
		}
	}
	if !strings.Contains(gotLog, "ate-secret://kubernetes.io/ns/name") {
		t.Fatalf("log lost the non-sensitive request: %s", gotLog)
	}
	if string(resp.GetOpaqueBytes()) != "hunter2-hunter2" {
		t.Fatalf("interceptor mutated the response returned to the client")
	}
}

func TestServerUnaryInterceptorLogsNilResponseOnHandlerError(t *testing.T) {
	log := captureDefaultLog(t)
	_, err := ServerUnaryInterceptor(context.Background(), &ateapipb.MintActorJWTRequest{}, &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/MintActorJWT"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, apierror.PermissionDenied("no")
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(log.String(), `"resp":null`) {
		t.Errorf("nil response should log as null: %s", log.String())
	}
}
