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

package ateinterceptors

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/protoredact"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ServerElapsedTrailer carries the server's handler duration in microseconds,
// so clients can report a latency unaffected by their own scheduling overhead.
const ServerElapsedTrailer = "x-server-elapsed-us"

func ServerUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	startTime := time.Now()

	resp, err := handler(ctx, req)

	elapsed := time.Since(startTime)

	// Observability trailer; failure here must not affect the RPC outcome.
	_ = grpc.SetTrailer(ctx, metadata.Pairs(
		ServerElapsedTrailer,
		strconv.FormatInt(elapsed.Microseconds(), 10),
	))

	pInfo, _ := principal.FromContext(ctx)

	slog.InfoContext(ctx, "Handle RPC",
		slog.String("method", info.FullMethod),
		slog.Any("req", protoredact.ForLog(req)),
		slog.Any("resp", protoredact.ForLog(resp)),
		slog.Any("err", err),
		slog.String("elapsed-time", elapsed.String()),
		slog.Any("principal", pInfo),
	)

	if err != nil {
		var statusErr interface {
			GRPCStatus() *status.Status
		}

		if errors.As(err, &statusErr) {
			return nil, statusErr.GRPCStatus().Err()
		}

		// No status error found in chain.
		return nil, status.Errorf(codes.Internal, "internal server error: %v", err)
	}

	return resp, err
}

// MaxDeadlineUnaryInterceptor returns an interceptor that caps the request context at maxDeadline.
func MaxDeadlineUnaryInterceptor(maxDeadline time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, cancel := context.WithTimeout(ctx, maxDeadline)
		defer cancel()
		return handler(ctx, req)
	}
}

// InternalServerUnaryInterceptor is for internal services to return full gRPC errors with specific error codes and debugging details.
func InternalServerUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	startTime := time.Now()

	resp, err := handler(ctx, req)

	slog.InfoContext(ctx, "Handle RPC",
		slog.String("method", info.FullMethod),
		slog.Any("req", protoredact.ForLog(req)),
		slog.Any("resp", protoredact.ForLog(resp)),
		slog.Any("err", err),
		slog.String("elapsed-time", time.Since(startTime).String()),
	)

	if err != nil {
		var statusErr interface {
			GRPCStatus() *status.Status
		}

		if errors.As(err, &statusErr) {
			return nil, statusErr.GRPCStatus().Err()
		}

		// No status error found in chain.
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, err
}
