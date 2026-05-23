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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ServerUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	startTime := time.Now()

	resp, err := handler(ctx, req)

	slog.InfoContext(ctx, "Handle RPC",
		slog.String("method", info.FullMethod),
		slog.Any("req", req),
		slog.Any("resp", resp),
		slog.Any("err", err),
		slog.String("elapsed-time", time.Since(startTime).String()),
	)

	if err != nil {
		var statusErr interface {
			GRPCStatus() *status.Status
		}

		if errors.As(err, &statusErr) {
			st := statusErr.GRPCStatus()
			return nil, status.Error(st.Code(), st.Message())
		}

		// No status error found in chain.
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return resp, err
}
