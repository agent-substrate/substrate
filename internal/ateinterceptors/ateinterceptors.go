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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

	slog.InfoContext(ctx, "Handle RPC",
		slog.String("method", info.FullMethod),
		slog.Any("req", sanitizeForLog(req)),
		slog.Any("resp", sanitizeForLog(resp)),
		slog.Any("err", err),
		slog.String("elapsed-time", elapsed.String()),
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
		return nil, status.Errorf(codes.Internal, "internal server error: %v", err)
	}

	return resp, err
}

// InternalServerUnaryInterceptor is for internal services to return full gRPC errors with specific error codes and debugging details.
func InternalServerUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	startTime := time.Now()

	resp, err := handler(ctx, req)

	slog.InfoContext(ctx, "Handle RPC",
		slog.String("method", info.FullMethod),
		slog.Any("req", sanitizeForLog(req)),
		slog.Any("resp", sanitizeForLog(resp)),
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

func sanitizeForLog(v any) any {
	msg, ok := v.(proto.Message)
	if !ok {
		return v
	}

	clone := proto.Clone(msg)
	clearEnvFields(clone.ProtoReflect())
	return clone
}

func clearEnvFields(msg protoreflect.Message) {
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if fd.Name() == "env" {
			msg.Clear(fd)
			return true
		}
		if fd.IsMap() {
			return true
		}
		if fd.IsList() {
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
					clearEnvFields(list.Get(i).Message())
				}
			}
			return true
		}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			clearEnvFields(value.Message())
		}
		return true
	})
}
