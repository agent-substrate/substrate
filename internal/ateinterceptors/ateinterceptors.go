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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
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
		slog.Any("req", sanitizeForLog(req)),
		slog.Any("resp", sanitizeForLog(resp)),
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

// redactedPlaceholder replaces the value of a string field marked
// debug_redact in the logged copy of a message. Names and structure are kept
// so the log still shows which fields were set.
const redactedPlaceholder = "[REDACTED]"

// sanitizeForLog returns a copy of v safe to log. Proto messages are cloned
// and every field carrying the debug_redact option is masked; other values are
// returned unchanged. The original message is never modified.
func sanitizeForLog(v any) any {
	msg, ok := v.(proto.Message)
	if !ok {
		return v
	}

	clone := proto.Clone(msg)
	redactDebugRedactFields(clone.ProtoReflect())
	return clone
}

// isDebugRedact reports whether fd carries [debug_redact = true]. The option
// is set in the .proto files next to the fields it protects; see EnvVar.value,
// EnvEntry.value, MintActorJWTResponse.actor_jwt and
// FetchSecretResponse.opaque_bytes.
func isDebugRedact(fd protoreflect.FieldDescriptor) bool {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	return ok && opts.GetDebugRedact()
}

// redactDebugRedactFields masks, in place, every populated field of msg that
// carries the debug_redact option, recursing through nested messages, lists
// and map values. Singular string fields are replaced with
// redactedPlaceholder; any other kind (bytes, repeated, map, message, ...) is
// cleared.
func redactDebugRedactFields(msg protoreflect.Message) {
	msg.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if isDebugRedact(fd) {
			if fd.Kind() == protoreflect.StringKind && !fd.IsList() && !fd.IsMap() {
				msg.Set(fd, protoreflect.ValueOfString(redactedPlaceholder))
			} else {
				msg.Clear(fd)
			}
			return true
		}
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				value.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					redactDebugRedactFields(mv.Message())
					return true
				})
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				list := value.List()
				for i := 0; i < list.Len(); i++ {
					redactDebugRedactFields(list.Get(i).Message())
				}
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			redactDebugRedactFields(value.Message())
		}
		return true
	})
}
