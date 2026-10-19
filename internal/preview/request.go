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

package preview

import (
	"context"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Header is the gRPC metadata key with which an API client says it intends
// to use preview features. It is a coarse guard against using them by
// accident, not an access control: any client may send it, and it applies to
// every enabled gate at once. Its value is a boolean, e.g. "true".
const Header = "Ate-Preview-Allowed"

type restrictedKey struct{}

// ContextWithOptOut marks ctx as being opted-out from preview features.
func ContextWithOptOut(ctx context.Context) context.Context {
	return context.WithValue(ctx, restrictedKey{}, true)
}

// ContextWithOptIn marks ctx as being opted-in to preview features, overriding
// any earlier ContextWithOptOut. Code which acts for the system rather than
// for a user, such as workflows, uses it so that work which an API request
// started is not limited by that request's opt-out.
func ContextWithOptIn(ctx context.Context) context.Context {
	return context.WithValue(ctx, restrictedKey{}, false)
}

// IsOptedOut reports whether the context has opted-out of using preview
// features.  By default, contexts are allowed to use preview, but some
// call-paths (e.g. public API) may restrict that, so we provide this mechanism
// to explicitly opt out.
func IsOptedOut(ctx context.Context) bool {
	restricted, _ := ctx.Value(restrictedKey{}).(bool)
	return restricted
}

// IsEnabledFor reports whether gate is enabled in this process and ctx has
// not opted-out of preview features.
func IsEnabledFor(ctx context.Context, gate Gate) bool {
	return IsEnabled(gate) && !IsOptedOut(ctx)
}

// requestOptedIn reports whether the incoming request in ctx sent Header
// with a true value.
func requestOptedIn(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get(Header) {
		if b, err := strconv.ParseBool(v); err == nil && b {
			return true
		}
	}
	return false
}

func optOutIfNotOptIn(ctx context.Context) context.Context {
	if requestOptedIn(ctx) {
		return ctx
	}
	return ContextWithOptOut(ctx)
}

// UnaryServerInterceptor restricts each request that did not send Header.
func UnaryServerInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(optOutIfNotOptIn(ctx), req)
}

// StreamServerInterceptor restricts each stream that did not send Header.
func StreamServerInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, &restrictedStream{ServerStream: ss, ctx: optOutIfNotOptIn(ss.Context())})
}

type restrictedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *restrictedStream) Context() context.Context { return s.ctx }

// DialOptionsWithOptIn returns gRPC dial options which send Header on every call made
// over the connection.
func DialOptionsWithOptIn() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(metadata.AppendToOutgoingContext(ctx, Header, "true"), method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(metadata.AppendToOutgoingContext(ctx, Header, "true"), desc, cc, method, opts...)
		}),
	}
}
