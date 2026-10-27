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
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestUnaryServerInterceptor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		want   bool
	}{
		{name: "no metadata", want: false},
		{name: "true", values: []string{"true"}, want: true},
		{name: "TRUE", values: []string{"TRUE"}, want: true},
		{name: "false", values: []string{"false"}, want: false},
		{name: "garbage", values: []string{"yes please"}, want: false},
		{name: "empty", values: []string{""}, want: false},
		{name: "any true", values: []string{"false", "true"}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.values != nil {
				md := metadata.MD{}
				md.Append(Header, tc.values...)
				ctx = metadata.NewIncomingContext(ctx, md)
			}
			var got bool
			_, err := UnaryServerInterceptor(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
				got = !IsOptedOut(ctx)
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("opted in (!IsOptedOut()) = %v, want %v", got, tc.want)
			}
		})
	}
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s fakeStream) Context() context.Context { return s.ctx }

func TestStreamServerInterceptor(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		ctx := context.Background()
		if allowed {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(Header, "true"))
		}
		var got bool
		err := StreamServerInterceptor(nil, fakeStream{ctx: ctx}, &grpc.StreamServerInfo{}, func(_ any, ss grpc.ServerStream) error {
			got = !IsOptedOut(ss.Context())
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if got != allowed {
			t.Errorf("header sent = %v: opted in (!IsOptedOut()) = %v", allowed, got)
		}
	}
}

func TestContextWithOptInOut(t *testing.T) {
	ctx := context.Background()
	if IsOptedOut(ctx) {
		t.Errorf("IsOptedOut(unmarked) = true, want false")
	}
	ctx = ContextWithOptOut(ctx)
	if !IsOptedOut(ctx) {
		t.Errorf("IsOptedOut(opted out) = false, want true")
	}
	ctx = ContextWithOptIn(ctx)
	if IsOptedOut(ctx) {
		t.Errorf("IsOptedOut(opted out, then in) = true, want false")
	}
	ctx = ContextWithOptOut(ctx)
	if !IsOptedOut(ctx) {
		t.Errorf("IsOptedOut(opted out, in, then out) = false, want true")
	}
}

func TestIsEnabledFor(t *testing.T) {
	InitForTestFake(t, []Gate{"A", "B"}, "A")
	for _, tc := range []struct {
		name string
		ctx  context.Context
		gate Gate
		want bool
	}{
		{name: "enabled, unmarked", ctx: context.Background(), gate: "A", want: true},
		{name: "enabled, opted out", ctx: ContextWithOptOut(context.Background()), gate: "A", want: false},
		{name: "enabled, opted in", ctx: ContextWithOptIn(ContextWithOptOut(context.Background())), gate: "A", want: true},
		{name: "disabled, opted in", ctx: ContextWithOptIn(context.Background()), gate: "B", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsEnabledFor(tc.ctx, tc.gate); got != tc.want {
				t.Errorf("IsEnabledFor(%s) = %v, want %v", tc.gate, got, tc.want)
			}
		})
	}
}

func TestDialOptions(t *testing.T) {
	lis := bufconn.Listen(1 << 16)
	allowed := make(chan bool, 1)
	srv := grpc.NewServer(
		grpc.ChainStreamInterceptor(StreamServerInterceptor),
		grpc.UnknownServiceHandler(func(_ any, ss grpc.ServerStream) error {
			allowed <- !IsOptedOut(ss.Context())
			return status.Error(codes.Unimplemented, "test")
		}),
	)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dial := func(t *testing.T, extra ...grpc.DialOption) *grpc.ClientConn {
		opts := append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		}, extra...)
		cc, err := grpc.NewClient("passthrough://bufnet", opts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cc.Close() })
		return cc
	}

	for _, tc := range []struct {
		name string
		opts []grpc.DialOption
		want bool
	}{
		{name: "without", want: false},
		{name: "with", opts: DialOptionsWithOptIn(), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc := dial(t, tc.opts...)

			_ = cc.Invoke(t.Context(), "/test.Svc/Unary", &emptypb.Empty{}, &emptypb.Empty{})
			if got := <-allowed; got != tc.want {
				t.Errorf("unary: opted in (!IsOptedOut()) on server = %v, want %v", got, tc.want)
			}

			stream, err := cc.NewStream(t.Context(), &grpc.StreamDesc{ServerStreams: true}, "/test.Svc/Stream")
			if err != nil {
				t.Fatal(err)
			}
			_ = stream.RecvMsg(&emptypb.Empty{})
			if got := <-allowed; got != tc.want {
				t.Errorf("stream: opted in (!IsOptedOut()) on server = %v, want %v", got, tc.want)
			}
		})
	}
}
