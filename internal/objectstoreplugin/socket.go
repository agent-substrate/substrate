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

package objectstoreplugin

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// ReadyWait is the ready wait servers pass to Dial. It rides through a sidecar
// restart, including the first 10s step of kubelet's crash-loop back-off, and
// fails a plugin that stays down well before a caller's deadline would.
const ReadyWait = 30 * time.Second

// reconnectParams keeps reconnect attempts to the local socket frequent, so a
// plugin that comes back is reached within the ready wait; gRPC's default back-off
// grows to two minutes.
var reconnectParams = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   time.Second,
	},
	MinConnectTimeout: 5 * time.Second,
}

// Listen removes any stale socket at path and listens on a fresh one that
// only the socket's owner can connect to.
func Listen(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("while creating socket directory: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("while removing stale socket %s: %w", path, err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("while listening on %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		lis.Close()
		return nil, fmt.Errorf("while restricting socket %s: %w", path, err)
	}
	return lis, nil
}

// Dial returns a long-lived client connection to a plugin socket. A unary
// call waits up to readyWait for the connection to become ready, then fails
// with codes.Unavailable, so a plugin that is lost after startup fails calls
// promptly while a brief restart does not. A call that starts on a ready
// connection is not bounded by readyWait, and one in flight when the plugin
// goes fails with codes.Unavailable at once. A caller that passes
// grpc.WaitForReady itself opts out of the bound and waits as long as its
// context allows; WaitReady does so at startup.
//
// The bound covers unary calls only. A streaming call gets gRPC's plain
// fail-fast and fails at once while the plugin restarts, so the first
// streaming RPC on this connection needs a grpc.WithStreamInterceptor that
// applies the same wait.
//
// The connection is unauthenticated: the socket is reachable only from
// inside the pod, and only by its owner.
func Dial(path string, readyWait time.Duration) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient("unix://"+path,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithConnectParams(reconnectParams),
		grpc.WithUnaryInterceptor(boundedWaitForReady(readyWait)),
	)
	if err != nil {
		return nil, fmt.Errorf("while dialing snapshot plugin at %s: %w", path, err)
	}
	return conn, nil
}

// boundedWaitForReady waits up to wait for cc to become ready before issuing
// a call that sets no grpc.WaitForReady option, then issues it fail-fast.
func boundedWaitForReady(wait time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		for _, opt := range opts {
			if _, ok := opt.(grpc.FailFastCallOption); ok {
				return invoker(ctx, method, req, reply, cc, opts...)
			}
		}
		if err := awaitReady(ctx, cc, wait); err != nil {
			return err
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// awaitReady returns nil once cc is ready or shut down, the latter left for
// the call itself to report. It returns codes.Unavailable if cc is not ready
// within wait, and ctx's error as a status if ctx ends first.
func awaitReady(ctx context.Context, cc *grpc.ClientConn, wait time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		state := cc.GetState()
		switch state {
		case connectivity.Ready, connectivity.Shutdown:
			return nil
		case connectivity.Idle:
			cc.Connect()
		}
		if !cc.WaitForStateChange(waitCtx, state) {
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			return status.Errorf(codes.Unavailable, "snapshot plugin at %s is not ready after %s (connection %s)", cc.Target(), wait, state)
		}
	}
}

// WaitReady blocks until the plugin behind conn reports that it is serving,
// or ctx ends, however long the plugin takes to come up. Bounding ctx is what
// turns a plugin that never comes up (a wrong socket path, a crash loop) into
// an error at startup.
func WaitReady(ctx context.Context, conn *grpc.ClientConn) error {
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("while waiting for the snapshot plugin at %s: %w", conn.Target(), err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("snapshot plugin at %s is %s", conn.Target(), resp.GetStatus())
	}
	return nil
}

// CallError returns the error a server reports to its own caller for a failed
// plugin call. codes.Unavailable, which a call reports when the plugin cannot
// be reached, becomes an apierror.Unavailable so the server's caller retries
// rather than treating it as Internal. Any other error is returned unchanged.
func CallError(err error) error {
	if status.Code(err) == codes.Unavailable {
		return apierror.Unavailable("snapshot plugin: %w", err)
	}
	return err
}
