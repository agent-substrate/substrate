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

	"github.com/agent-substrate/substrate/internal/apierror"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

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

// Dial returns a long-lived client connection to a plugin socket. Calls wait
// for the plugin to come up rather than failing while it starts, so a caller
// that must not hang on a plugin that never comes up checks WaitReady first.
//
// The connection is unauthenticated: the socket is reachable only from
// inside the pod, and only by its owner.
func Dial(path string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient("unix://"+path,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, fmt.Errorf("while dialing snapshot plugin at %s: %w", path, err)
	}
	return conn, nil
}

// WaitReady blocks until the plugin behind conn reports that it is serving,
// or ctx ends. Bounding ctx is what turns a plugin that never comes up (a
// wrong socket path, a crash loop) into an error at startup rather than a
// transfer that hangs.
func WaitReady(ctx context.Context, conn *grpc.ClientConn) error {
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
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
