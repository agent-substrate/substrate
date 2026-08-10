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

package otlprelay

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial opens the ateom half of the relay: a gRPC connection over atelet's unix
// socket, to be handed to the OTLP exporters via serverboot's ExporterConn.
//
// It returns (nil, nil) when sockPath is empty or absent, which the caller reads
// as "export directly instead". The existence check is what makes the fallback
// deterministic at startup: grpc.NewClient is lazy, so a connection to a missing
// socket would be created happily and only fail later, per export, with the
// telemetry already lost. Losing spans is not worth failing ateom over either,
// hence a fallback rather than an error.
//
// The connection is plaintext by design. A unix socket cannot leave the node, so
// there is no transport to protect; access is controlled by the socket's file
// permissions instead (see socketMode).
func Dial(ctx context.Context, sockPath string) (*grpc.ClientConn, error) {
	if sockPath == "" {
		return nil, nil
	}
	if err := validateSocketPath(sockPath); err != nil {
		return nil, err
	}
	if _, err := os.Stat(sockPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			slog.WarnContext(ctx, "OTLP relay socket absent, exporting telemetry directly over the pod network",
				slog.String("socket", sockPath))
			return nil, nil
		}
		return nil, fmt.Errorf("while checking the OTLP relay socket %q: %w", sockPath, err)
	}

	// gRPC resolves a "unix://" target to a unix socket dialer natively, so the
	// OTLP exporters above this connection are unchanged: OTLP is gRPC, and gRPC
	// needs only a reliable byte stream.
	conn, err := grpc.NewClient("unix://"+sockPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("while dialing the OTLP relay socket %q: %w", sockPath, err)
	}
	slog.InfoContext(ctx, "Exporting telemetry through the atelet OTLP relay", slog.String("socket", sockPath))
	return conn, nil
}
