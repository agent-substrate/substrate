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

package register

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

// maxCrashMessageBytes matches the control plane's limit on a crash message.
const maxCrashMessageBytes = 4096

// CrashReporter tells the control plane, through the node-local atelet, that
// the sandbox of an actor this ateworker hosts died on its own.
type CrashReporter struct {
	socketPath string
	tlsConfig  *tls.Config
}

// NewCrashReporter loads the ateworker's credentials once, at startup. Only the
// connection fields of cfg are used.
func NewCrashReporter(cfg ReportConfig) (*CrashReporter, error) {
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return nil, fmt.Errorf("crash reporter: %w", err)
	}
	return &CrashReporter{socketPath: cfg.SocketPath, tlsConfig: tlsConfig}, nil
}

// ReportCrashed reports that actor's sandbox died, retrying until the control
// plane answers or ctx ends. A crash stays true, so unlike a suspend request it
// is worth retrying for as long as the caller allows. A refusal, the actor
// having moved on or an operation in flight owning it, is an answer and is
// logged rather than returned.
func (r *CrashReporter) ReportCrashed(ctx context.Context, actor resources.ActorAttribution, message string) error {
	conn, err := ateletdial.Dial(r.socketPath, r.tlsConfig)
	if err != nil {
		return fmt.Errorf("dial atelet: %w", err)
	}
	defer conn.Close()
	return reportCrashedWithRetry(ctx, conn, actor, message, initialReportBackoff)
}

func reportCrashedWithRetry(ctx context.Context, conn grpc.ClientConnInterface, actor resources.ActorAttribution, message string, backoff time.Duration) error {
	req := &ateletpb.ReportActorCrashedRequest{
		ActorAtespace: actor.Ref.Atespace,
		ActorName:     actor.Ref.Name,
		ActorUid:      actor.UID,
		Message:       truncateMessage(message),
	}
	for {
		callCtx, cancel := context.WithTimeout(ctx, reportTimeout)
		_, err := ateletpb.NewWorkerSupportClient(conn).ReportActorCrashed(callCtx, req)
		cancel()
		switch status.Code(err) {
		case codes.OK:
			return nil
		case codes.NotFound, codes.FailedPrecondition:
			slog.InfoContext(ctx, "Control plane declined the actor crash report",
				slog.String("actor", actor.Ref.String()),
				slog.String("actor_uid", actor.UID),
				slog.Any("err", err))
			return nil
		case codes.Unavailable, codes.Aborted, codes.ResourceExhausted, codes.DeadlineExceeded:
		default:
			return fmt.Errorf("reporting actor crash: %w", err)
		}
		slog.WarnContext(ctx, "Retrying actor crash report", slog.Duration("in", backoff), slog.Any("err", err))
		select {
		case <-ctx.Done():
			return fmt.Errorf("reporting actor crash: %w (last error: %w)", ctx.Err(), err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxReportBackoff)
	}
}

// truncateMessage keeps message within the control plane's limit, on a rune
// boundary.
func truncateMessage(message string) string {
	message = strings.ToValidUTF8(message, "�")
	if len(message) <= maxCrashMessageBytes {
		return message
	}
	cut := maxCrashMessageBytes
	for cut > 0 && !utf8RuneStart(message[cut]) {
		cut--
	}
	return message[:cut]
}

func utf8RuneStart(b byte) bool {
	return b&0xC0 != 0x80
}
