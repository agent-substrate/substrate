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

package main

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/protoredact"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// requestLogHerder answers Run and fails Restore, so the test covers the
// request log of a handled and of a failed RPC.
type requestLogHerder struct {
	ateletpb.UnimplementedAteomHerderServer
}

func (requestLogHerder) Run(context.Context, *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	return &ateletpb.RunResponse{}, nil
}

func (requestLogHerder) Restore(context.Context, *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	return nil, apierror.Unavailable("asset fetch failed")
}

// TestAteomHerderRequestLogMasksSignedAssetURL sends Run and Restore with a
// signed https asset URL through a server built with atelet's own options and
// logger, and checks the request log carries neither the URL's user info nor
// its query.
func TestAteomHerderRequestLogMasksSignedAssetURL(t *testing.T) {
	const (
		signedURL = "https://asset-user:userinfo-secret@storage.example.com/bucket/gvisor.tar.zstd?X-Goog-Signature=query-secret&X-Goog-Expires=900"
		sum       = "d547d81401461fd1c679c5c4fa0a6c2b8ef7dc3c22ce23c9e25dcc4c69cfd06f"
	)
	var logBuf bytes.Buffer
	origLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(origLogger) })
	serverboot.InitLoggerWithWriter(&logBuf)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(ateomHerderServerOptions(insecure.NewCredentials())...)
	ateletpb.RegisterAteomHerderServer(srv, requestLogHerder{})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := ateletpb.NewAteomHerderClient(conn)

	assets := &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		Assets: map[string]*ateletpb.ArchAssets{
			"amd64": {Files: map[string]*ateletpb.AssetFile{"gvisor": {Url: signedURL, Sha256: sum}}},
		},
	}
	ctx := t.Context()
	if _, err := client.Run(ctx, &ateletpb.RunRequest{ActorUid: "uid-run", SandboxAssets: assets}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := client.Restore(ctx, &ateletpb.RestoreRequest{ActorUid: "uid-restore", SandboxAssets: assets}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Restore err = %v, want Unavailable", err)
	}
	srv.Stop()

	gotLog := logBuf.String()
	for _, leak := range []string{"userinfo-secret", "query-secret", "asset-user", "X-Goog-Signature"} {
		if strings.Contains(gotLog, leak) {
			t.Fatalf("request log contains %q from the signed asset URL:\n%s", leak, gotLog)
		}
	}
	for _, method := range []string{ateletpb.AteomHerder_Run_FullMethodName, ateletpb.AteomHerder_Restore_FullMethodName} {
		var line string
		for l := range strings.Lines(gotLog) {
			if strings.Contains(l, `"method":"`+method+`"`) {
				line = l
			}
		}
		if line == "" {
			t.Fatalf("no request log line for %s:\n%s", method, gotLog)
		}
		// The sha256 still names the asset.
		for _, want := range []string{`"url":"` + protoredact.Placeholder + `"`, `"sha256":"` + sum + `"`} {
			if !strings.Contains(line, want) {
				t.Fatalf("%s request log missing %s:\n%s", method, want, line)
			}
		}
	}
}
