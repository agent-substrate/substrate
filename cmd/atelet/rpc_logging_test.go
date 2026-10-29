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

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/protoredact"
	"go.ate.dev/substrate/internal/serverboot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// requestLogHerder answers Run and fails Restore, so the test covers the
// request log of a handled and of a failed RPC.
type requestLogHerder struct {
	ateletpb.UnimplementedAteletServer
}

func (requestLogHerder) Run(context.Context, *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	return &ateletpb.RunResponse{}, nil
}

func (requestLogHerder) Restore(context.Context, *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	return nil, apierror.Unavailable("asset fetch failed")
}

// TestAteletRequestLogMasksEnvVarValue sends Run and Restore with an
// environment variable value through a server built with atelet's own options
// and logger, and checks the request log masks the value while keeping the
// variable name.
func TestAteletRequestLogMasksEnvVarValue(t *testing.T) {
	const secretVal = "sk-secret-env-value"
	var logBuf bytes.Buffer
	origLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(origLogger) })
	serverboot.InitLoggerWithWriter(&logBuf)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(ateletServerOptions(insecure.NewCredentials())...)
	ateletpb.RegisterAteletServer(srv, requestLogHerder{})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := ateletpb.NewAteletClient(conn)

	spec := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{{
			Name: "main",
			Env:  []*ateletpb.EnvVar{{Name: "API_KEY", Value: secretVal}},
		}},
	}
	ctx := t.Context()
	if _, err := client.Run(ctx, &ateletpb.RunRequest{ActorUid: "uid-run", Spec: spec}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := client.Restore(ctx, &ateletpb.RestoreRequest{ActorUid: "uid-restore", Spec: spec}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Restore err = %v, want Unavailable", err)
	}
	srv.Stop()

	gotLog := logBuf.String()
	if strings.Contains(gotLog, secretVal) {
		t.Fatalf("request log contains %q from EnvVar.value:\n%s", secretVal, gotLog)
	}
	for _, method := range []string{ateletpb.Atelet_Run_FullMethodName, ateletpb.Atelet_Restore_FullMethodName} {
		var line string
		for l := range strings.Lines(gotLog) {
			if strings.Contains(l, `"method":"`+method+`"`) {
				line = l
			}
		}
		if line == "" {
			t.Fatalf("no request log line for %s:\n%s", method, gotLog)
		}
		// The env var name is kept while its value is masked.
		for _, want := range []string{`"value":"` + protoredact.Placeholder + `"`, `"name":"API_KEY"`} {
			if !strings.Contains(line, want) {
				t.Fatalf("%s request log missing %s:\n%s", method, want, line)
			}
		}
	}
}
