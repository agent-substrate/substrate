//go:build linux

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

package kata

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// startFakeAgent serves methods on grpc.AgentService and returns a client
// connected to it.
func startFakeAgent(t *testing.T, methods map[string]ttrpc.Method) *AgentClient {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv, err := ttrpc.NewServer()
	if err != nil {
		t.Fatalf("ttrpc.NewServer: %v", err)
	}
	srv.Register("grpc.AgentService", methods)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(context.Background(), l) }()
	t.Cleanup(func() {
		_ = srv.Close()
		if err := <-serveErr; err != nil && !errors.Is(err, ttrpc.ErrServerClosed) {
			t.Logf("fake kata-agent server: %v", err)
		}
	})

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ac := &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}
	t.Cleanup(func() { _ = ac.Close() })
	return ac
}

// The restore path fails when this call fails, so a wrong service or method name
// here would fail every micro-VM restore.
func TestReseedRandomDevSendsData(t *testing.T) {
	got := make(chan []byte, 1)
	ac := startFakeAgent(t, map[string]ttrpc.Method{
		"ReseedRandomDev": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.ReseedRandomDevRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			got <- req.GetData()
			return &emptypb.Empty{}, nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	want := []byte("per-restore nonce")
	if err := ac.ReseedRandomDev(ctx, want); err != nil {
		t.Fatalf("ReseedRandomDev: %v", err)
	}
	select {
	case data := <-got:
		if !bytes.Equal(data, want) {
			t.Errorf("agent got data %q, want %q", data, want)
		}
	default:
		t.Fatal("agent never received ReseedRandomDev")
	}
}

func TestReseedRandomDevReturnsAgentError(t *testing.T) {
	ac := startFakeAgent(t, map[string]ttrpc.Method{
		"ReseedRandomDev": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.ReseedRandomDevRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			return nil, errors.New("reseed refused")
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ac.ReseedRandomDev(ctx, []byte("nonce")); err == nil {
		t.Fatal("ReseedRandomDev returned nil for an agent error, want the error")
	}
}

// kata-agent's do_read_stream signals stdio pipe EOF after container exit by
// returning an empty ReadStreamResponse with a nil error. StreamReader.Read
// must translate that into io.EOF on the first empty response.
func TestStreamReaderEmptyDataReturnsEOF(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		stderr bool
	}{
		{name: "stdout", method: "ReadStdout", stderr: false},
		{name: "stderr", method: "ReadStderr", stderr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks := [][]byte{[]byte("line 1\n"), []byte("line 2\n")}
			var calls int
			ac := startFakeAgent(t, map[string]ttrpc.Method{
				tc.method: func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
					var req agentpb.ReadStreamRequest
					if err := unmarshal(&req); err != nil {
						return nil, err
					}
					if req.GetContainerId() != "c_main" || req.GetExecId() != "c_main" {
						t.Errorf("got container=%q exec=%q, want c_main/c_main", req.GetContainerId(), req.GetExecId())
					}
					i := calls
					calls++
					if i < len(chunks) {
						return &agentpb.ReadStreamResponse{Data: chunks[i]}, nil
					}
					if i == len(chunks) {
						return &agentpb.ReadStreamResponse{}, nil
					}
					t.Errorf("Read called again after the empty EOF response")
					return nil, errors.New("unexpected Read after EOF")
				},
			})

			r := NewStdioReader(t.Context(), ac, "c_main", "c_main", tc.stderr)
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("io.ReadAll: %v", err)
			}
			if want := "line 1\nline 2\n"; string(got) != want {
				t.Errorf("got %q, want %q", got, want)
			}
			wantCalls := len(chunks) + 1
			if calls != wantCalls {
				t.Errorf("RPC calls = %d, want %d (must stop on first empty response)", calls, wantCalls)
			}

			// A zero-length read buffer must not be treated as stream EOF or issue an RPC.
			n, err := r.Read(nil)
			if n != 0 || err != nil {
				t.Errorf("Read(nil) = (%d, %v), want (0, nil)", n, err)
			}
			if calls != wantCalls {
				t.Errorf("RPC calls after Read(nil) = %d, want %d (no extra RPC)", calls, wantCalls)
			}
		})
	}
}
