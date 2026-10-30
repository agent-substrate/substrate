//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// startFakeAgent serves grpc.AgentService/ReseedRandomDev with handle and returns
// a client connected to it.
func startFakeAgent(t *testing.T, handle func(*agentpb.ReseedRandomDevRequest) error) *AgentClient {
	t.Helper()
	return serveFakeAgent(t, map[string]ttrpc.Method{
		"ReseedRandomDev": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.ReseedRandomDevRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			if err := handle(&req); err != nil {
				return nil, err
			}
			return &emptypb.Empty{}, nil
		},
	})
}

// serveFakeAgent serves methods as grpc.AgentService and returns a client
// connected to it.
func serveFakeAgent(t *testing.T, methods map[string]ttrpc.Method) *AgentClient {
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
	go func() { _ = srv.Serve(context.Background(), l) }()
	t.Cleanup(func() { _ = srv.Close() })

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ac := &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}
	t.Cleanup(func() { _ = ac.Close() })
	return ac
}

// unary serves one grpc.AgentService method with handle.
func unary[Req, Resp any](handle func(*Req) (Resp, error)) ttrpc.Method {
	return func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
		var req Req
		if err := unmarshal(&req); err != nil {
			return nil, err
		}
		return handle(&req)
	}
}

// The restore path fails when this call fails, so a wrong service or method name
// here would fail every micro-VM restore.
func TestReseedRandomDevSendsData(t *testing.T) {
	got := make(chan []byte, 1)
	ac := startFakeAgent(t, func(req *agentpb.ReseedRandomDevRequest) error {
		got <- req.GetData()
		return nil
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
	ac := startFakeAgent(t, func(*agentpb.ReseedRandomDevRequest) error {
		return errors.New("reseed refused")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ac.ReseedRandomDev(ctx, []byte("nonce")); err == nil {
		t.Fatal("ReseedRandomDev returned nil for an agent error, want the error")
	}
}

func TestStartRootfsContainerClosesStdin(t *testing.T) {
	tests := []struct {
		name          string
		closeStdinErr error
		wantErr       bool
		wantCalls     []string
	}{{
		name:      "closes stdin before starting",
		wantCalls: []string{"CreateContainer", "CloseStdin", "StartContainer"},
	}, {
		name:          "does not start when CloseStdin fails",
		closeStdinErr: errors.New("close refused"),
		wantErr:       true,
		wantCalls:     []string{"CreateContainer", "CloseStdin"},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu    sync.Mutex
				calls []string
			)
			// record decodes the request into req, records the call, and
			// fails it with reply if reply is not nil.
			record := func(name string, req any, reply error) ttrpc.Method {
				return func(_ context.Context, unmarshal func(any) error) (any, error) {
					mu.Lock()
					defer mu.Unlock()
					if err := unmarshal(req); err != nil {
						return nil, err
					}
					calls = append(calls, name)
					if reply != nil {
						return nil, reply
					}
					return &emptypb.Empty{}, nil
				}
			}
			closeReq := &agentpb.CloseStdinRequest{}
			ac := serveFakeAgent(t, map[string]ttrpc.Method{
				"CreateContainer": record("CreateContainer", &agentpb.CreateContainerRequest{}, nil),
				"CloseStdin":      record("CloseStdin", closeReq, tt.closeStdinErr),
				"StartContainer":  record("StartContainer", &agentpb.StartContainerRequest{}, nil),
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := ac.StartRootfsContainer(ctx, "c1", &specs.Spec{})
			if (err != nil) != tt.wantErr {
				t.Fatalf("StartRootfsContainer() = %v, want error: %t", err, tt.wantErr)
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(calls, tt.wantCalls) {
				t.Errorf("agent calls = %v, want %v", calls, tt.wantCalls)
			}
			if closeReq.GetContainerId() != "c1" || closeReq.GetExecId() != "c1" {
				t.Errorf("CloseStdin got container %q exec %q, want both %q", closeReq.GetContainerId(), closeReq.GetExecId(), "c1")
			}
		})
	}
}

// Empty data from the agent is the end of the stream; so is an agent error.
func TestStreamReaderEndsOnEmptyDataAndOnError(t *testing.T) {
	reads := 0
	ac := serveFakeAgent(t, map[string]ttrpc.Method{"ReadStdout": unary(func(req *agentpb.ReadStreamRequest) (*agentpb.ReadStreamResponse, error) {
		reads++
		switch {
		case req.GetContainerId() == "gone":
			return nil, errors.New("cannot get stream reader")
		case reads == 1:
			return &agentpb.ReadStreamResponse{Data: []byte("line\n")}, nil
		default:
			return &agentpb.ReadStreamResponse{}, nil
		}
	})})
	buf := make([]byte, 64)
	r := NewStdioReader(t.Context(), ac, "c1", "c1", false)
	if n, err := r.Read(buf); err != nil || string(buf[:n]) != "line\n" {
		t.Fatalf("first Read = %q, %v; want the line", buf[:n], err)
	}
	if n, err := r.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("Read after the stream ended = %d, %v; want 0, io.EOF", n, err)
	}
	if reads != 2 {
		t.Errorf("reads = %d, want 2: the reader must not ask again after an empty answer", reads)
	}
	if n, err := NewStdioReader(t.Context(), ac, "gone", "gone", false).Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("Read with an agent error = %d, %v; want 0, io.EOF", n, err)
	}
}
