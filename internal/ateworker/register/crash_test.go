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
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

// fakeConn answers ReportActorCrashed with the queued errors, then OK.
type fakeConn struct {
	grpc.ClientConnInterface
	errs []error
	got  []*ateletpb.ReportActorCrashedRequest
}

func (c *fakeConn) Invoke(_ context.Context, _ string, args, _ any, _ ...grpc.CallOption) error {
	c.got = append(c.got, args.(*ateletpb.ReportActorCrashedRequest))
	if len(c.errs) == 0 {
		return nil
	}
	err := c.errs[0]
	c.errs = c.errs[1:]
	return err
}

var testActor = resources.ActorAttribution{
	Ref: resources.ActorRef{Atespace: "team-a", Name: "actor-1"},
	UID: "01234567-89ab-cdef-0123-456789abcdef",
}

func TestReportCrashed(t *testing.T) {
	tests := []struct {
		name      string
		errs      []error
		wantErr   bool
		wantCalls int
	}{
		{name: "accepted", wantCalls: 1},
		{name: "retries until delivered", errs: []error{status.Error(codes.Unavailable, ""), status.Error(codes.Aborted, "")}, wantCalls: 3},
		{name: "refusal is an answer", errs: []error{status.Error(codes.FailedPrecondition, "")}, wantCalls: 1},
		{name: "bad request is not retried", errs: []error{status.Error(codes.InvalidArgument, "")}, wantErr: true, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := &fakeConn{errs: tc.errs}
			err := reportCrashedWithRetry(context.Background(), conn, testActor, "cloud-hypervisor exited", time.Millisecond)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if len(conn.got) != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", len(conn.got), tc.wantCalls)
			}
			req := conn.got[0]
			if req.GetActorAtespace() != "team-a" || req.GetActorName() != "actor-1" || req.GetActorUid() != testActor.UID || req.GetMessage() != "cloud-hypervisor exited" {
				t.Errorf("request = %v", req)
			}
		})
	}
}

func TestReportCrashedGivesUpWithContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	conn := &fakeConn{errs: make([]error, 1000)}
	for i := range conn.errs {
		conn.errs[i] = status.Error(codes.Unavailable, "")
	}
	if err := reportCrashedWithRetry(ctx, conn, testActor, "x", time.Millisecond); err == nil {
		t.Fatal("err = nil, want the context error")
	}
}

func TestTruncateMessage(t *testing.T) {
	long := strings.Repeat("é", maxCrashMessageBytes)
	got := truncateMessage(long)
	if len(got) > maxCrashMessageBytes {
		t.Errorf("len = %d, want <= %d", len(got), maxCrashMessageBytes)
	}
	if !strings.HasPrefix(long, got) || strings.ContainsRune(got, '�') {
		t.Errorf("truncation split a rune")
	}
}
