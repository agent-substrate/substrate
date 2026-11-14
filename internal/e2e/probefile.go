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

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"go.ate.dev/substrate/internal/resources"
)

// ProbeWrittenContent is the fixed string the probe's /writefile writes.
const ProbeWrittenContent = "written by probe"

// ProbeJSON calls a probe endpoint on an actor through router and decodes its
// reply.
func ProbeJSON(ctx context.Context, t *testing.T, router *RouterClient, actorRef resources.ActorRef, path string) map[string]string {
	t.Helper()

	resp, err := router.Get(ctx, actorRef, path)
	if err != nil {
		t.Fatalf("GET %s on %s: %v", path, actorRef.Name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s on %s: status %d: %s", path, actorRef.Name, resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s on %s: %v", path, actorRef.Name, err)
	}
	return out
}

// RequireProbeContent fails unless path in the actor holds want.
func RequireProbeContent(ctx context.Context, t *testing.T, router *RouterClient, actorRef resources.ActorRef, path, want string) {
	t.Helper()

	got := ProbeJSON(ctx, t, router, actorRef, "/readfile?path="+path)
	if got["error"] != "" {
		t.Fatalf("reading %s on %s: %s", path, actorRef.Name, got["error"])
	}
	if got["content"] != want {
		t.Errorf("content of %s on %s = %q, want %q", path, actorRef.Name, got["content"], want)
	}
}

// RequireProbeUnreadable fails if path in the actor can be read.
func RequireProbeUnreadable(ctx context.Context, t *testing.T, router *RouterClient, actorRef resources.ActorRef, path string) {
	t.Helper()

	if got := ProbeJSON(ctx, t, router, actorRef, "/readfile?path="+path); got["error"] == "" {
		t.Errorf("%s on %s is readable (%q), want it absent or hidden", path, actorRef.Name, got["content"])
	}
}

// RequireProbeWrite writes ProbeWrittenContent to path in the actor and fails
// if the write is rejected.
func RequireProbeWrite(ctx context.Context, t *testing.T, router *RouterClient, actorRef resources.ActorRef, path string) {
	t.Helper()

	if got := ProbeJSON(ctx, t, router, actorRef, "/writefile?path="+path); got["error"] != "" {
		t.Fatalf("writing %s on %s: %s", path, actorRef.Name, got["error"])
	}
}

// RequireProbeWriteRejected fails if path in the actor can be written.
func RequireProbeWriteRejected(ctx context.Context, t *testing.T, router *RouterClient, actorRef resources.ActorRef, path string) {
	t.Helper()

	if got := ProbeJSON(ctx, t, router, actorRef, "/writefile?path="+path); got["error"] == "" {
		t.Errorf("write to %s on %s succeeded, want it rejected as read-only", path, actorRef.Name)
	}
}
