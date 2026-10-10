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

// Package imagefs checks the actor's view of its image's filesystem against
// what the image's layers record.
package imagefs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// statResponse mirrors the probe's /stat payload.
type statResponse struct {
	Path  string `json:"path"`
	UID   uint32 `json:"uid"`
	GID   uint32 `json:"gid"`
	Mode  string `json:"mode"`
	Error string `json:"error"`
}

// layerOwners are paths whose owner and mode the probe image's layers record.
// The probe is built on the repo's default ko base image (.ko.yaml), a
// distroless static image whose base layer has /home/nonroot as drwx------
// owned by 65532:65532 (distroless's nonroot user) and root-owned system
// files.
var layerOwners = []statResponse{
	{Path: "/home/nonroot", UID: 65532, GID: 65532, Mode: "drwx------"},
	{Path: "/etc/passwd", UID: 0, GID: 0, Mode: "-rw-r--r--"},
	{Path: "/home", UID: 0, GID: 0, Mode: "drwxr-xr-x"},
}

// TestImageLayerOwnership asserts that files keep the owners the image's
// layer tars record, as they do under containerd: a private home directory
// given to a non-root user must not show up as root-owned inside the actor.
// It checks once on the first start and again after a suspend and resume,
// which restores the actor over the same cached layers.
//
// It runs against whichever sandbox class E2E_SANDBOX_CLASS selects, so CI
// covers both gvisor and micro-VM from one suite.
func TestImageLayerOwnership(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	atespace, tmpl := e2e.DeployProbe(t, env["BUCKET_NAME"], "imagefs")
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: "imagefs-actor"}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: tmpl.GetMetadata().GetName()},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", ref.Name, err)
	}
	t.Cleanup(func() {
		// DeleteActor requires the actor to be suspended.
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	})

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %q: %v", ref.Name, err)
	}
	t.Run("first start", func(t *testing.T) { checkOwners(t, ctx, rc, ref) })

	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatalf("SuspendActor %q: %v", ref.Name, err)
	}
	waitForState(t, ctx, clients, ref, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %q after suspend: %v", ref.Name, err)
	}
	t.Run("after suspend and resume", func(t *testing.T) { checkOwners(t, ctx, rc, ref) })
}

func checkOwners(t *testing.T, ctx context.Context, rc *e2e.RouterClient, ref *ateapipb.ObjectRef) {
	t.Helper()
	for _, want := range layerOwners {
		got := probeStat(t, ctx, rc, ref, want.Path)
		if got.Error != "" {
			t.Errorf("stat %s inside the actor: %s", want.Path, got.Error)
			continue
		}
		if got.UID != want.UID || got.GID != want.GID || got.Mode != want.Mode {
			t.Errorf("%s inside the actor is %s %d:%d, want %s %d:%d as the image's layer records it",
				want.Path, got.Mode, got.UID, got.GID, want.Mode, want.UID, want.GID)
		}
	}
}

func probeStat(t *testing.T, ctx context.Context, rc *e2e.RouterClient, ref *ateapipb.ObjectRef, path string) statResponse {
	t.Helper()
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: ref.Atespace, Name: ref.Name}, "/stat?"+url.Values{"path": {path}}.Encode())
	if err != nil {
		t.Fatalf("GET /stat %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /stat %s: status %d, body %q", path, resp.StatusCode, body)
	}
	var out statResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding /stat %s: %v", path, err)
	}
	return out
}

func waitForState(t *testing.T, ctx context.Context, clients *e2e.Clients, ref *ateapipb.ObjectRef, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach state %v", ref.Name, want)
}
