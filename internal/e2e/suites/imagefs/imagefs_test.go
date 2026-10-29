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

// Package imagefs checks the actor's view of its image's filesystem and
// process identity against what the image records.
package imagefs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
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
// .ko.yaml builds the probe on the root distroless static base, whose base
// layer has /home/nonroot as drwx------ owned by 65532:65532 (distroless's
// nonroot user), /var/tmp as root-owned 1777, and root-owned system files.
// /var/tmp, not /tmp, checks the sticky bit: gVisor mounts its own 1777 tmpfs
// over an empty /tmp.
var layerOwners = []statResponse{
	{Path: "/home/nonroot", UID: 65532, GID: 65532, Mode: "drwx------"},
	{Path: "/var/tmp", UID: 0, GID: 0, Mode: "dtrwxrwxrwx"},
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
// covers both gVisor and microVM from one suite.
func TestImageLayerOwnership(t *testing.T) {
	if _, err := e2e.CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	atespace, tmpl := e2e.DeployProbe(t, "imagefs")
	ref := &ateapi.ObjectRef{Atespace: atespace, Name: "imagefs-actor"}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: tmpl.GetMetadata().GetName()},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", ref.Name, err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref})
	})

	rc, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %q: %v", ref.Name, err)
	}
	t.Run("first start", func(t *testing.T) { checkOwners(t, ctx, rc, ref) })

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("SuspendActor %q: %v", ref.Name, err)
	}
	if got := suspended.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("SuspendActor %q: state %v, want %v", ref.Name, got, ateapi.ActorState_ACTOR_STATE_SUSPENDED)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %q after suspend: %v", ref.Name, err)
	}
	t.Run("after suspend and resume", func(t *testing.T) { checkOwners(t, ctx, rc, ref) })
}

// identityResponse mirrors the nonroot probe's /identity payload.
type identityResponse struct {
	UID        int    `json:"uid"`
	GID        int    `json:"gid"`
	Groups     []int  `json:"groups"`
	Cwd        string `json:"cwd"`
	CapEff     string `json:"capEff"`
	WriteError string `json:"writeError"`
	Error      string `json:"error"`
}

// The nonroot probe image's USER is nonroot:nonroot. The distroless base's
// /etc/passwd and /etc/group give that user and group id 65532 and list the
// user in no other group. Its WORKDIR, /home/nonroot, is 65532:65532 and
// drwx------.
const (
	nonrootID      = 65532
	nonrootWorkdir = "/home/nonroot"
)

// TestImageUserAndWorkdir asserts that the actor's process runs as its image's
// USER, resolved by name against the image's own /etc/passwd and /etc/group,
// with no effective capabilities, and starts in the image's WORKDIR, where
// that user can create files. The WORKDIR is private to the user, so the
// write also needs the layer owners TestImageLayerOwnership checks.
//
// The golden bake runs the probe as that user and binds :80, and the actor
// resumes from that golden snapshot. It runs against whichever sandbox class
// E2E_SANDBOX_CLASS selects, so CI covers both gVisor and microVM.
func TestImageUserAndWorkdir(t *testing.T) {
	if _, err := e2e.CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	atespace, templates := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/probe/probe.yaml.tmpl",
		Template: "internal/e2e/fixtures/probe/probe-nonroot-template.yaml.tmpl",
	}, "imagefs-user", nil, e2e.WithImageUser("nonroot:nonroot"))
	ref := &ateapi.ObjectRef{Atespace: atespace, Name: "imagefs-user-actor"}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: atespace, Name: templates[0].GetMetadata().GetName()},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", ref.Name, err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref})
	})

	rc, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %q: %v", ref.Name, err)
	}

	got := probeIdentity(t, ctx, rc, ref)
	if got.Error != "" {
		t.Errorf("probe reported an error reading its identity: %s", got.Error)
	}
	if got.UID != nonrootID || got.GID != nonrootID {
		t.Errorf("process runs as %d:%d, want %d:%d from the image's USER nonroot:nonroot", got.UID, got.GID, nonrootID, nonrootID)
	}
	// Linux may list the primary group among the supplementary ones. Any
	// other group, such as root's 0, was not granted by the image.
	for _, g := range got.Groups {
		if g != nonrootID {
			t.Errorf("supplementary groups = %v, want none besides %d", got.Groups, nonrootID)
			break
		}
	}
	// Without ambient capabilities, a non-root process has none effective.
	if capEff, err := strconv.ParseUint(got.CapEff, 16, 64); err != nil || capEff != 0 {
		t.Errorf("CapEff = %q, want no effective capabilities for a non-root USER", got.CapEff)
	}
	if got.Cwd != nonrootWorkdir {
		t.Errorf("working directory = %q, want the image's WORKDIR %q", got.Cwd, nonrootWorkdir)
	}
	if got.WriteError != "" {
		t.Errorf("creating a file in the WORKDIR as the image's USER: %s", got.WriteError)
	}
}

func probeIdentity(t *testing.T, ctx context.Context, rc *e2e.RouterClient, ref *ateapi.ObjectRef) identityResponse {
	t.Helper()
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: ref.Atespace, Name: ref.Name}, "/identity")
	if err != nil {
		t.Fatalf("GET /identity: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /identity: status %d, body %q", resp.StatusCode, body)
	}
	var out identityResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding /identity: %v", err)
	}
	return out
}

func checkOwners(t *testing.T, ctx context.Context, rc *e2e.RouterClient, ref *ateapi.ObjectRef) {
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

func probeStat(t *testing.T, ctx context.Context, rc *e2e.RouterClient, ref *ateapi.ObjectRef, path string) statResponse {
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
