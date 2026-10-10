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

package golden

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestGoldenApplications exercises the real control plane and gVisor worker.
// A surviving sandbox or probed sidecar must not hide an exited application.
func TestGoldenApplications(t *testing.T) {
	if e2e.IsMicroVM() {
		t.Skip("application liveness at FULL checkpoint is a gVisor contract")
	}
	base, image := setup(t)
	for _, tc := range []struct {
		name    string
		code    int
		sidecar bool
	}{
		{name: "no-probe-exit-1", code: 1},
		{name: "no-probe-exit-0", code: 0},
		{name: "healthy-probed-sidecar", code: 1, sidecar: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			containers := []*ateapipb.Container{{
				Name: "app", Image: image,
				Command: []string{"/ko-app/golden", fmt.Sprintf("--exit-code=%d", tc.code)},
			}}
			if tc.sidecar {
				containers = append(containers, servingContainer(image, "sidecar", 8081))
			}
			tmpl := createTemplate(t, base, tc.name, containers)
			golden := waitForGolden(t, e2e.TemplateRef(tmpl))
			if golden.GetGoldenTag() != nil {
				t.Fatalf("exited application produced a golden tag: %v", golden.GetGoldenTag())
			}
			want := fmt.Sprintf(`application container "app" exited before checkpoint (exit code %d)`, tc.code)
			if msg := golden.GetErrorMessage(); !strings.Contains(msg, "GoldenActorCrashed") || !strings.Contains(msg, want) {
				t.Fatalf("golden error = %q, want checkpoint failure containing %q", msg, want)
			}
		})
	}
	t.Run("healthy-multicontainer-restore", func(t *testing.T) {
		tmpl := createTemplate(t, base, "healthy", []*ateapipb.Container{
			servingContainer(image, "app", 8080), servingContainer(image, "sidecar", 8081),
		})
		golden := waitForGolden(t, e2e.TemplateRef(tmpl))
		if golden.GetErrorMessage() != "" || golden.GetGoldenTag() == nil {
			t.Fatalf("healthy golden failed: %v", golden)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		api := e2e.GetClients().SubstrateAPI
		tag, err := api.GetTag(ctx, &ateapipb.GetTagRequest{Tag: golden.GetGoldenTag()})
		if err != nil {
			t.Fatal(err)
		}
		goldenURI := snapshotURI(tag.GetStatus().GetSnapshot())
		if goldenURI == "" {
			t.Fatal("published golden tag has no snapshot")
		}
		router, err := e2e.NewRouterClient(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer router.Close()
		bootIDs := map[int]string{}
		for _, name := range []string{"alpha", "beta"} {
			ref := &ateapipb.ObjectRef{Atespace: tmpl.GetMetadata().GetAtespace(), Name: name}
			actor, err := api.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name}, ActorTemplate: e2e.TemplateRef(tmpl),
			}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if _, err := api.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true}); err != nil {
					t.Errorf("cleanup actor %v: %v", ref, err)
				}
			})
			if got := durableSnapshotURI(actor.GetStatus()); got != goldenURI {
				t.Fatalf("actor %s snapshot = %q, want golden %q", name, got, goldenURI)
			}
			if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, e2e.GetClients(), &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
				t.Fatalf("restore actor %s from golden: %v", name, err)
			}
			for _, port := range []int{8080, 8081} {
				id := waitForBootID(t, ctx, router, resources.ActorRefFromObjectRef(ref), port)
				if name == "alpha" {
					bootIDs[port] = id
				} else if id != bootIDs[port] {
					t.Errorf("port %d boot ID = %q, want shared golden boot ID %q; container cold-booted", port, id, bootIDs[port])
				}
			}
		}
		if bootIDs[8080] == bootIDs[8081] {
			t.Fatal("both ports reached the same process; expected two restored containers")
		}
	})
}

func setup(t *testing.T) (*ateapipb.ActorTemplate, string) {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatal(err)
	}
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	image := strings.TrimSpace(string(e2e.RunCmdOutput(t, []string{"KO_CONFIG_PATH=" + root},
		filepath.Join(root, "hack/run-tool.sh"), "ko", "build", "--base-import-paths",
		"github.com/agent-substrate/substrate/internal/e2e/fixtures/golden")))
	clients := e2e.GetClients()
	ctx := t.Context()
	src := e2e.SubstrateCounterFixture()
	wp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(src.PoolNamespace).Get(ctx, src.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	srcTemplate, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Atespace: src.Atespace, Name: src.Name},
	})
	if err != nil {
		t.Fatal(err)
	}
	if srcTemplate.GetSandboxConfig().GetSandboxClass() != ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR {
		t.Fatal("golden regression requires a gVisor fixture")
	}
	ns := e2e.CreateNamespace(t).Name
	labels := map[string]string{"golden-regression": ns}
	poolSpec := wp.Spec.DeepCopy()
	poolSpec.Replicas = 2
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(ns).Create(ctx, &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "golden", Namespace: ns, Labels: labels},
		Spec:       *poolSpec,
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: ns}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := clients.SubstrateAPI.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: ns}}); err != nil {
			t.Errorf("cleanup atespace %s: %v", ns, err)
		}
	})
	return &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: ns},
		WorkerSelector: &ateapipb.Selector{MatchLabels: labels},
		SandboxConfig:  srcTemplate.GetSandboxConfig(),
		Resources:      srcTemplate.GetResources(),
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/golden-regression/" + ns + "/",
			PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	}, image
}

func servingContainer(image, name string, port int32) *ateapipb.Container {
	return &ateapipb.Container{
		Name: name, Image: image,
		Command: []string{"/ko-app/golden", fmt.Sprintf("--port=%d", port)},
		WakeupProbe: &ateapipb.ContainerWakeupProbe{
			HttpGet: &ateapipb.HTTPGetAction{Path: "/healthz", Port: port}, TimeoutSeconds: 60,
		},
	}
}

func createTemplate(t *testing.T, base *ateapipb.ActorTemplate, name string, containers []*ateapipb.Container) *ateapipb.ActorTemplate {
	t.Helper()
	tmpl := &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: base.GetMetadata().GetAtespace(), Name: name},
		WorkerSelector: base.WorkerSelector, SandboxConfig: base.SandboxConfig,
		Resources: base.Resources, SnapshotConfig: base.SnapshotConfig, Containers: containers,
	}
	created, err := e2e.GetClients().SubstrateAPI.CreateActorTemplate(t.Context(), &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := e2e.GetClients().SubstrateAPI.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: e2e.TemplateRef(created)}); err != nil {
			t.Errorf("cleanup template %s: %v", name, err)
		}
	})
	return created
}

func waitForGolden(t *testing.T, ref *ateapipb.ObjectRef) *ateapipb.GoldenSnapshotStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), e2e.TemplateReadyTimeout(t))
	defer cancel()
	var last *ateapipb.GoldenSnapshotStatus
	for {
		tmpl, err := e2e.GetClients().SubstrateAPI.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: ref})
		if err != nil {
			t.Fatalf("GetActorTemplate %v (last status %v): %v", ref, last, err)
		}
		last = tmpl.GetStatus().GetGoldenSnapshotStatus()
		if last.GetGoldenTag() != nil || last.GetErrorMessage() != "" {
			return last
		}
		select {
		case <-ctx.Done():
			t.Fatalf("golden %v did not finish: %v (last status %v)", ref, ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

func waitForBootID(t *testing.T, ctx context.Context, router *e2e.RouterClient, actor resources.ActorRef, port int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		id, err := readBootID(ctx, router, actor, port)
		if err == nil {
			return id
		}
		select {
		case <-ctx.Done():
			t.Fatalf("actor %s port %d never served its boot ID: %v", actor, port, err)
		case <-time.After(time.Second):
		}
	}
}

func readBootID(ctx context.Context, router *e2e.RouterClient, actor resources.ActorRef, port int) (string, error) {
	body, code, err := requestActor(ctx, router, actor, port, http.MethodGet, "/")
	if err != nil {
		return "", err
	}
	if code != http.StatusOK || strings.TrimSpace(string(body)) == "" {
		return "", fmt.Errorf("HTTP %d: %q", code, body)
	}
	return strings.TrimSpace(string(body)), nil
}

func durableSnapshotURI(status *ateapipb.ActorStatus) string {
	var best *ateapipb.Snapshot
	for _, snap := range status.GetSnapshots() {
		if snapshotURI(snap) != "" && (best == nil || snap.GetGeneration() > best.GetGeneration()) {
			best = snap
		}
	}
	return snapshotURI(best)
}

func snapshotURI(snap *ateapipb.Snapshot) string {
	for _, storage := range snap.GetStorage() {
		if storage.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE &&
			storage.GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
			return storage.GetObject().GetSnapshotUri()
		}
	}
	return ""
}
