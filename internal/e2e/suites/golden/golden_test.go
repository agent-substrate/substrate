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

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/api/infra/k8s/infra.ate.dev/v1alpha1"
	"go.ate.dev/substrate/api/ateapi/v1"
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
			containers := []*ateapi.Container{{
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
		tmpl := createTemplate(t, base, "healthy", []*ateapi.Container{
			servingContainer(image, "app", 8080), servingContainer(image, "sidecar", 8081),
		})
		golden := waitForGolden(t, e2e.TemplateRef(tmpl))
		if golden.GetErrorMessage() != "" || golden.GetGoldenTag() == nil {
			t.Fatalf("healthy golden failed: %v", golden)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		api := e2e.GetClients().SubstrateAPI
		tag, err := api.GetTag(ctx, &ateapi.GetTagRequest{Tag: golden.GetGoldenTag()})
		if err != nil {
			t.Fatal(err)
		}
		goldenURI := tagSnapshotURI(tag)
		if goldenURI == "" {
			t.Fatal("published golden tag has no snapshot")
		}
		router, err := e2e.NewRouterClient(t, ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer router.Close()
		bootIDs := map[int]string{}
		for _, name := range []string{"alpha", "beta"} {
			ref := &ateapi.ObjectRef{Atespace: tmpl.GetMetadata().GetAtespace(), Name: name}
			actor, err := api.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name}, ActorTemplate: e2e.TemplateRef(tmpl),
			}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				if _, err := api.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
					t.Errorf("cleanup actor %v: %v", ref, err)
				}
			})
			if got := durableSnapshotURI(actor.GetStatus()); got != goldenURI {
				t.Fatalf("actor %s snapshot = %q, want golden %q", name, got, goldenURI)
			}
			if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, e2e.GetClients(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
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

func setup(t *testing.T) (*ateapi.ActorTemplate, string) {
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
		"go.ate.dev/substrate/internal/e2e/fixtures/golden")))
	clients := e2e.GetClients()
	ctx := t.Context()
	src := e2e.SubstrateCounterFixture()
	wp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(src.PoolNamespace).Get(ctx, src.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	srcTemplate, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
		ActorTemplate: &ateapi.ObjectRef{Atespace: src.Atespace, Name: src.Name},
	})
	if err != nil {
		t.Fatal(err)
	}
	if wp.Spec.DefaultSandboxClass() != v1alpha1.SandboxClassGvisor {
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
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{
		Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: ns}},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := clients.SubstrateAPI.DeleteAtespace(ctx, &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: ns}}); err != nil {
			t.Errorf("cleanup atespace %s: %v", ns, err)
		}
	})
	return &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: ns},
		WorkerSelector: &ateapi.Selector{MatchLabels: labels},
		Resources:      srcTemplate.GetResources(),
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/golden-regression/" + ns + "/",
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		},
	}, image
}

func servingContainer(image, name string, port int32) *ateapi.Container {
	return &ateapi.Container{
		Name: name, Image: image,
		Command: []string{"/ko-app/golden", fmt.Sprintf("--port=%d", port)},
		WakeupProbe: &ateapi.WakeupProbe{
			HttpGet: &ateapi.HttpGetAction{Path: "/healthz", Port: port}, TimeoutSeconds: 60,
		},
	}
}

func createTemplate(t *testing.T, base *ateapi.ActorTemplate, name string, containers []*ateapi.Container) *ateapi.ActorTemplate {
	t.Helper()
	tmpl := &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: base.GetMetadata().GetAtespace(), Name: name},
		WorkerSelector: base.WorkerSelector,
		Resources:      base.Resources, SnapshotConfig: base.SnapshotConfig, Containers: containers,
	}
	created, err := e2e.GetClients().SubstrateAPI.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: tmpl})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := e2e.GetClients().SubstrateAPI.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: e2e.TemplateRef(created)}); err != nil {
			t.Errorf("cleanup template %s: %v", name, err)
		}
	})
	return created
}

func waitForGolden(t *testing.T, ref *ateapi.ObjectRef) *ateapi.GoldenSnapshotStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), e2e.TemplateReadyTimeout(t))
	defer cancel()
	var last *ateapi.GoldenSnapshotStatus
	for {
		tmpl, err := e2e.GetClients().SubstrateAPI.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref})
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
