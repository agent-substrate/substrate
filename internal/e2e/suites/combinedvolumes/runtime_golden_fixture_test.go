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

package combinedvolumes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

type runtimeGoldenFixture struct {
	golden, cold *ateapipb.ActorTemplate
	volumes      []string
	name         string
	router       *e2e.RouterClient
	ctx          context.Context
	clients      *e2e.Clients
}

func newRuntimeGoldenFixture(t *testing.T) *runtimeGoldenFixture {
	t.Helper()
	logDir := os.Getenv("E2E_RUNTIME_LOG_DIR")
	if logDir == "" {
		t.Fatal("E2E_RUNTIME_LOG_DIR required to preserve runtime evidence")
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()
	if storageClassOrEmpty(ctx, t, clients) == "" {
		t.Fatal("real NFS CSI required")
	}
	ns := e2e.CreateNamespace(t)
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatal(err)
	}
	probeAtespace, _ := e2e.DeployProbe(t, env["BUCKET_NAME"], "runtimegolden")
	src := e2e.SubstrateFixture{Atespace: probeAtespace, Name: probeName, PoolNamespace: probeAtespace, PoolName: probeName}
	golden := e2e.CreateSubstrateTemplateFrom(ctx, t, clients, ns.Name, src, e2e.SubstrateTemplateOptions{
		Atespace: atespace, Name: "runtime-" + ns.Name, PoolName: probeName, PoolReplicas: 2,
		Labels:         map[string]string{"runtimegolden": ns.Name},
		SnapshotConfig: &ateapipb.SnapshotConfig{StorageLocation: fmt.Sprintf("gs://%s/%s/", env["BUCKET_NAME"], ns.Name), GoldenSnapshotConfig: &ateapipb.GoldenSnapshotConfig{Mode: ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_RUNTIME}},
		Modify: func(x *ateapipb.ActorTemplate) {
			x.Containers[0].VolumeMounts = append(x.Containers[0].VolumeMounts, &ateapipb.VolumeMount{Name: sharedVolume, MountPath: sharedPath})
			x.Volumes = append(x.Volumes, &ateapipb.Volume{Name: sharedVolume, External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{}}})
		},
	})
	cold := proto.Clone(golden).(*ateapipb.ActorTemplate)
	cold.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: "cold-" + ns.Name}
	cold.Status = nil
	cold.SnapshotConfig.GoldenSnapshotConfig.Mode = ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED
	cold, err = clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: cold})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActorTemplate(context.Background(), &ateapipb.DeleteActorTemplateRequest{ActorTemplate: e2e.TemplateRef(cold)})
	})
	volumes := []string{"a-" + ns.Name, "b-" + ns.Name}
	for _, v := range volumes {
		_, err := clients.SubstrateAPI.CreateSharedVolume(ctx, &ateapipb.CreateSharedVolumeRequest{SharedVolume: &ateapipb.SharedVolume{Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: v}, StorageClassName: e2e.StorageClass, Capacity: extCapacity, DeleteTrigger: ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, err := clients.SubstrateAPI.DeleteSharedVolume(context.Background(), &ateapipb.DeleteSharedVolumeRequest{SharedVolume: &ateapipb.ObjectRef{Atespace: atespace, Name: v}})
			if err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(func() {
		dir := logDir
		kctx := os.Getenv("KUBECTL_CONTEXT")
		pods, err := exec.Command("kubectl", "--context="+kctx, "-n", ns.Name, "get", "pods", "-o", "name").Output()
		if err != nil {
			t.Error(err)
			return
		}
		for _, pod := range strings.Fields(string(pods)) {
			logs, err := exec.Command("kubectl", "--context="+kctx, "-n", ns.Name, "logs", pod, "--all-containers=true", "--prefix=true", "--tail=-1").CombinedOutput()
			if err != nil {
				t.Errorf("collect %s: %v", pod, err)
			}
			if err := os.WriteFile(filepath.Join(dir, ns.Name+"-"+strings.TrimPrefix(pod, "pod/")+".log"), logs, 0644); err != nil {
				t.Error(err)
			}
		}
	})
	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(router.Close)
	return &runtimeGoldenFixture{golden: golden, cold: cold, volumes: volumes, name: ns.Name, router: router, ctx: ctx, clients: clients}
}

func (f *runtimeGoldenFixture) create(t *testing.T, tmpl *ateapipb.ActorTemplate, name, vol string) *ateapipb.Actor {
	t.Helper()
	a, err := f.clients.SubstrateAPI.CreateActor(f.ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}, ActorTemplate: e2e.TemplateRef(tmpl), SharedVolumeBindings: map[string]string{sharedVolume: vol}}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func (f *runtimeGoldenFixture) remove(t *testing.T, a *ateapipb.Actor) {
	ref := resources.ActorRefFromActor(a).ToObjectRef()
	_, _ = f.clients.SubstrateAPI.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: ref})
	if _, err := f.clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
		t.Errorf("delete actor: %v", err)
	}
}
func (f *runtimeGoldenFixture) resume(t *testing.T, a *ateapipb.Actor) {
	t.Helper()
	if _, err := e2e.ResumeActorAwaitCapacity(t, f.ctx, f.clients, &ateapipb.ResumeActorRequest{Actor: resources.ActorRefFromActor(a).ToObjectRef()}); err != nil {
		t.Fatal(err)
	}
}
func (f *runtimeGoldenFixture) suspend(t *testing.T, a *ateapipb.Actor) *ateapipb.Actor {
	t.Helper()
	r, err := f.clients.SubstrateAPI.SuspendActor(f.ctx, &ateapipb.SuspendActorRequest{Actor: resources.ActorRefFromActor(a).ToObjectRef()})
	if err != nil {
		t.Fatal(err)
	}
	return r.GetActor()
}
func (f *runtimeGoldenFixture) post(t *testing.T, a *ateapipb.Actor, path string, body any) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := f.router.PostJSON(f.ctx, resources.ActorRefFromActor(a), path, b)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("%s status %d", path, resp.StatusCode)
	}
}

func runtimeStatePath() string {
	return "/runtime-state?runtime=" + os.Getenv("E2E_SANDBOX_CLASS")
}

// gVisor regenerates its procfs boot ID on restore. Its runtime-only boundary
// is checked with a retained pause-process nonce in the direct runsc test.
func usesGuestBootMarker() bool { return os.Getenv("E2E_SANDBOX_CLASS") == "microvm" }
