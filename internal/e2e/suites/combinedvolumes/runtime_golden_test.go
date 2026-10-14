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
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

type latencySample struct {
	Mode         string          `json:"mode"`
	Index        int             `json:"index"`
	Warmup       bool            `json:"warmup"`
	Milliseconds float64         `json:"milliseconds"`
	Success      bool            `json:"success"`
	Actor        string          `json:"actor"`
	BootID       string          `json:"boot_id,omitempty"`
	ProcessNonce string          `json:"process_nonce,omitempty"`
	Snapshots    json.RawMessage `json:"source_snapshots"`
}

// TestRuntimeGoldenVolumes is an explicit real-cluster experiment. It must fail
// without microVM/NFS, rather than count a skipped precondition as evidence.
func TestRuntimeGoldenVolumes(t *testing.T) {
	if os.Getenv("E2E_RUNTIME_GOLDEN") != "true" {
		t.Skip("opt in with E2E_RUNTIME_GOLDEN=true")
	}
	if os.Getenv("E2E_SANDBOX_CLASS") != "microvm" {
		t.Fatal("microvm required")
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
		dir := os.Getenv("E2E_RUNTIME_LOG_DIR")
		if dir == "" {
			t.Error("E2E_RUNTIME_LOG_DIR required to preserve runtime evidence")
			return
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Error(err)
			return
		}
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
	defer router.Close()
	create := func(t *testing.T, tmpl *ateapipb.ActorTemplate, name, vol string) *ateapipb.Actor {
		t.Helper()
		a, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}, ActorTemplate: e2e.TemplateRef(tmpl), SharedVolumeBindings: map[string]string{sharedVolume: vol}}})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	remove := func(a *ateapipb.Actor) {
		ref := resources.ActorRefFromActor(a).ToObjectRef()
		_, _ = clients.SubstrateAPI.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: ref})
	}
	resume := func(t *testing.T, a *ateapipb.Actor) {
		t.Helper()
		if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: resources.ActorRefFromActor(a).ToObjectRef()}); err != nil {
			t.Fatal(err)
		}
	}
	suspend := func(t *testing.T, a *ateapipb.Actor) *ateapipb.Actor {
		t.Helper()
		r, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: resources.ActorRefFromActor(a).ToObjectRef()})
		if err != nil {
			t.Fatal(err)
		}
		return r.GetActor()
	}
	post := func(t *testing.T, a *ateapipb.Actor, path string, body any) {
		t.Helper()
		b, _ := json.Marshal(body)
		resp, err := router.PostJSON(ctx, resources.ActorRefFromActor(a), path, b)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			t.Fatalf("%s status %d", path, resp.StatusCode)
		}
	}
	// Distinct actor-selected volumes must receive no placeholder/application
	// state. The same boot ID across clones proves a resumed kernel; distinct
	// process nonces prove applications start after the snapshot boundary.
	var goldenBoot, firstNonce string
	for i, v := range volumes {
		a := create(t, golden, fmt.Sprintf("check-%d-%s", i, ns.Name), v)
		func() {
			defer remove(a)
			if len(a.GetStatus().GetSnapshots()) == 0 {
				t.Fatal("runtime actor has no source snapshot")
			}
			resume(t, a)
			ref := resources.ActorRefFromActor(a)
			state := probeJSON(ctx, t, router, ref, "/runtime-state")
			if state["boot_id"] == "" || state["process_nonce"] == "" {
				t.Fatal("missing runtime identity")
			}
			if i == 0 {
				goldenBoot = state["boot_id"]
				firstNonce = state["process_nonce"]
			} else if state["boot_id"] != goldenBoot || state["process_nonce"] == firstNonce {
				t.Fatalf("not distinct processes from one resumed kernel: %v", state)
			}
			requireUnreadable(ctx, t, router, ref, sharedPath+"/actor-"+strconv.Itoa(1-i))
			requireWrite(ctx, t, router, ref, sharedPath+"/actor-"+strconv.Itoa(i))
			root := sharedPath + "/state"
			challenge := "challenge-" + v
			post(t, a, "/volume-fixture", map[string]string{"root": root, "value": v})
			post(t, a, "/volume-state?root="+url.QueryEscape(root)+"&challenge="+url.QueryEscape(challenge), nil)
			for generation := 1; generation <= 2; generation++ {
				a = suspend(t, a)
				resume(t, a)
				after := probeJSON(ctx, t, router, ref, "/runtime-state")
				if after["boot_id"] != goldenBoot || after["process_nonce"] != state["process_nonce"] {
					t.Fatal("own restore restarted application or guest")
				}
				resp, err := router.Get(ctx, ref, "/volume-state")
				if err != nil {
					t.Fatal(err)
				}
				var obs struct {
					Challenge      string                       `json:"challenge"`
					Cached         string                       `json:"cached"`
					ByPath         struct{ Data, Error string } `json:"by_path"`
					Held           struct{ Data, Error string }
					Unlinked       struct{ Data, Error string }
					Directory      []string
					DirectoryError string `json:"directory_error"`
				}
				err = json.NewDecoder(resp.Body).Decode(&obs)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("retained state actor=%s generation=%d: %+v", v, generation, obs)
				if obs.Challenge != challenge || obs.Cached != v || obs.ByPath.Data != v || obs.Held.Data != v || obs.DirectoryError != "" {
					t.Fatalf("retained-state mismatch: %+v", obs)
				}
				// Deleted-open-file recovery is observed explicitly; not silently treated
				// as a correctness pass for that unsupported ordinary-restore case.
				if obs.Unlinked.Error != "" {
					t.Logf("LIMITATION: deleted-open file restore: %s", obs.Unlinked.Error)
				} else if obs.Unlinked.Data != v {
					t.Fatal("incorrect deleted-open contents")
				}
			}
		}()
	}
	samples := 100
	if v := os.Getenv("E2E_LATENCY_SAMPLES"); v != "" {
		samples, err = strconv.Atoi(v)
		if err != nil || samples < 0 {
			t.Fatal("invalid sample count")
		}
	}
	// Zero runs the correctness cases without collecting latency.
	if samples == 0 {
		return
	}
	output := os.Getenv("E2E_LATENCY_OUTPUT")
	if output == "" {
		t.Fatal("E2E_LATENCY_OUTPUT required")
	}
	f, err := os.Create(output)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	rng := rand.New(rand.NewPCG(42, 91))
	for i := -5; i < samples; i++ {
		modes := []string{"cold", "runtime-golden", "own"}
		rng.Shuffle(len(modes), func(a, b int) { modes[a], modes[b] = modes[b], modes[a] })
		for _, mode := range modes {
			t.Run(fmt.Sprintf("%s/%d", mode, i), func(t *testing.T) {
				tmpl := golden
				if mode == "cold" {
					tmpl = cold
				}
				a := create(t, tmpl, fmt.Sprintf("sample-%s-%d-%s", mode, i+5, ns.Name), volumes[0])
				defer remove(a)
				var ownNonce string
				if mode == "own" {
					resume(t, a)
					ownNonce = probeJSON(ctx, t, router, resources.ActorRefFromActor(a), "/runtime-state")["process_nonce"]
					a = suspend(t, a)
				}
				snap, _ := json.Marshal(a.GetStatus().GetSnapshots())
				sample := latencySample{Mode: mode, Index: i, Warmup: i < 0, Actor: a.GetMetadata().GetName(), Snapshots: snap}
				start := time.Now()
				defer func() {
					sample.Milliseconds = float64(time.Since(start).Microseconds()) / 1000
					sample.Success = !t.Failed()
					if err := enc.Encode(sample); err != nil {
						t.Error(err)
					}
					f.Sync()
				}()
				if mode == "cold" && len(a.GetStatus().GetSnapshots()) != 0 {
					t.Fatal("cold sample has snapshot")
				}
				if mode != "cold" && len(a.GetStatus().GetSnapshots()) == 0 {
					t.Fatal("restore sample has no snapshot")
				}
				resume(t, a)
				ref := resources.ActorRefFromActor(a)
				state := probeJSON(ctx, t, router, ref, "/runtime-state")
				sample.BootID = state["boot_id"]
				sample.ProcessNonce = state["process_nonce"]
				if mode == "cold" && sample.BootID == goldenBoot {
					t.Fatal("cold sample restored golden")
				}
				if mode != "cold" && sample.BootID != goldenBoot {
					t.Fatal("restore sample cold-booted")
				}
				if mode == "own" && sample.ProcessNonce != ownNonce {
					t.Fatal("own restore restarted process")
				}
				path := sharedPath + "/" + sample.Actor
				requireWrite(ctx, t, router, ref, path)
				requireContent(ctx, t, router, ref, path, probeWrittenContent)
			})
		}
	}
}
