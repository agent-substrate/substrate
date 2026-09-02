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

package demo

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestUpdateTemplateSandboxConfig covers the SandboxConfig side of repointing
// a suspended actor at a different ActorTemplate: the actor first runs under
// template A (whose pool resolves the fixture's SandboxConfig), suspends —
// recording that config on its durable snapshot — and is then repointed at
// template B, whose pool names a freshly created SandboxConfig (a copy of the
// fixture's under a new name, so a new UID). The resume after the repoint
// must boot with the config recorded on template B's golden snapshot, not the
// one on the actor's own snapshot — observable in the snapshot the next
// suspend produces, which records the config the checkpoint reported.
func TestUpdateTemplateSandboxConfig(t *testing.T) {
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	// Create a new SandboxConfig: a spec-identical copy of the one the
	// fixture pool resolves to, under a fresh name. Same binaries, so the
	// sandbox boots the same either way; only the reference identity
	// (name/UID) differs, which is exactly what the test asserts on.
	//
	baseCfg := fixturePoolSandboxConfig(ctx, t, clients)
	newCfg := &v1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "update-sbcfg-" + nsObj.Name},
		Spec:       *baseCfg.Spec.DeepCopy(),
	}
	// Never the class default: only pool B's explicit reference may pick it.
	newCfg.Spec.Default = false
	newCfg, err = clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Create(ctx, newCfg, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create SandboxConfig copy: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Delete(cleanupCtx, newCfg.Name, metav1.DeleteOptions{}); err != nil {
			t.Logf("failed to delete SandboxConfig %q: %v", newCfg.Name, err)
		}
	})

	//
	// Template A rides the fixture's SandboxConfig; template B's pool names
	// the new one. The pools get disjoint labels so each template's golden
	// snapshot builds on its own pool — B's golden must record the new
	// config, and a golden actor landing on pool A would record the old one.
	//
	nameA, nameB := "sbcfg-a-"+nsObj.Name, "sbcfg-b-"+nsObj.Name
	snapshotsConfig := func(name string) *ateapipb.SnapshotsConfig {
		return &ateapipb.SnapshotsConfig{
			StorageLocation: "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + name,
			OnPause:         ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
		}
	}
	e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace:        demoAtespace,
		Name:            nameA,
		PoolName:        "sbcfg-a",
		PoolReplicas:    2,
		Labels:          map[string]string{"demo": nsObj.Name + "-a"},
		SnapshotsConfig: snapshotsConfig(nameA),
	})
	createdB := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace:              demoAtespace,
		Name:                  nameB,
		PoolName:              "sbcfg-b",
		PoolReplicas:          2,
		Labels:                map[string]string{"demo": nsObj.Name + "-b"},
		SnapshotsConfig:       snapshotsConfig(nameB),
		PoolSandboxConfigName: newCfg.Name,
		Modify: func(tmpl *ateapipb.ActorTemplate) {
			tmpl.SandboxConfig.ConfigName = newCfg.Name
		},
	})

	// Template B's golden snapshot must have recorded the new config: the
	// repointed resume below resolves its sandbox from this reference.
	refB := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: nameB}
	tmplB, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: refB})
	if err != nil {
		t.Fatalf("failed to get ActorTemplate %q: %v", nameB, err)
	}
	goldenB, err := clients.SubstrateAPI.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{
		ActorSnapshot: tmplB.GetStatus().GetGoldenSnapshotStatus().GetGoldenSnapshot(),
	})
	if err != nil {
		t.Fatalf("failed to get template B's golden snapshot: %v", err)
	}
	if got := goldenB.GetStatus().GetSandboxConfigRef(); got.GetUid() != string(newCfg.UID) {
		t.Fatalf("template B's golden snapshot records SandboxConfig %q (uid %q), want the new config %q (uid %q)",
			got.GetName(), got.GetUid(), newCfg.Name, string(newCfg.UID))
	}

	//
	// Create an Actor from template A and run it once.
	//
	actorID := "update-sbcfg-" + nsObj.Name
	actorRef := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID}
	t.Logf("Creating Actor %q from substrate template %q...", actorID, nameA)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: nameA},
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: actorRef})
	}()

	t.Logf("Resuming Actor %q under template A...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	// The first resume also absorbs the freshly created pool's worker
	// startup, so it gets a longer budget than the steady-state waits.
	waitForActorStateWithTimeout(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING, 120*time.Second)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor under template A: %v", err)
	}
	validateCounterResponse(t, resp, "under template A", 1, 1)

	//
	// Suspend: the durable snapshot must record the config the sprint booted
	// with — the old one. This is the reference a plain (non-repointed)
	// resume would boot from.
	//
	t.Logf("Suspending Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	suspended, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get suspended Actor: %v", err)
	}
	snapshotA, err := clients.SubstrateAPI.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{
		ActorSnapshot: suspended.GetStatus().GetLatestSnapshot(),
	})
	if err != nil {
		t.Fatalf("failed to get the suspended Actor's snapshot: %v", err)
	}
	oldRef := snapshotA.GetStatus().GetSandboxConfigRef()
	if oldRef.GetUid() == "" {
		t.Fatal("suspended Actor's snapshot records no sandbox_config_ref")
	}
	if oldRef.GetUid() == string(newCfg.UID) {
		t.Fatalf("Actor booted under template A already ran the new SandboxConfig %q; the test cannot distinguish old from new", newCfg.Name)
	}

	//
	// Repoint at template B and resume: the boot must resolve the new
	// SandboxConfig from B's golden snapshot, not the old one recorded on
	// the actor's own snapshot.
	//
	t.Logf("Repointing Actor %q at template %q...", actorID, nameB)
	if _, err := clients.SubstrateAPI.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      suspended.GetMetadata(),
		ActorTemplate: refB,
	}}); err != nil {
		t.Fatalf("failed to update Actor's template: %v", err)
	}

	t.Logf("Resuming Actor %q under template B...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after template update: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	repointed, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get repointed Actor: %v", err)
	}
	if got, want := repointed.GetStatus().GetCurrentActorTemplateUid(), createdB.GetMetadata().GetUid(); got != want {
		t.Errorf("repointed Actor current_actor_template_uid = %q, want template B's %q", got, want)
	}

	// The repoint forces a data-only restore: the guest cold-boots from B
	// (memory counter resets) while the durable dir carries over (file
	// counter continues).
	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor after template update: %v", err)
	}
	validateCounterResponse(t, resp, "after template update", 1, 2)

	// A second suspend closes the loop: the snapshot chain now carries the
	// new config, so later resumes keep booting with it.
	t.Logf("Suspending Actor %q again...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to suspend Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	suspended, err = clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get re-suspended Actor: %v", err)
	}
	snapshotB, err := clients.SubstrateAPI.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{
		ActorSnapshot: suspended.GetStatus().GetLatestSnapshot(),
	})
	if err != nil {
		t.Fatalf("failed to get the re-suspended Actor's snapshot: %v", err)
	}
	if got := snapshotB.GetStatus().GetSandboxConfigRef(); got.GetUid() != string(newCfg.UID) {
		t.Errorf("re-suspended Actor's snapshot records SandboxConfig %q (uid %q), want the new config %q (uid %q)",
			got.GetName(), got.GetUid(), newCfg.Name, string(newCfg.UID))
	}
}

// fixturePoolSandboxConfig resolves the SandboxConfig the counter fixture's
// WorkerPool boots with, mirroring the control plane's resolution: the pool's
// explicit SandboxConfigName when set, else the cluster-wide default for the
// pool's SandboxClass.
func fixturePoolSandboxConfig(ctx context.Context, t *testing.T, clients *e2e.Clients) *v1alpha1.SandboxConfig {
	t.Helper()
	fixture := e2e.SubstrateCounterFixture()
	wp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(fixture.PoolNamespace).Get(ctx, fixture.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s (deploy with: %s): %v", fixture.PoolNamespace, fixture.PoolName, fixture.DeployWith, err)
	}
	if name := wp.Spec.SandboxConfigName; name != "" {
		sc, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get SandboxConfig %q named by WorkerPool %s/%s: %v", name, fixture.PoolNamespace, fixture.PoolName, err)
		}
		return sc
	}
	class := wp.Spec.SandboxClass
	if class == "" {
		class = v1alpha1.SandboxClassGvisor
	}
	list, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list SandboxConfigs: %v", err)
	}
	for i := range list.Items {
		if sc := &list.Items[i]; sc.Spec.SandboxClass == class && sc.Spec.Default {
			return sc
		}
	}
	t.Fatalf("no default SandboxConfig for class %q", class)
	return nil
}
