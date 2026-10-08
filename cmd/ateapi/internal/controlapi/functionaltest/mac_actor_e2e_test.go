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

package functionaltest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

func macActorE2ERequired() bool { return os.Getenv("ATE_MAC_ACTOR_E2E_REQUIRED") == "true" }

func requireMacActorE2EConfig(t *testing.T) (endpoint, image string) {
	t.Helper()
	endpoint = os.Getenv("ATE_MAC_ACTOR_E2E_ENDPOINT")
	image = os.Getenv("ATE_MAC_ACTOR_E2E_IMAGE")
	missing := endpoint == "" || image == "" || os.Getenv("ATE_MAC_ACTOR_E2E_CLIENT_CERT") == "" || os.Getenv("ATE_MAC_ACTOR_E2E_CLIENT_KEY") == "" || os.Getenv("ATE_MAC_ACTOR_E2E_SERVER_CA") == ""
	if missing && macActorE2ERequired() {
		t.Fatal("Mac Actor E2E is required but endpoint, image, or mTLS files are not fully configured")
	}
	if missing {
		t.Skip("set ATE_MAC_ACTOR_E2E_ENDPOINT, ATE_MAC_ACTOR_E2E_IMAGE, and the three ATE_MAC_ACTOR_E2E_* TLS paths to run")
	}
	return endpoint, image
}

// TestMacActorE2E exercises the public Substrate API through placement,
// authenticated host dispatch, real guest readiness, routing metadata, and
// termination. The HostRuntime endpoint is an external Apple Silicon Mac.
func TestMacActorE2E(t *testing.T) {
	endpoint, image := requireMacActorE2EConfig(t)
	runtime, err := controlapi.NewGRPCHostRuntime(
		os.Getenv("ATE_MAC_ACTOR_E2E_CLIENT_CERT"),
		os.Getenv("ATE_MAC_ACTOR_E2E_CLIENT_KEY"),
		os.Getenv("ATE_MAC_ACTOR_E2E_SERVER_CA"),
	)
	if err != nil {
		t.Fatalf("configure HostRuntime client: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	ns := namespaceForTest("ns-mac-actor-e2e")
	tc := setupTestWithHostRuntime(t, ns, runtime)
	defer tc.cleanup()

	const sandboxConfigName = "macos-vz-e2e"
	_, err = tc.substrateClient.ApiV1alpha1().SandboxConfigs().Create(context.Background(), &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxConfigName},
		Spec:       atev1alpha1.SandboxConfigSpec{SandboxClass: atev1alpha1.SandboxClassMacOS},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create Mac SandboxConfig: %v", err)
	}
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		_, err := tc.sandboxConfigLister.Get(sandboxConfigName)
		return err == nil, nil
	}); err != nil {
		t.Fatalf("Mac SandboxConfig did not reach the lister: %v", err)
	}

	const workerName = "mac-worker-e2e"
	if _, err := tc.client.CreateWorker(context.Background(), &ateapipb.CreateWorkerRequest{Worker: &ateapipb.Worker{
		Metadata:     &ateapipb.ResourceMetadata{Name: workerName},
		SandboxClass: string(atev1alpha1.SandboxClassMacOS),
		ExternalHost: &ateapipb.ExternalWorkerHost{RuntimeEndpoint: endpoint, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}}); err != nil {
		t.Fatalf("register Mac Worker: %v", err)
	}

	const templateName = "mac-template-e2e"
	if _, err := tc.client.CreateActorTemplate(context.Background(), &ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: templateName},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			OnCommit:        ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DISK,
			StorageLocation: testStorageLocation,
		},
		SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MACOS, ConfigName: sandboxConfigName},
		MacVm: &ateapipb.MacVMWorkload{Image: image, WakeupProbe: &ateapipb.ContainerWakeupProbe{
			HttpGet: &ateapipb.HTTPGetAction{Port: 8123, Path: "/ready"}, TimeoutSeconds: 180,
		}},
	}}); err != nil {
		t.Fatalf("create Mac ActorTemplate: %v", err)
	}

	const actorName = "mac-actor-e2e"
	created, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: actorName}, ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: templateName},
	}})
	if err != nil {
		t.Fatalf("create Mac Actor: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}})
	if err != nil {
		t.Fatalf("resume Mac Actor: %v", err)
	}
	resumed, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}})
	if err != nil {
		t.Fatalf("get resumed Mac Actor: %v", err)
	}
	if resumed.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("state = %v, want RUNNING", resumed.GetStatus().GetState())
	}
	assignment := resumed.GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() != workerName || assignment.GetRuntimeEndpoint() != endpoint {
		t.Fatalf("assignment = %v, want worker %q at %q", assignment, workerName, endpoint)
	}
	actorEndpoint := assignment.GetActorEndpoint()
	if actorEndpoint.GetHost() == "" || actorEndpoint.GetPort() < 1 || actorEndpoint.GetPort() > 65535 {
		t.Fatalf("Actor endpoint = %v", actorEndpoint)
	}
	probeClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := probeClient.Get(fmt.Sprintf("http://%s:%d/ready", actorEndpoint.GetHost(), actorEndpoint.GetPort()))
	if err != nil {
		t.Fatalf("probe routed Actor endpoint: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("probe status = %s", resp.Status)
	}

	paused, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}})
	if err != nil {
		t.Fatalf("pause Mac Actor: %v", err)
	}
	if paused.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED {
		t.Fatalf("state after pause = %v, want PAUSED", paused.GetActor().GetStatus().GetState())
	}
	local := paused.GetActor().GetStatus().GetLocalSnapshot()
	if local.GetSnapshotName() == "" || paused.GetActor().GetStatus().GetAssignedNode() != workerName {
		t.Fatalf("local Mac snapshot = %v, want snapshot pinned to %q", local, workerName)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}}); err != nil {
		t.Fatalf("resume paused Mac Actor: %v", err)
	}
	resumedAgain, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}})
	if err != nil {
		t.Fatalf("get re-resumed Mac Actor: %v", err)
	}
	if resumedAgain.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING || resumedAgain.GetStatus().GetWorkerAssignment().GetWorker().GetName() != workerName {
		t.Fatalf("re-resumed Actor = %v, want RUNNING on %q", resumedAgain.GetStatus(), workerName)
	}
	actorEndpoint = resumedAgain.GetStatus().GetWorkerAssignment().GetActorEndpoint()
	resp, err = probeClient.Get(fmt.Sprintf("http://%s:%d/ready", actorEndpoint.GetHost(), actorEndpoint.GetPort()))
	if err != nil {
		t.Fatalf("probe re-resumed Actor endpoint: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("re-resumed probe status = %s", resp.Status)
	}
	pausedAgain, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}})
	if err != nil {
		t.Fatalf("second pause Mac Actor: %v", err)
	}
	secondLocal := pausedAgain.GetActor().GetStatus().GetLocalSnapshot()
	if secondLocal.GetSnapshotName() == "" || secondLocal.GetSnapshotName() == local.GetSnapshotName() {
		t.Fatalf("second local snapshot = %v, want a newly minted snapshot after %q", secondLocal, local.GetSnapshotName())
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}}); err != nil {
		t.Fatalf("resume twice-paused Mac Actor: %v", err)
	}
	resumedThird, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}})
	if err != nil {
		t.Fatalf("get twice-resumed Mac Actor: %v", err)
	}
	if resumedThird.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("state after second local resume = %v, want RUNNING", resumedThird.GetStatus().GetState())
	}
	actorEndpoint = resumedThird.GetStatus().GetWorkerAssignment().GetActorEndpoint()
	resp, err = probeClient.Get(fmt.Sprintf("http://%s:%d/ready", actorEndpoint.GetHost(), actorEndpoint.GetPort()))
	if err != nil {
		t.Fatalf("probe twice-resumed Actor endpoint: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("twice-resumed probe status = %s", resp.Status)
	}

	if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}, AnyState: true}); err != nil {
		t.Fatalf("delete Mac Actor: %v", err)
	}
	if _, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName}}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetActor after deletion status = %v, want NotFound", status.Code(err))
	}
	t.Logf("completed Mac Actor lifecycle for UID %s at %s", created.GetMetadata().GetUid(), actorEndpoint.GetHost())
}
