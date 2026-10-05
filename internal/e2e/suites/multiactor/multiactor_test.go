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

package multiactor

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const multiactorTemplate = "probe-multiactor"

// Separate fixture namespaces for each sandbox class.
var multiactorNamespace = e2e.FixtureName("ate-e2e") + "-multiactor"

// whoamiResponse identifies the actor that served the request.
type whoamiResponse struct {
	Atespace string `json:"atespace"`
	File     string `json:"file"`
	UID      string `json:"uid"`
	Error    string `json:"error"`
}

// TestTwoActorsShareOneWorker verifies placement, routing, and independent suspension.
func TestTwoActorsShareOneWorker(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatal(err)
	}

	e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/probe/probe-multiactor.yaml.tmpl",
		Template: "internal/e2e/fixtures/probe/probe-multiactor-template.yaml.tmpl",
	}, env["BUCKET_NAME"], "multiactor", false)

	names := []string{"ma-first", "ma-second"}
	pods := map[string]string{}
	for _, name := range names {
		e2e.CreateActor(t, ctx, clients, &ateapipb.ObjectRef{Atespace: multiactorNamespace, Name: name}, &ateapipb.ObjectRef{Atespace: multiactorNamespace, Name: multiactorTemplate})
		resumeActor(t, ctx, clients, name)
		pods[name] = workerPodOf(t, ctx, clients, name)
		t.Logf("actor %s is RUNNING on worker pod %s", name, pods[name])
	}
	if pods[names[0]] != pods[names[1]] {
		t.Fatalf("actors landed on different workers (%s and %s); the pool has one replica, so one of them was not placed where expected",
			pods[names[0]], pods[names[1]])
	}

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	for _, name := range names {
		got := whoami(t, ctx, rc, name)
		if got.Atespace != multiactorNamespace || got.File != name {
			t.Errorf("request for %s was served by atespace=%q actor=%q; each actor must be reached in its own sandbox",
				name, got.Atespace, got.File)
		}
	}

	// Suspending one actor must leave the other serving.
	suspendActor(t, ctx, clients, names[0])
	if got := whoami(t, ctx, rc, names[1]); got.File != names[1] {
		t.Errorf("after suspending %s, %s answered as %q; suspending one actor disturbed its neighbor",
			names[0], names[1], got.File)
	}
}

func resumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: multiactorNamespace, Name: name}}); err != nil {
		t.Fatalf("ResumeActor %q: %v", name, err)
	}
}

func suspendActor(t *testing.T, ctx context.Context, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: multiactorNamespace, Name: name}}); err != nil {
		t.Fatalf("SuspendActor %q: %v", name, err)
	}
}

// workerPodOf is the pod the actor was placed on, once it is RUNNING.
func workerPodOf(t *testing.T, ctx context.Context, clients *e2e.Clients, name string) string {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: multiactorNamespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor %q: %v", name, err)
	}
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("actor %q is %v, want RUNNING", name, got)
	}
	pod := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	if pod == "" {
		t.Fatalf("actor %q is RUNNING with no worker pod", name)
	}
	return pod
}

func whoami(t *testing.T, ctx context.Context, rc *e2e.RouterClient, name string) whoamiResponse {
	t.Helper()
	var out whoamiResponse
	if err := rc.GetJSON(ctx, resources.ActorRef{Atespace: multiactorNamespace, Name: name}, "/whoami", &out); err != nil {
		t.Fatal(err)
	}
	if out.Error != "" {
		t.Logf("/whoami for %q reported: %s", name, out.Error)
	}
	return out
}
