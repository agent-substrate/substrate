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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

type applicationState struct {
	BootID  string `json:"bootID"`
	Counter int64  `json:"counter"`
}

func TestActorSuspendRejectsExitedApplication(t *testing.T) {
	if e2e.IsMicroVM() {
		t.Skip("application liveness at FULL checkpoint is a gVisor contract")
	}
	base, image := setup(t)
	container := servingContainer(image, "app", 8080)
	container.WakeupProbe = nil
	tmpl := createTemplate(t, base, "actor-recovery", []*ateapipb.Container{container})
	golden := waitForGolden(t, e2e.TemplateRef(tmpl))
	if golden.GetErrorMessage() != "" || golden.GetGoldenTag() == nil {
		t.Fatalf("healthy golden failed: %v", golden)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	clients := e2e.GetClients()
	api := clients.SubstrateAPI
	ref := &ateapipb.ObjectRef{Atespace: tmpl.GetMetadata().GetAtespace(), Name: "actor-recovery"}
	created, err := api.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
		ActorTemplate: e2e.TemplateRef(tmpl),
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
	router, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	actorRef := resources.ActorRefFromObjectRef(ref)
	resume := func() {
		t.Helper()
		if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
			t.Fatal(err)
		}
		waitForBootID(t, ctx, router, actorRef, 8080)
	}
	readState := func(method, path string) applicationState {
		t.Helper()
		body, code, err := requestActor(ctx, router, actorRef, 8080, method, path)
		if err != nil || code != http.StatusOK {
			t.Fatalf("%s %s: HTTP %d, %v, body %q", method, path, code, err, body)
		}
		var state applicationState
		if err := json.Unmarshal(body, &state); err != nil || state.BootID == "" {
			t.Fatalf("application state = %q, %v", body, err)
		}
		return state
	}
	resume()
	savedState := readState(http.MethodPost, "/increment")
	suspended, err := api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	snapshotURI := suspended.GetActor().GetStatus().GetExternalSnapshot().GetSnapshotUri()
	if snapshotURI == "" || snapshotURI == created.GetStatus().GetExternalSnapshot().GetSnapshotUri() {
		t.Fatalf("ordinary suspend did not create its own snapshot: %q", snapshotURI)
	}
	assertSnapshot := func(actor *ateapipb.Actor, state ateapipb.ActorState) {
		t.Helper()
		if got := actor.GetStatus().GetState(); got != state {
			t.Fatalf("actor state = %v, want %v", got, state)
		}
		if got := actor.GetStatus().GetExternalSnapshot().GetSnapshotUri(); got != snapshotURI {
			t.Fatalf("external snapshot = %q, want last good snapshot %q", got, snapshotURI)
		}
	}
	resume()
	if got := readState(http.MethodGet, "/state"); got != savedState {
		t.Fatalf("first restore state = %+v, want %+v", got, savedState)
	}
	if got := readState(http.MethodPost, "/increment"); got.Counter != savedState.Counter+1 {
		t.Fatalf("unsaved counter = %d, want %d", got.Counter, savedState.Counter+1)
	}
	if body, code, err := requestActor(ctx, router, actorRef, 8080, http.MethodPost, "/exit"); err != nil || code != http.StatusAccepted {
		t.Fatalf("exit application: HTTP %d, %v, body %q", code, err, body)
	}
	if _, code, err := requestActor(ctx, router, actorRef, 8080, http.MethodGet, "/state"); err == nil && code == http.StatusOK {
		t.Fatal("application still serves requests after exiting")
	}
	if _, err := api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err == nil {
		t.Fatal("suspend accepted an exited application")
	}
	crashed, err := api.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshot(crashed, ateapipb.ActorState_ACTOR_STATE_CRASHED)
	reverted, err := api.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshot(reverted.GetActor(), ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	resume()
	if got := readState(http.MethodGet, "/state"); got != savedState {
		t.Fatalf("recovered state = %+v, want snapshot state %+v; cold boot or unsaved state was restored", got, savedState)
	}
}

func requestActor(ctx context.Context, router *e2e.RouterClient, actor resources.ActorRef, port int, method, path string) ([]byte, int, error) {
	conn, err := router.Connect(ctx, actor, port)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+actor.Name+path, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Close = true
	req.Header.Set(atenet.TargetActorHeader, actor.String())
	if err := req.Write(conn); err != nil {
		return nil, 0, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read actor response: %w", err)
	}
	return body, resp.StatusCode, nil
}
