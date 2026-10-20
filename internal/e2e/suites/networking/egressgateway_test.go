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

package networking

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	atev1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TestAssignedEgressGateway runs in both sandbox lanes. The actor uses a
// template in another Atespace, but its gateway comes from its own binding.
func TestAssignedEgressGateway(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	clients := e2e.GetClients()
	gateways := clients.SubstrateK8s.ApiV1alpha1().EgressGateways()
	installed, err := gateways.Get(ctx, "default", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("gateway-%d", time.Now().UnixNano())
	gateway := &atev1.EgressGateway{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: installed.Spec}
	if _, err := gateways.Create(ctx, gateway, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gateways.Delete(context.Background(), name, metav1.DeleteOptions{}) })
	create := &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: name}, EgressGateway: &ateapipb.ObjectRef{Name: name},
	}}
	// Wait for ateapi's informer to observe the newly created gateway.
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := clients.SubstrateAPI.CreateAtespace(ctx, create)
		if status.Code(err) == codes.FailedPrecondition {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: name}})
	})
	fixture := e2e.EgressFixture()
	ref := &ateapipb.ObjectRef{Atespace: name, Name: "actor"}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: name, Name: ref.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: fixture.Namespace, Name: fixture.Name},
	}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: ref})
	})
	e2e.EnsureEgressPolicy(t, ctx, clients, ref, e2e.EgressAllowAll()...)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	target := e2e.DeployServerPod(t, ctx, egressHTTPTarget())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	code, body := fetchThroughEgressActor(t, ctx, router, resources.ActorRef{Atespace: name, Name: ref.Name}, "http://"+target.Address()+"/healthz")
	if code != http.StatusOK {
		t.Fatalf("assigned gateway fetch = %d: %s", code, body)
	}
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if err := gateways.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	// Probe creation with another Atespace to observe cache deletion before
	// attempting restore. Any probe that wins the deletion race is removed.
	probeName := name + "-probe"
	create.Atespace.Metadata.Name = probeName
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := clients.SubstrateAPI.CreateAtespace(ctx, create)
		if status.Code(err) == codes.FailedPrecondition {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		_, err = clients.SubstrateAPI.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: probeName}})
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	_, err = e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), name) {
		t.Fatalf("resume after gateway deletion = %v, want FailedPrecondition naming %s", err, name)
	}
}
