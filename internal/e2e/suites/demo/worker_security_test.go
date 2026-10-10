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
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestWorkerSecurityContextLifecycle(t *testing.T) {
	ctx := t.Context()
	clients := e2e.GetClients()
	src := e2e.SubstrateCounterFixture()
	pool, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(src.PoolNamespace).Get(ctx, src.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name             string
		dropCapabilities []corev1.Capability
	}{
		{name: "drop SYS_PTRACE", dropCapabilities: []corev1.Capability{"SYS_PTRACE"}},
		{name: "drop MKNOD", dropCapabilities: []corev1.Capability{"MKNOD"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ns := e2e.CreateNamespace(t)
			template := pool.Spec.Template.DeepCopy()
			if template == nil {
				template = &v1alpha1.WorkerPoolPodTemplate{}
			}
			// The source ServiceAccount belongs to the fixture's namespace.
			template.ServiceAccountName = nil
			template.SecurityContext = &v1alpha1.WorkerPoolSecurityContext{
				DropCapabilities:         tt.dropCapabilities,
				AllowPrivilegeEscalation: ptr.To(false),
			}
			at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, ns.Name, e2e.SubstrateTemplateOptions{
				Atespace: demoAtespace, Name: "worker-security-" + ns.Name,
				PoolName: "worker-security", PoolReplicas: 1, PoolTemplate: template,
				Labels: map[string]string{"worker-security": ns.Name},
			})
			pods, err := clients.K8s.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: "ate.dev/worker-pool=worker-security"})
			if err != nil {
				t.Fatal(err)
			}
			if len(pods.Items) != 1 {
				t.Fatalf("worker pod count: %d, want 1", len(pods.Items))
			}
			sc := pods.Items[0].Spec.Containers[0].SecurityContext
			if sc == nil || sc.Capabilities == nil {
				t.Fatal("worker capabilities were not configured")
			}
			if !slices.Contains(sc.Capabilities.Drop, corev1.Capability("ALL")) {
				t.Fatal("worker did not drop the container runtime's default capabilities")
			}
			for _, capability := range tt.dropCapabilities {
				if slices.Contains(sc.Capabilities.Add, capability) {
					t.Fatalf("worker still adds dropped capability %s", capability)
				}
			}
			if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
				t.Fatal("worker privilege escalation was not disabled")
			}
			name := "worker-security-" + ns.Name
			ref := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: name}
			if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: name}, ActorTemplate: e2e.TemplateRef(at),
			}}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: ref})
				_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: ref})
			})
			for turn := 1; turn <= 3; turn++ {
				if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
				response, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: name})
				if err != nil {
					t.Fatal(err)
				}
				validateCounterResponse(t, response, "worker security", turn, turn)
				if turn == 1 {
					if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
