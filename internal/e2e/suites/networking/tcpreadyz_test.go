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
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestTCPReadiness exercises the public template, persisted configuration, both
// internal RPC hops, and the sandbox's start and restore readiness gates. The
// server exposes only gRPC; an HTTP probe on its port can never succeed.
func TestTCPReadiness(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	clients := e2e.GetClients()
	atespace, templates := e2e.DeploySubstrateFixture(t, ctx, clients, grpcEchoFixtureManifests, env["BUCKET_NAME"], "tcpreadyz", false)

	newTemplate := func(t *testing.T, name string, port int32, timeout int32) *ateapipb.ActorTemplate {
		t.Helper()
		tmpl := proto.Clone(templates[0]).(*ateapipb.ActorTemplate)
		tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: atespace, Name: name}
		tmpl.Status = nil
		tmpl.Containers[0].Args = []string{"grpc", "--listen=:80", "--listen-delay=5s"}
		tmpl.Containers[0].Readyz = &ateapipb.ContainerReadyz{
			TcpSocket: &ateapipb.TCPSocketAction{Port: port}, TimeoutSeconds: timeout,
		}
		created, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if _, err := clients.SubstrateAPI.DeleteActorTemplate(cleanupCtx, &ateapipb.DeleteActorTemplateRequest{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
			}); err != nil {
				t.Logf("deleting template: %v", err)
			}
		})
		stored, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: e2e.TemplateRef(created)})
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(stored.GetContainers()[0].GetReadyz(), tmpl.Containers[0].Readyz) {
			t.Fatalf("stored readiness changed: %v", stored.GetContainers()[0].GetReadyz())
		}
		return created
	}

	t.Run("delayed listener and restore", func(t *testing.T) {
		started := time.Now()
		tmpl := newTemplate(t, "tcp-delayed", 80, 30)
		// The cold-start gate must not produce a golden snapshot while the
		// non-HTTP server is still in its five-second pre-listen delay.
		for time.Since(started) < 5*time.Second {
			stored, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: e2e.TemplateRef(tmpl)})
			if err != nil {
				t.Fatal(err)
			}
			if stored.GetStatus().GetGoldenSnapshotStatus().GetGoldenSnapshot() != nil {
				t.Fatal("golden snapshot became ready before the TCP listener could open")
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
		e2e.WaitForSubstrateTemplateReady(ctx, t, clients, atespace, tmpl.GetMetadata().GetName())
		name, _ := createAndResumeSubstrateActor(t, ctx, "tcpreadyz", e2e.SubstrateFixture{Atespace: atespace, Name: tmpl.GetMetadata().GetName()})
		ref := &ateapipb.ObjectRef{Atespace: networkingAtespace, Name: name}
		rpcCtx := metadata.AppendToOutgoingContext(ctx, atenet.TargetActorHeader, networkingAtespace+"/"+name)
		conn, err := grpc.NewClient(routerAddress(t, ctx), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		client := grpcechopb.NewEchoClient(conn)
		checkEcho := func() {
			t.Helper()
			waitForGRPCRouteReady(t, rpcCtx, client, "TCP readiness")
			response, err := client.Echo(rpcCtx, &grpcechopb.EchoRequest{Message: "TCP readiness"})
			if err != nil || response.GetMessage() != "TCP readiness" {
				t.Fatalf("Echo after readiness: response=%v err=%v", response, err)
			}
		}
		checkEcho()
		if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
			t.Fatal(err)
		}
		if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
			t.Fatal(err)
		}
		checkEcho()
	})

	t.Run("never listening", func(t *testing.T) {
		tmpl := newTemplate(t, "tcp-refused", 81, 1)
		ref := &ateapipb.ObjectRef{Atespace: atespace, Name: fmt.Sprintf("tcp-refused-%d", time.Now().UnixNano())}
		if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
			ActorTemplate: e2e.TemplateRef(tmpl),
		}}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			// A failed start may already have torn down its sandbox. Remove
			// the disposable worker before asking the API to clean up its actor.
			actor, err := clients.SubstrateAPI.GetActor(cleanupCtx, &ateapipb.GetActorRequest{Actor: ref})
			if err != nil {
				t.Errorf("getting failed actor for cleanup: %v", err)
				return
			}
			assignment := actor.GetStatus().GetWorkerAssignment()
			if pod := assignment.GetWorkerPod(); pod != "" {
				if assignment.GetWorkerNamespace() != atespace {
					t.Errorf("worker outside fixture namespace: %v", assignment)
					return
				}
				zero := int64(0)
				if err := clients.K8s.CoreV1().Pods(atespace).Delete(cleanupCtx, pod, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !apierrors.IsNotFound(err) {
					t.Errorf("deleting failed worker: %v", err)
					return
				}
				for {
					_, err := clients.K8s.CoreV1().Pods(atespace).Get(cleanupCtx, pod, metav1.GetOptions{})
					if apierrors.IsNotFound(err) {
						break
					}
					select {
					case <-cleanupCtx.Done():
						t.Errorf("waiting for failed worker deletion: %v", cleanupCtx.Err())
						return
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
			if _, err := clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true}); err != nil {
				t.Errorf("deleting failed actor: %v", err)
			}
		})
		deadline, stop := context.WithTimeout(ctx, 60*time.Second)
		defer stop()
		started := time.Now()
		_, err := e2e.ResumeActorAwaitCapacity(t, deadline, clients, &ateapipb.ResumeActorRequest{Actor: ref})
		if err == nil || !strings.Contains(err.Error(), "readyz") {
			t.Fatalf("ResumeActor did not return a runtime readiness failure: %v", err)
		}
		if time.Since(started) < time.Second {
			t.Fatal("readiness failed before the configured probe deadline")
		}
		stored, err := clients.SubstrateAPI.GetActor(deadline, &ateapipb.GetActorRequest{Actor: ref})
		if err != nil {
			t.Fatal(err)
		}
		if stored.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			t.Fatal("never-listening TCP target became RUNNING")
		}
		t.Log("never-listening target timed out through the runtime readiness gate")
	})
}
