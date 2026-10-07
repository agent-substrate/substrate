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

// Command mac-lab exercises a deployed Control API, never an in-process store.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:18443", "deployed Control API")
	pki := flag.String("pki", "", "lab PKI directory")
	runtime := flag.String("runtime", "host.container.internal:9443", "HostRuntime address reachable from the API")
	image := flag.String("image", "", "host's exact digest-pinned fixture reference")
	flag.Parse()
	if *pki == "" || *image == "" {
		log.Fatal("--pki and --image are required; this command creates and deletes one disposable guest")
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(*pki, "operator.crt"), filepath.Join(*pki, "operator.key"))
	if err != nil {
		log.Fatal(err)
	}
	pem, err := os.ReadFile(filepath.Join(*pki, "ca.crt"))
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		log.Fatal("invalid CA")
	}
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: "api.ate-system.svc",
	})))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	start := time.Now()
	if err := exercise(ctx, ateapipb.NewControlClient(conn), *runtime, *image); err != nil {
		log.Fatal(err)
	}
	log.Printf("PASS: deployed Mac Actor lifecycle in %s", time.Since(start).Round(time.Millisecond))
}

func exercise(ctx context.Context, c ateapipb.ControlClient, runtime, image string) (result error) {
	name := fmt.Sprintf("mac-lab-%d", time.Now().UnixNano())
	global := &ateapipb.ObjectRef{Name: name}
	ref := &ateapipb.ObjectRef{Atespace: name, Name: "smoke"}
	// Cleanup gets its own deadline even when the exercise times out. A failed
	// cleanup is a failed run and leaves the resource name in the log.
	cleanup := func(kind string, fn func(context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := fn(ctx); err != nil && status.Code(err) != codes.NotFound {
			result = errors.Join(result, fmt.Errorf("cleanup %s %s: %w", kind, name, err))
		}
	}
	w, err := c.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: name}, SandboxClass: "macos-vz",
		ExternalHost: &ateapipb.ExternalWorkerHost{RuntimeEndpoint: runtime, Capacity: &ateapipb.WorkerResources{Actors: 1}},
	}})
	if err != nil {
		return fmt.Errorf("register Worker: %w", err)
	}
	defer cleanup("Worker", func(ctx context.Context) error {
		_, err := c.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: global})
		return err
	})
	if w.GetStatus().GetCapacity().GetActors() != 1 {
		return fmt.Errorf("registered capacity = %v", w.GetStatus().GetCapacity())
	}
	log.Printf("Worker %s registered ACTIVE, capacity=1", name)
	if _, err := c.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}}); err != nil {
		return err
	}
	defer cleanup("Atespace", func(ctx context.Context) error {
		_, err := c.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: global})
		return err
	})
	if _, err := c.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: name, Name: ref.Name},
		SnapshotConfig: &ateapipb.SnapshotConfig{OnPause: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DISK, OnCommit: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DISK, StorageLocation: "s3://mac-lab-disabled"},
		SandboxConfig:  &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_MACOS, ConfigName: "macos-vz-lab"},
		MacVm:          &ateapipb.MacVMWorkload{Image: image, WakeupProbe: &ateapipb.ContainerWakeupProbe{HttpGet: &ateapipb.HTTPGetAction{Port: 8123, Path: "/ready"}, TimeoutSeconds: 180}},
	}}); err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	defer cleanup("ActorTemplate", func(ctx context.Context) error {
		_, err := c.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: ref})
		return err
	})
	actor, err := c.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: name, Name: ref.Name}, ActorTemplate: ref}})
	if err != nil {
		return err
	}
	defer cleanup("Actor", func(ctx context.Context) error {
		_, err := c.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true})
		return err
	})
	log.Printf("Actor UID=%s created", actor.GetMetadata().GetUid())
	var snapshot string
	for cycle := range 3 {
		if _, err := c.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
			return fmt.Errorf("resume %d: %w", cycle, err)
		}
		a, err := c.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
		if err != nil {
			return err
		}
		assignment := a.GetStatus().GetWorkerAssignment()
		if a.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING || assignment.GetWorker().GetName() != name || assignment.GetRuntimeEndpoint() != runtime {
			return fmt.Errorf("unexpected running assignment: %v", a.GetStatus())
		}
		ep := assignment.GetActorEndpoint()
		url := "http://" + net.JoinHostPort(ep.GetHost(), strconv.Itoa(int(ep.GetPort()))) + "/ready"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := client.Do(req)
		client.CloseIdleConnections()
		if err != nil {
			return fmt.Errorf("routed readiness %s: %w", url, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("readiness %s: %s", url, resp.Status)
		}
		log.Printf("cycle=%d RUNNING persisted endpoint=%s HTTP=%d", cycle, url, resp.StatusCode)
		if cycle == 2 {
			break
		}
		paused, err := c.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref})
		if err != nil {
			return err
		}
		local := paused.GetActor().GetStatus().GetLocalSnapshot()
		if paused.GetActor().GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_PAUSED || local.GetSnapshotName() == "" || local.GetSnapshotName() == snapshot || len(local.GetNodeVmsWithLocalSnapshots()) != 1 || local.GetNodeVmsWithLocalSnapshots()[0] != name {
			return fmt.Errorf("invalid pause receipt: %v", paused)
		}
		snapshot = local.GetSnapshotName()
		log.Printf("cycle=%d PAUSED snapshot=%s", cycle, snapshot)
	}
	if _, err := c.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true}); err != nil {
		return err
	}
	if _, err := c.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("get deleted Actor: %v, want NotFound", err)
	}
	log.Printf("Actor deleted; GetActor=NotFound")
	return nil
}
