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

package steps

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// AteClient connects to the ate-api-server, port-forwarding if needed.
func (e *Env) AteClient(ctx context.Context) (*ateclient.Client, error) {
	return ateclient.NewClient(ctx, e.Cfg.Kubeconfig, e.Cfg.Context, "", "", false)
}

// EnsureAtespace creates the atespace if it does not already exist. The store
// enforces that an ActorTemplate's atespace exists at create time.
func EnsureAtespace(ctx context.Context, client *ateclient.Client, atespace string) error {
	_, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: atespace},
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

// ActorTemplateFromManifest parses a single protojson-shaped YAML or JSON
// document into an ActorTemplate, as `kubectl ate create actor-template`
// does. Parsing is strict: unknown fields are an error, so typos don't
// silently drop configuration.
func ActorTemplateFromManifest(manifest []byte) (*ateapipb.ActorTemplate, error) {
	jsonData, err := yaml.YAMLToJSON(manifest)
	if err != nil {
		return nil, fmt.Errorf("invalid YAML: %w", err)
	}
	if string(jsonData) == "null" {
		return nil, fmt.Errorf("manifest is empty")
	}
	template := &ateapipb.ActorTemplate{}
	if err := protojson.Unmarshal(jsonData, template); err != nil {
		return nil, err
	}
	return template, nil
}

// CreateActorTemplate creates the template through the ate API. Actor
// templates are immutable (no update RPC), so an existing template is left
// in place: delete the demo and redeploy to change it.
func CreateActorTemplate(ctx context.Context, client *ateclient.Client, template *ateapipb.ActorTemplate) error {
	ref := resources.ActorTemplateRefFromActorTemplate(template)
	_, err := client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: template})
	if status.Code(err) == codes.AlreadyExists {
		log.Stepf("actor template %s already exists; keeping it (delete the demo to replace it)", ref)
		return nil
	}
	if err != nil {
		return fmt.Errorf("creating actor template %s: %w", ref, err)
	}
	return nil
}

// WaitActorTemplateGolden blocks until the template's golden snapshot is
// built, the substrate counterpart of the ActorTemplate CRD's Ready
// condition. It fails fast when the template reconciler reports an error.
func WaitActorTemplateGolden(ctx context.Context, client *ateclient.Client, ref resources.ActorTemplateRef, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	// The last Get error rides along in the timeout error: a wait that spent
	// its whole budget on a broken connection should say so, not point at the
	// template reconciler.
	var lastErr error
	for {
		template, err := client.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{ActorTemplate: ref.ToObjectRef()})
		lastErr = err
		if err == nil {
			goldenStatus := template.GetStatus().GetGoldenSnapshotStatus()
			if goldenStatus.GetGoldenSnapshot().GetName() != "" {
				return nil
			}
			if msg := goldenStatus.GetErrorMessage(); msg != "" {
				return fmt.Errorf("actor template %s failed: %s", ref, msg)
			}
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for actor template %s golden snapshot: last error: %w", ref, lastErr)
			}
			return fmt.Errorf("timed out waiting for actor template %s golden snapshot", ref)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// DeleteSubstrateDemo removes a substrate demo's control-plane resources:
// every actor created from the given templates, then the templates (which
// server-side also removes their golden actors and snapshots), then the
// atespaces. As with DeleteDemoActors, a cluster without a reachable
// ate-api-server is not an error -- there is nothing to clean up.
func (e *Env) DeleteSubstrateDemo(ctx context.Context, refs []resources.ActorTemplateRef, atespaces []string) error {
	present, err := e.Kube.DeploymentExists(ctx, NamespaceAteSystem, "ate-api-server")
	if err != nil {
		return err
	}
	if !present {
		log.Step("ate-api-server not found; skipping actor cleanup")
		return nil
	}

	client, err := e.AteClient(ctx)
	if err != nil {
		log.Warnf("could not connect to ate-api-server; skipping actor cleanup: %v", err)
		return nil
	}
	defer client.Close()

	actors, err := listAllActors(ctx, client)
	if err != nil {
		log.Warnf("could not list actors; skipping actor cleanup: %v", err)
		return nil
	}

	for _, ref := range refs {
		log.Stepf("Deleting actors for %s", ref)
		for _, actor := range actors {
			if resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate()) != ref {
				continue
			}
			actorRef := resources.ActorRefFromActor(actor)
			log.Stepf("  deleting actor %s/%s", actorRef.Atespace, actorRef.Name)
			// AnyState skips the SUSPENDED precondition: teardown discards the
			// actor, so there is nothing to be gained by suspending it first.
			if _, err := client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
				Actor:    actorRef.ToObjectRef(),
				AnyState: true,
			}); err != nil {
				log.Warnf("while deleting actor %s/%s: %v", actorRef.Atespace, actorRef.Name, err)
			}
		}
	}

	for _, ref := range refs {
		if _, err := client.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: ref.ToObjectRef()}); err != nil && status.Code(err) != codes.NotFound {
			log.Warnf("while deleting actor template %s: %v", ref, err)
		}
	}

	for _, atespace := range atespaces {
		if _, err := client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: atespace}}); err != nil && status.Code(err) != codes.NotFound {
			log.Warnf("while deleting atespace %s: %v", atespace, err)
		}
	}
	return nil
}
