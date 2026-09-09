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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TemplateRef identifies a demo's ActorTemplate. Actors are deleted by matching
// against it because the demo manifests own the template, not the actors that
// were created from it.
type TemplateRef struct {
	// Atespace the object lives in. The demos name each atespace after the
	// k8s namespace holding its worker pool, so this field also addresses
	// the namespaced objects a demo waits on, such as pool Deployments.
	Atespace string
	Name     string
}

// DeleteDemoActors removes every actor created from the given ActorTemplates.
//
// Demo teardown has to do this before deleting the manifests: an ActorTemplate
// removed out from under running actors leaves them stranded. As in the shell
// version, a cluster with no ate-api-server, or an apiserver that cannot be
// reached, is not an error -- there is nothing to clean up on a cluster that
// never had the control plane, and DeleteAll runs this for every demo.
func (e *Env) DeleteDemoActors(ctx context.Context, refs ...TemplateRef) error {
	if len(refs) == 0 {
		return nil
	}

	present, err := e.Kube.DeploymentExists(ctx, NamespaceAteSystem, "ate-api-server")
	if err != nil {
		return err
	}
	if !present {
		log.Step("ate-api-server not found; skipping actor cleanup")
		return nil
	}

	client, err := ateclient.NewClient(ctx, e.Cfg.Kubeconfig, e.Cfg.Context, "", "", false)
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
		log.Stepf("Deleting actors for %s/%s", ref.Atespace, ref.Name)
		for _, actor := range actors {
			if actor.GetActorTemplate().GetAtespace() != ref.Atespace || actor.GetActorTemplate().GetName() != ref.Name {
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
	return nil
}

// listAllActors pages through every actor in every atespace.
func listAllActors(ctx context.Context, client *ateclient.Client) ([]*ateapipb.Actor, error) {
	var all []*ateapipb.Actor
	pageToken := ""
	for {
		// An empty Atespace lists across all atespaces.
		resp, err := client.ListActors(ctx, &ateapipb.ListActorsRequest{
			PageSize:  1000,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, resp.GetActors()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return all, nil
		}
	}
}
