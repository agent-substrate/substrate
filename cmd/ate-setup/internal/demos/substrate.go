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

package demos

import (
	"context"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// SubstrateTemplate names one protojson ActorTemplate manifest and the
// resource it creates.
type SubstrateTemplate struct {
	// Manifest is the protojson *.yaml.tmpl path, relative to the repository
	// root.
	Manifest string
	// Ref is the template's (atespace, name) identity.
	Ref resources.ActorTemplateRef
}

// Substrate covers demos in the substrate-resource shape: one CRD manifest
// for the namespace and worker pool, plus ActorTemplates created through the
// ate API. It is the Go counterpart of deploy_substrate_demo /
// delete_substrate_demo in hack/install-ate.sh.
type Substrate struct {
	// DemoName is the registry name, e.g. "demo-parking".
	DemoName string
	// Short is the one-line summary, in cobra's sense of the word.
	Short string

	// WorkerPoolManifest is the *.yaml.tmpl holding the namespace and
	// WorkerPool, relative to the repository root.
	WorkerPoolManifest string

	// Deployments are the pool Deployments to wait for at deploy time, in
	// order. The WorkerPool controller names each Deployment after its
	// WorkerPool.
	Deployments []steps.TemplateRef

	// Templates are the demo's ActorTemplates, created in order.
	Templates []SubstrateTemplate

	// RenderValues optionally supplies extra placeholder values at deploy
	// time, for demos whose manifests need more than ${BUCKET_NAME} (e.g. a
	// freshly built image digest). Delete renders only the pool manifest, so
	// deploy-only values may live solely here.
	RenderValues func(ctx context.Context, e *steps.Env) (map[string]string, error)
}

func (d *Substrate) Name() string        { return d.DemoName }
func (d *Substrate) Description() string { return d.Short }

// Flags registers nothing: most demos take no options.
func (d *Substrate) Flags(*pflag.FlagSet) {}

// SubstrateDemo returns the demo's substrate configuration. It exists so
// tests can reach the manifests and template refs of demos that embed
// Substrate as uniformly as of ones that are a plain *Substrate. (A method
// named Substrate would be shadowed by the embedded field of that name.)
func (d *Substrate) SubstrateDemo() *Substrate { return d }

// atespaces returns the distinct atespaces of Templates, in first-use order.
func (d *Substrate) atespaces() []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range d.Templates {
		if !seen[t.Ref.Atespace] {
			seen[t.Ref.Atespace] = true
			out = append(out, t.Ref.Atespace)
		}
	}
	return out
}

func (d *Substrate) Deploy(ctx context.Context, e *steps.Env) error {
	log.Step(d.DemoName + "_deploy")
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}

	var values map[string]string
	if d.RenderValues != nil {
		var err error
		if values, err = d.RenderValues(ctx, e); err != nil {
			return err
		}
	}
	// Placeholders that RenderValues supplies are substituted; the rest of
	// the optional set is dropped, so a demo opts lines in by providing
	// values for them (render.Expand drops before it substitutes).
	drop := make([]string, 0, len(ExternalVolumePlaceholders))
	for _, name := range ExternalVolumePlaceholders {
		if _, ok := values[name]; !ok {
			drop = append(drop, name)
		}
	}

	manifest, err := Render(e, d.WorkerPoolManifest, values, drop)
	if err != nil {
		return err
	}
	if err := e.ResolveAndApplyBytes(ctx, manifest); err != nil {
		return err
	}
	for _, ref := range d.Deployments {
		if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, ref.Atespace, ref.Name, steps.DemoTimeout); err != nil {
			return err
		}
	}

	client, err := e.AteClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	for _, atespace := range d.atespaces() {
		if err := steps.EnsureAtespace(ctx, client, atespace); err != nil {
			return err
		}
	}

	for _, t := range d.Templates {
		manifest, err := Render(e, t.Manifest, values, drop)
		if err != nil {
			return err
		}
		// The ko:// image references become digest-pinned ones before the
		// manifest is parsed, which is what the ActorTemplate schema requires;
		// manifests without ko:// references pass through unchanged.
		resolved, err := e.ResolveManifestBytes(ctx, manifest)
		if err != nil {
			return err
		}
		template, err := steps.ActorTemplateFromManifest(resolved)
		if err != nil {
			return err
		}
		if err := steps.CreateActorTemplate(ctx, client, template); err != nil {
			return err
		}
	}

	// Block until the golden snapshots exist. On a cold cluster the first
	// ActorTemplate pays one-time costs (downloading runsc, the first gVisor
	// pod start, image pulls); blocking here means callers run against an
	// already-warm node instead of racing that cold-start work.
	for _, t := range d.Templates {
		log.Stepf("Waiting for the %s golden snapshot...", t.Ref)
		if err := steps.WaitActorTemplateGolden(ctx, client, t.Ref, steps.DemoTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (d *Substrate) Delete(ctx context.Context, e *steps.Env) error {
	log.Step(d.DemoName + "_delete")
	refs := make([]resources.ActorTemplateRef, 0, len(d.Templates))
	for _, t := range d.Templates {
		refs = append(refs, t.Ref)
	}
	if err := e.DeleteSubstrateDemo(ctx, refs, d.atespaces()); err != nil {
		return err
	}
	manifest, err := Render(e, d.WorkerPoolManifest, nil, ExternalVolumePlaceholders)
	if err != nil {
		return err
	}
	return e.Kube.DeleteBytes(ctx, manifest)
}
