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
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/render"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// bucketNamePlaceholder is substituted into every demo template with the
// snapshot bucket for this environment.
const bucketNamePlaceholder = "BUCKET_NAME"

// ExternalVolumePlaceholders are the optional external-volume hooks in the
// counter and autoscaled-workerpool templates. Every path except
// `deploy demo counter --with-external-volume` drops them, which removes the
// lines entirely.
var ExternalVolumePlaceholders = []string{
	"VALIDATE_EXISTING_FILE_PATH_ARG",
	"EXTERNAL_VOLUME_MOUNTS",
	"EXTERNAL_VOLUMES",
}

// Render expands a demo template with the configured bucket name and the
// build version (pool templates pin worker pods to version-labeled nodes).
func Render(e *steps.Env, relPath string, extraValues map[string]string, drop []string) ([]byte, error) {
	version, _, err := e.SubstrateVersion()
	if err != nil {
		return nil, err
	}
	values := map[string]string{
		bucketNamePlaceholder: e.Cfg.BucketName,
		"SUBSTRATE_VERSION":   version,
	}
	for k, v := range extraValues {
		values[k] = v
	}
	return render.Template(e.Cfg.Path(relPath), values, drop)
}

// Simple covers the demos that are one template plus a fixed set of readiness
// waits: render, ko apply, wait; and on delete, remove the actors then the same
// rendered manifest.
//
// Demos that need more embed it and override a method, calling back into
// DeployWorkload and WaitReady to keep the shared ordering.
type Simple struct {
	// DemoName is the registry name, e.g. "demo-counter". It cannot be called
	// Name: that is the accessor the Demo interface requires.
	DemoName string
	// Short is the one-line summary, in cobra's sense of the word.
	Short string

	// Template is the *.yaml.tmpl path, relative to the repository root.
	Template string

	// Deployments are the Deployments to wait for at deploy time, in order.
	// The WorkerPool controller names each Deployment after its WorkerPool.
	Deployments []steps.TemplateRef

	// ActorTemplates are the demo's ActorTemplates. Their actors are removed
	// before the manifests at delete time.
	ActorTemplates []steps.TemplateRef
}

func (d *Simple) Name() string        { return d.DemoName }
func (d *Simple) Description() string { return d.Short }

// Flags registers nothing: most demos take no options.
func (d *Simple) Flags(*pflag.FlagSet) {}

// TemplatePath exposes the demo's template through the Demo interface, so tests
// can check that every template renders cleanly.
func (d *Simple) TemplatePath() string { return d.Template }

func (d *Simple) Deploy(ctx context.Context, e *steps.Env) error {
	log.Step(d.DemoName + "_deploy")
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	if err := d.DeployWorkload(ctx, e); err != nil {
		return err
	}
	return d.WaitReady(ctx, e)
}

// DeployWorkload renders the demo template and applies it through ko, without
// waiting. Demos that install add-ons alongside the workload call this directly
// so they can order the add-ons against it.
func (d *Simple) DeployWorkload(ctx context.Context, e *steps.Env) error {
	manifest, err := Render(e, d.Template, nil, ExternalVolumePlaceholders)
	if err != nil {
		return err
	}
	return e.ResolveAndApplyBytes(ctx, manifest)
}

// WaitReady blocks until the demo's Deployments are rolled out.
func (d *Simple) WaitReady(ctx context.Context, e *steps.Env) error {
	if len(d.Deployments) == 0 {
		return nil
	}
	log.Stepf("Waiting for %s to be ready...", d.DemoName)
	for _, ref := range d.Deployments {
		if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, ref.Atespace, ref.Name, steps.DemoTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (d *Simple) Delete(ctx context.Context, e *steps.Env) error {
	log.Step(d.DemoName + "_delete")
	if err := e.DeleteDemoActors(ctx, d.ActorTemplates...); err != nil {
		return err
	}
	manifest, err := Render(e, d.Template, nil, ExternalVolumePlaceholders)
	if err != nil {
		return err
	}
	return e.Kube.DeleteBytes(ctx, manifest)
}
