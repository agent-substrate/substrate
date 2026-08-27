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

// Package autoscaledworkerpool installs the counter workload plus the
// custom-metrics stack that drives a HorizontalPodAutoscaler over its
// WorkerPool.
//
// The demo is Kind only: it ships its own prometheus-adapter and a Kind
// specific HPA, so it is not offered at all on a GKE install.
package autoscaledworkerpool

import (
	"context"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// namespace holds the demo workload, its prometheus-adapter, and the HPA.
const namespace = "ate-demo-autoscaled-workerpool"

// Add-ons that only make sense on Kind.
const (
	prometheusAdapterManifest = "demos/autoscaled-workerpool/prometheus-adapter.yaml"
	hpaKindManifest           = "demos/autoscaled-workerpool/hpa-kind.yaml"
)

type demo struct {
	demos.Simple
}

func init() {
	demos.Register(&demo{Simple: demos.Simple{
		DemoName:       "demo-autoscaled-workerpool",
		Short:          "A WorkerPool scaled by an HPA over custom metrics (Kind only)",
		Template:       "demos/autoscaled-workerpool/autoscaled-workerpool.yaml.tmpl",
		Deployments:    []steps.TemplateRef{{Namespace: namespace, Name: "counter"}},
		ActorTemplates: []steps.TemplateRef{{Namespace: namespace, Name: "counter"}},
	}})
}

func (d *demo) KindOnly() bool { return true }

func (d *demo) Deploy(ctx context.Context, e *steps.Env) error {
	if err := e.RequireKind(d.DemoName); err != nil {
		return err
	}
	log.Step(d.DemoName + "_deploy")
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	// The prometheus-adapter and HPA manifests below land in the demo's
	// namespace, which the workload template creates. Create it up front so
	// they can be applied in any order.
	if err := e.Kube.EnsureNamespace(ctx, namespace); err != nil {
		return err
	}

	log.Step("Deploying autoscaled-workerpool workload...")
	if err := d.DeployWorkload(ctx, e); err != nil {
		return err
	}

	log.Step("Deploying prometheus-adapter and HPA for kind...")
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Path(prometheusAdapterManifest)); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, namespace,
		"prometheus-adapter", e.Cfg.WaitTimeout(steps.BootstrapTimeout)); err != nil {
		return err
	}
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Path(hpaKindManifest)); err != nil {
		return err
	}

	return d.WaitReady(ctx, e)
}

func (d *demo) Delete(ctx context.Context, e *steps.Env) error {
	if err := e.RequireKind(d.DemoName); err != nil {
		return err
	}
	log.Step(d.DemoName + "_delete")

	if err := e.DeleteDemoActors(ctx, d.ActorTemplates...); err != nil {
		return err
	}
	// The HPA goes first so it cannot scale the pool back up while the
	// workload is being removed.
	if err := e.Kube.DeletePath(ctx, e.Cfg.Path(hpaKindManifest)); err != nil {
		return err
	}
	if err := e.Kube.DeletePath(ctx, e.Cfg.Path(prometheusAdapterManifest)); err != nil {
		return err
	}

	manifest, err := demos.Render(e, d.Template, nil, demos.ExternalVolumePlaceholders)
	if err != nil {
		return err
	}
	return e.Kube.DeleteBytes(ctx, manifest)
}
