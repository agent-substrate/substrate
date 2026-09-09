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
	"github.com/agent-substrate/substrate/internal/resources"
)

// namespace holds the demo workload, its prometheus-adapter, and the HPA;
// it doubles as the atespace holding the demo's ActorTemplate.
const namespace = "ate-demo-autoscaled-workerpool"

// Add-ons that only make sense on Kind.
const (
	prometheusAdapterManifest = "demos/autoscaled-workerpool/prometheus-adapter.yaml"
	hpaKindManifest           = "demos/autoscaled-workerpool/hpa-kind.yaml"
)

type demo struct {
	demos.Substrate
}

func init() {
	demos.Register(&demo{Substrate: demos.Substrate{
		DemoName:           "demo-autoscaled-workerpool",
		Short:              "A WorkerPool scaled by an HPA over custom metrics (Kind only)",
		WorkerPoolManifest: "demos/autoscaled-workerpool/autoscaled-workerpool.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "counter"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/autoscaled-workerpool/autoscaled-workerpool-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "counter"},
		}},
	}})
}

func (d *demo) KindOnly() bool { return true }

func (d *demo) Deploy(ctx context.Context, e *steps.Env) error {
	if err := e.RequireKind(d.DemoName); err != nil {
		return err
	}
	if err := d.Substrate.Deploy(ctx, e); err != nil {
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
	return e.Kube.ApplyPath(ctx, e.Cfg.Path(hpaKindManifest))
}

func (d *demo) Delete(ctx context.Context, e *steps.Env) error {
	if err := e.RequireKind(d.DemoName); err != nil {
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
	return d.Substrate.Delete(ctx, e)
}
