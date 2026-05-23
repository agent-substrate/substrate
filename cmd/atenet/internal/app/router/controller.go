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

package router

import (
	"context"
	"log/slog"
	"time"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Controller monitors ActorTemplates and coordinates configuration updates
// for the Envoy xDS and external processing servers.
type Controller struct {
	k8sClient  client.Client
	clientset  kubernetes.Interface
	cfg        RouterConfig
	xdsSrv     *XdsServer
	extprocSrv *ExtProcServer

	atStore     atStore
	envoyRunner *envoyrunner
}

func NewController(
	k8sClient client.Client,
	clientset kubernetes.Interface,
	cfg RouterConfig,
	xdsSrv *XdsServer,
	extprocSrv *ExtProcServer,
) *Controller {
	xdsSrv.SetConfig(cfg.HttpPort, cfg.ExtprocPort, cfg.ExtprocAddr)

	var store atStore
	if cfg.TemplatesFile != "" {
		store = newFileATStore(cfg.TemplatesFile)
	} else {
		store = newk8sATStore(k8sClient)
	}

	return &Controller{
		k8sClient:  k8sClient,
		clientset:  clientset,
		cfg:        cfg,
		xdsSrv:     xdsSrv,
		extprocSrv: extprocSrv,

		atStore:     store,
		envoyRunner: newEnvoyRunner(k8sClient, cfg),
	}
}

func (c *Controller) Start(ctx context.Context) error {
	// Run first reconcile eagerly on startup
	if err := c.reconcile(ctx); err != nil {
		slog.ErrorContext(ctx, "Error during initial eager router reconciliation", slog.String("err", err.Error()))
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.reconcile(ctx); err != nil {
				slog.ErrorContext(ctx, "Error during router reconciliation", slog.String("err", err.Error()))
			}
		}
	}
}

func (c *Controller) reconcile(ctx context.Context) error {
	_, err := c.atStore.readyTemplates(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get ActorTemplates", slog.String("err", err.Error()))
		return err
	}

	if err := c.xdsSrv.UpdateSnapshot(); err != nil {
		slog.ErrorContext(ctx, "xDS Configuration generation problem", slog.String("err", err.Error()))
		return err
	}

	if !c.cfg.Standalone && c.cfg.TemplatesFile == "" {
		// Reconcile Envoy router Deployment and Kubernetes cluster entities
		err := c.envoyRunner.reconcile(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Error during Envoy router reconciliation", slog.String("err", err.Error()))
			return err
		}
	}

	return nil
}
