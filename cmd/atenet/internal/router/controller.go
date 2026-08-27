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
)

// Controller monitors ActorTemplates and coordinates configuration updates for
// the ingress Envoy's xDS server. It is part of the ingress control plane and
// only runs in a mode that serves ingress — the egress Envoy is statically
// configured and has no templates to watch.
type Controller struct {
	xdsSrv *XdsServer

	atStore atStore
}

func NewController(
	store atStore,
	xdsSrv *XdsServer,
) *Controller {
	return &Controller{
		xdsSrv:  xdsSrv,
		atStore: store,
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

	return nil
}
