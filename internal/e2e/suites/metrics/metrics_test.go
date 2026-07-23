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

// Package metrics is an e2e suite that drives an actor lifecycle and then asserts
// the platform metrics in e2e.PlatformMetricPrefixes reach the kind stack's OTel
// Collector. It closes the "silent regression" gap: a renamed or dropped
// instrument fails here rather than surfacing as an empty dashboard. The prefix
// set grows as each metric slice lands. Requires the demo counter template to be
// installed (override with E2E_TEMPLATE_NAMESPACE / _NAME).
package metrics

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const metricsAtespace = "ate-metrics-e2e"

func templateRef() (namespace, name string) {
	namespace, name = "ate-demo-counter", "counter"
	if v := os.Getenv("E2E_TEMPLATE_NAMESPACE"); v != "" {
		namespace = v
	}
	if v := os.Getenv("E2E_TEMPLATE_NAME"); v != "" {
		name = v
	}
	return namespace, name
}

func TestPlatformMetricsEmitted(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	tmplNS, tmplName := templateRef()
	actorID := "metrics-probe"

	// CreateActor requires the atespace to exist first; ignore AlreadyExists.
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: metricsAtespace}},
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: metricsAtespace, Name: actorID},
		ActorTemplateNamespace: tmplNS,
		ActorTemplateName:      tmplName,
	}}); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: metricsAtespace, Name: actorID}})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: metricsAtespace, Name: actorID}})
	})

	// Resume so the pool has an assigned worker, which the ateapi worker-count
	// observable reports. Later metric slices extend e2e.PlatformMetricPrefixes;
	// they add the drive steps their instruments need.
	resume(t, ctx, clients, actorID)

	deadline := time.Now().Add(2 * time.Minute)
	var missing []string
	for time.Now().Before(deadline) {
		scrape, err := e2e.ScrapeCollectorMetrics(ctx)
		if err != nil {
			t.Fatalf("ScrapeCollectorMetrics: %v", err)
		}
		if missing = e2e.MissingPlatformMetrics(scrape, e2e.PlatformMetricPrefixes); len(missing) == 0 {
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("platform metrics never reached the collector: missing %v", missing)
}

func resume(t *testing.T, ctx context.Context, clients *e2e.Clients, actorID string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: metricsAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	waitForStatus(t, ctx, clients, actorID, ateapipb.Actor_STATUS_RUNNING)
}

func waitForStatus(t *testing.T, ctx context.Context, clients *e2e.Clients, actorID string, want ateapipb.Actor_Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: metricsAtespace, Name: actorID},
		})
		if err == nil && resp.GetStatus() == want {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("actor %q never reached %v", actorID, want)
}
