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
	"fmt"
	"os"

	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	actorID := fmt.Sprintf("metrics-probe-%d", time.Now().UnixNano())

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

	// Drive request through the router so Envoy ext_proc emits atenet_router_route_duration.
	rClient, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rClient.Close()
	resp, err := rClient.Get(ctx, resources.ActorRef{Atespace: metricsAtespace, Name: actorID}, "/")
	if err != nil {
		t.Fatalf("rClient.Get: %v", err)
	}
	_ = resp.Body.Close()

	// Trigger an actor crash to verify ate_actor_crashes counter emission.
	triggerActorCrash(t, ctx, clients, actorID)

	deadline := time.Now().Add(2 * time.Minute)
	var missing []string
	var ateomSeen bool
	var lastLabelErr error
	for time.Now().Before(deadline) {
		scrape, err := e2e.ScrapeCollectorMetrics(ctx)
		if err != nil {
			t.Fatalf("ScrapeCollectorMetrics: %v", err)
		}
		missing = e2e.MissingPlatformMetrics(scrape, e2e.PlatformMetricPrefixes)
		ateomSeen = e2e.CollectorHasService(scrape, "ateom-gvisor", "ateom-microvm")
		if len(missing) == 0 && ateomSeen {
			// Verify ate_actor_crashes metric carries valid, non-empty low-cardinality labels for all attributes.
			foundCrashLine := false
			for _, line := range strings.Split(scrape, "\n") {
				if strings.HasPrefix(line, "ate_actor_crashes") {
					foundCrashLine = true
					opVal := extractPrometheusLabelValue(line, "ate_actor_operation_name")
					reasonVal := extractPrometheusLabelValue(line, "ate_failure_reason")
					tmplNSVal := extractPrometheusLabelValue(line, "ate_template_namespace")
					tmplNameVal := extractPrometheusLabelValue(line, "ate_template_name")
					workerPoolVal := extractPrometheusLabelValue(line, "ate_workerpool_name")
					sandboxVal := extractPrometheusLabelValue(line, "ate_sandbox_class")

					var errs []string
					if opVal == "" {
						errs = append(errs, "ate_actor_operation_name label is missing or empty")
					} else if ateattr.NormalizeOperationName(opVal) != opVal {
						errs = append(errs, fmt.Sprintf("ate_actor_operation_name %q is invalid (must be one of {create, resume, suspend, pause, delete, unknown})", opVal))
					}

					if reasonVal == "" {
						errs = append(errs, "ate_failure_reason label is missing or empty")
					} else if !ateerrors.IsValidReason(reasonVal) {
						errs = append(errs, fmt.Sprintf("ate_failure_reason %q is invalid (must be a registered ateerrors reason enum like CORRUPTED_ASSIGNMENT, WORKER_POD_GONE, WORKER_REASSIGNED, UNKNOWN)", reasonVal))
					}

					if tmplNSVal == "" {
						errs = append(errs, "ate_template_namespace label is missing or empty")
					}
					if tmplNameVal == "" {
						errs = append(errs, "ate_template_name label is missing or empty")
					}
					if workerPoolVal == "" {
						errs = append(errs, "ate_workerpool_name label is missing or empty")
					}
					if sandboxVal == "" {
						errs = append(errs, "ate_sandbox_class label is missing or empty")
					}

					if len(errs) == 0 {
						return
					}
					lastLabelErr = fmt.Errorf("scraped metric line %q failed label validation:\n  - %s\n  (Extracted labels: op=%q, reason=%q, tmplNS=%q, tmplName=%q, workerPool=%q, sandboxClass=%q)",
						line, strings.Join(errs, "\n  - "), opVal, reasonVal, tmplNSVal, tmplNameVal, workerPoolVal, sandboxVal)
				}
			}
			if !foundCrashLine {
				lastLabelErr = fmt.Errorf("ate_actor_crashes metric line not found in collector scrape output")
			}
		}

		time.Sleep(3 * time.Second)
	}

	if lastLabelErr != nil {
		t.Fatalf("platform telemetry validation failed: missing metrics %v, ateom pushed=%v, error detail: %v", missing, ateomSeen, lastLabelErr)
	}
	t.Fatalf("platform telemetry never reached the collector: missing metrics %v, ateom pushed=%v", missing, ateomSeen)
}

func triggerActorCrash(t *testing.T, ctx context.Context, clients *e2e.Clients, actorID string) {
	t.Helper()

	// Get running actor to find its assigned worker pod.
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: metricsAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	// Delete the assigned worker pod directly.
	// The WorkerPoolSyncer will detect the pod is gone, crash the actor via syncer.go,
	// and emit the ate_actor_crashes counter.
	if ass := actor.GetWorkerAssignment(); ass != nil && ass.GetWorkerPod() != "" {
		podName := ass.GetWorkerPod()
		podNS := ass.GetWorkerNamespace()
		if err := clients.K8s.CoreV1().Pods(podNS).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("Delete worker pod failed: %v", err)
		}
	} else {
		t.Fatalf("Actor %s has no assigned worker pod to delete", actorID)
	}

	waitForStatus(t, ctx, clients, actorID, ateapipb.Actor_STATUS_CRASHED)
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

func extractPrometheusLabelValue(line, labelName string) string {
	key := labelName + `="`
	idx := strings.Index(line, key)
	if idx == -1 {
		return ""
	}
	start := idx + len(key)
	end := strings.IndexByte(line[start:], '"')
	if end == -1 {
		return ""
	}
	return line[start : start+end]
}
