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

// Package benchmarking is the contract gate for the load-test workflow.
// It deploys the real benchmarking/workloads manifest and drives one
// boomer VU's full lifecycle via glutton.ExerciseBenchmark, so a change
// to the ATE API surface or the WorkerPool/ActorTemplate manifest schema
// that would silently break benchmarking fails here.
package benchmarking

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/glutton"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	workloadsNamespace = "benchmark-workloads"
	gluttonTemplate    = "glutton"
)

// TestGluttonBenchmarkContract helps ensure that changes will not disrupt benchmarking
func TestGluttonBenchmarkContract(t *testing.T) {
	// Fail fast if the env the workloads manifest depends on is missing —
	// benchmarking/workloads/deploy.sh sources this from .ate-dev-env.sh
	// and substitutes it into the ActorTemplate's snapshotsConfig.location.
	if _, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	ctx := context.Background()
	clients := e2e.GetClients()

	deployWorkloads(t)
	waitForGluttonTemplateReady(ctx, t, clients)

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	// 200ms between RPCs gives the atenet router time to pick up the
	// Resume before Ping is routed.
	if err := glutton.ExerciseBenchmark(ctx, clients.SubstrateAPI, &http.Client{Timeout: 30 * time.Second}, rc.BaseURL(), 200*time.Millisecond); err != nil {
		t.Fatalf("glutton.ExerciseBenchmark: %v", err)
	}
}

// deployWorkloads applies benchmarking/workloads/manifests/workloads.yaml.tmpl
// via its own deploy.sh so any drift in the template or the script (which
// orchestrator.py also invokes) is caught here. Registers a Cleanup to
// tear down the workloads.
func deployWorkloads(t *testing.T) {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	deploy := filepath.Join(root, "benchmarking/workloads/deploy.sh")

	t.Logf("Deploying workloads via %s --deploy", deploy)
	e2e.RunCmd(t, deploy, "--deploy", "--worker-count", "1")

	t.Cleanup(func() {
		t.Logf("Tearing down workloads via %s --delete", deploy)
		e2e.RunCmd(t, deploy, "--delete")
	})
}

// waitForGluttonTemplateReady polls the ActorTemplate until it reaches
// PhaseReady (golden snapshot has been created). Mirrors the wait in
// orchestrator.py after deploy_workloads.
func waitForGluttonTemplateReady(ctx context.Context, t *testing.T, clients *e2e.Clients) {
	t.Helper()
	timeout := 5 * time.Minute
	deadline := time.Now().Add(timeout)
	var lastPhase v1alpha1.PhaseType
	for time.Now().Before(deadline) {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(workloadsNamespace).Get(ctx, gluttonTemplate, metav1.GetOptions{})
		if err == nil {
			lastPhase = at.Status.Phase
			if lastPhase == v1alpha1.PhaseReady {
				t.Logf("ActorTemplate %s/%s is Ready (golden=%q)", workloadsNamespace, gluttonTemplate, at.Status.GoldenSnapshot)
				return
			}
			if lastPhase == v1alpha1.PhaseFailed {
				t.Fatalf("ActorTemplate %s/%s entered PhaseFailed", workloadsNamespace, gluttonTemplate)
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out after %v waiting for ActorTemplate %s/%s (last phase: %s)", timeout, workloadsNamespace, gluttonTemplate, lastPhase)
}
