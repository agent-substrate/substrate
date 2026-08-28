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

package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WaitForTemplateReady blocks until the ActorTemplate's golden actor has
// booted and been snapshotted. The default 5 minute timeout can be
// overridden with E2E_TEMPLATE_READY_TIMEOUT.
func WaitForTemplateReady(ctx context.Context, t *testing.T, clients *Clients, namespace, name string) {
	t.Helper()

	timeout := 5 * time.Minute
	if v := os.Getenv("E2E_TEMPLATE_READY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("invalid E2E_TEMPLATE_READY_TIMEOUT %q: %v", v, err)
		}
		timeout = d
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastPhase v1alpha1.PhaseType
	for {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			lastPhase = at.Status.Phase
			if lastPhase == v1alpha1.PhaseReady {
				return
			}
			if lastPhase == v1alpha1.PhaseFailed {
				t.Fatalf("ActorTemplate %s/%s transitioned to Failed", namespace, name)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out after %v waiting for ActorTemplate %s/%s to be Ready (last phase %q, err %v)", timeout, namespace, name, lastPhase, err)
		case <-time.After(time.Second):
		}
	}
}

// TemplateRef builds the substrate template reference an Actor carries.
func TemplateRef(at *ateapipb.ActorTemplate) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: at.GetMetadata().GetAtespace(), Name: at.GetMetadata().GetName()}
}

// SubstrateCounterTemplateOptions shapes CreateSubstrateCounterTemplate.
type SubstrateCounterTemplateOptions struct {
	// Atespace and Name locate the new template. The atespace is created if
	// missing; Name must be unique within it (atespaces are shared across a
	// suite's tests, unlike the k8s namespaces the CRD templates lived in).
	Atespace string
	Name     string
	// PoolName and PoolReplicas shape the WorkerPool CRD created in the test's
	// k8s namespace.
	PoolName     string
	PoolReplicas int32
	// Labels tie the template's workerSelector to the pool, keeping this
	// pool's workers invisible to other namespaces' actors.
	Labels map[string]string
	// SnapshotsConfig for the new template; nil copies the source's.
	SnapshotsConfig *ateapipb.SnapshotsConfig
	// Modify, when set, edits the template before it is created.
	Modify func(*ateapipb.ActorTemplate)
}

// CreateSubstrateCounterTemplate creates a per-test WorkerPool CRD plus a
// substrate ActorTemplate copying the resolved runtime (sandbox config, ateom
// image, container images, sandbox size) from the substrate counter demo for
// the sandbox class under test. It registers cleanup of the template (which
// does not ride the k8s namespace GC the CRD templates did) and blocks until
// the golden snapshot exists.
func CreateSubstrateCounterTemplate(ctx context.Context, t *testing.T, clients *Clients, namespace string, opts SubstrateCounterTemplateOptions) *ateapipb.ActorTemplate {
	t.Helper()

	src := SubstrateCounterFixture()

	existingWp, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(src.PoolNamespace).Get(ctx, src.PoolName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s (deploy with: %s): %v", src.PoolNamespace, src.PoolName, src.DeployWith, err)
	}
	srcTmpl, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{Atespace: src.Atespace, Name: src.Name},
	})
	if err != nil {
		t.Fatalf("failed to get ActorTemplate %s/%s (deploy with: %s): %v", src.Atespace, src.Name, src.DeployWith, err)
	}

	wp := &v1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.PoolName,
			Namespace: namespace,
			Labels:    opts.Labels,
		},
		Spec: v1alpha1.WorkerPoolSpec{
			Replicas:          opts.PoolReplicas,
			WorkerImage:       existingWp.Spec.WorkerImage,
			SandboxClass:      existingWp.Spec.SandboxClass,
			SandboxConfigName: existingWp.Spec.SandboxConfigName,
		},
	}
	if _, err := clients.SubstrateK8s.ApiV1alpha1().WorkerPools(namespace).Create(ctx, wp, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	// CreateActorTemplate requires the atespace to exist first.
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: opts.Atespace}}}); err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("failed to create atespace %q: %v", opts.Atespace, err)
	}

	snapshots := opts.SnapshotsConfig
	if snapshots == nil {
		snapshots = srcTmpl.GetSnapshotsConfig()
	}
	tmpl := &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: opts.Atespace, Name: opts.Name},
		WorkerSelector: &ateapipb.Selector{MatchLabels: opts.Labels},
		Containers:     srcTmpl.GetContainers(),
		// The source's limits size the sandbox. Copying them matters most on
		// micro-VM, where an ActorTemplate that declares none boots the guest
		// at the kata config default (2GiB) instead of the demo's 512Mi.
		Resources: srcTmpl.GetResources(),
		// Both sandbox_class and config_name are required; the source carries
		// the pair for the class under test.
		SandboxConfig:   srcTmpl.GetSandboxConfig(),
		SnapshotsConfig: snapshots,
		Volumes:         srcTmpl.GetVolumes(),
	}
	if opts.Modify != nil {
		opts.Modify(tmpl)
	}
	created, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl})
	if err != nil {
		t.Fatalf("failed to create ActorTemplate %s/%s: %v", opts.Atespace, opts.Name, err)
	}
	// Registered before the golden wait so a template whose golden never
	// builds still gets cleaned up; the actors are gone by then (test-body
	// defers run before cleanups).
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := clients.SubstrateAPI.DeleteActorTemplate(cleanupCtx, &ateapipb.DeleteActorTemplateRequest{
			ActorTemplate: &ateapipb.ObjectRef{Atespace: opts.Atespace, Name: opts.Name},
		}); err != nil && status.Code(err) != codes.NotFound {
			t.Logf("failed to delete ActorTemplate %s/%s: %v", opts.Atespace, opts.Name, err)
		}
	})

	// The timeout budgets for the micro-VM golden (a CH cold boot plus
	// checkpoint on nested KVM) being slower than the gVisor one.
	t.Logf("Waiting for ActorTemplate %s/%s golden snapshot...", opts.Atespace, opts.Name)
	WaitForSubstrateTemplateReady(ctx, t, clients, opts.Atespace, opts.Name)

	return created
}

// WaitForSubstrateTemplateReady blocks until the substrate ActorTemplate's
// golden snapshot exists, the same readiness the CRD phase poll above proves.
// The timeout follows the sandbox class under test (see TemplateReadyTimeout).
func WaitForSubstrateTemplateReady(ctx context.Context, t *testing.T, clients *Clients, atespace, name string) {
	t.Helper()

	timeout := TemplateReadyTimeout(t)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastStatus *ateapipb.GoldenSnapshotStatus
	for {
		at, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapipb.GetActorTemplateRequest{
			ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		})
		if err == nil {
			lastStatus = at.GetStatus().GetGoldenSnapshotStatus()
			if lastStatus.GetGoldenSnapshot() != nil {
				return
			}
			if msg := lastStatus.GetErrorMessage(); msg != "" {
				t.Fatalf("ActorTemplate %s/%s failed to build its golden snapshot: %s", atespace, name, msg)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out after %v waiting for ActorTemplate %s/%s golden snapshot (last status %v, err %v)", timeout, atespace, name, lastStatus, err)
		case <-time.After(time.Second):
		}
	}
}
