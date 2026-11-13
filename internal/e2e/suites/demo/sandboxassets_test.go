// Copyright 2026 The Agent Substrate Authors
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

package demo

// What atelet does with a SandboxConfig version whose assets it cannot
// fetch.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/internal/hardware"
	"google.golang.org/protobuf/testing/protocmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestBadVersionSHA covers a SandboxConfig version whose asset SHA256 is
// wrong: atelet leaves it out and keeps the worker on what it last
// registered, until the version is fixed.
//  1. An actor boots on v1, increments its counters twice (memory=2, file=2,
//     rootfs=2), and suspends with a v1-stamped MEMORY snapshot.
//  2. v2 is added with a wrong SHA256 while v1 stays the default: atelet
//     records a SandboxRuntimeSyncFailed Event on the worker pod naming the
//     incomplete version, and the worker advertises v1 alone.
//  3. defaultVersion moves to the broken v2: atelet records the Event for
//     v2 itself and the worker still advertises v1, so the actor restores
//     MEMORY on v1 (memory=3, file=3, rootfs=3).
//  4. v2's SHA256 is fixed: the worker converges to v2 as default_runtime
//     with v1 restorable.
func TestBadVersionSHA(t *testing.T) {
	t.Parallel()
	f := setupSuspendedV1Actor(t, "bad-sha")

	// 2. Add v2 with a wrong SHA256 as a restorable version. Only assets
	// that differ from v1 get a wrong SHA256, so any assets shared with v1
	// stay cache hits.
	v2Bad := *f.v2.DeepCopy()
	for arch, files := range v2Bad.Assets {
		for name, entry := range files {
			if entry == f.v1.Assets[arch][name] {
				continue
			}
			entry.SHA256 = strings.Repeat("0", 64)
			files[name] = entry
		}
	}
	f.updateVersions(t, f.v1.Name, []atev1.SandboxVersionConfig{f.v1, v2Bad})
	versionNote := fmt.Sprintf("SandboxConfig %q, version %q:", f.configName, v2Bad.Name)
	f.waitForSyncFailureEvent(t, "incomplete sandbox runtimes: "+versionNote)
	worker, err := f.clients.SubstrateAPI.GetWorker(f.ctx, &ateapi.GetWorkerRequest{Worker: f.workerRef})
	if err != nil {
		t.Fatalf("failed to get Worker %s after the broken restorable version: %v", f.workerRef.GetName(), err)
	}
	if diff := cmp.Diff(f.v1Runtime, hardware.ColdBootRuntime(worker.GetStatus()), protocmp.Transform()); diff != "" {
		t.Fatalf("worker default_runtime changed after the broken restorable version (-want v1 +got):\n%s", diff)
	}
	if classes := worker.GetStatus().GetSandboxClasses(); len(classes) != 1 || len(classes[0].GetRestorableRuntimes()) != 0 {
		t.Fatalf("worker advertised the broken restorable version: %v", classes)
	}

	// 3. Make the broken v2 the default. The Event names v2 directly, without
	// the prefix a restorable failure carries.
	f.updateVersions(t, v2Bad.Name, []atev1.SandboxVersionConfig{f.v1, v2Bad})
	f.waitForSyncFailureEvent(t, versionNote)
	worker, err = f.clients.SubstrateAPI.GetWorker(f.ctx, &ateapi.GetWorkerRequest{Worker: f.workerRef})
	if err != nil {
		t.Fatalf("failed to get Worker %s after the broken default version: %v", f.workerRef.GetName(), err)
	}
	if diff := cmp.Diff(f.v1Runtime, hardware.ColdBootRuntime(worker.GetStatus()), protocmp.Transform()); diff != "" {
		t.Fatalf("worker default_runtime changed after the broken default version (-want v1 +got):\n%s", diff)
	}
	f.resumeAndCheck(t, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, f.v1Runtime)

	// 4. Fix v2's SHA256.
	f.updateVersions(t, f.v2.Name, []atev1.SandboxVersionConfig{f.v1, f.v2})
	f.waitForWorker(t, func(status *ateapi.WorkerStatus) bool {
		defHash := runtimeAttribute(hardware.ColdBootRuntime(status), hardware.AttrSandboxBundleHash)
		return defHash != "" && defHash != f.v1Hash && hardware.WorkerMatches(status, f.v1Runtime)
	})
}

// waitForSyncFailureEvent polls until atelet has recorded a Warning
// SandboxRuntimeSyncFailed Event on the fixture worker's pod, related to the
// fixture's SandboxConfig, whose note starts with wantNote. Events are
// recorded asynchronously, so one can trail the sync attempt it reports.
func (f *sandboxRuntimeFixture) waitForSyncFailureEvent(t *testing.T, wantNote string) {
	t.Helper()
	w, err := f.clients.SubstrateAPI.GetWorker(f.ctx, &ateapi.GetWorkerRequest{Worker: f.workerRef})
	if err != nil {
		t.Fatalf("failed to get Worker %s: %v", f.workerRef.GetName(), err)
	}
	podUID := types.UID(w.GetWorkerPodUid())

	const timeout = 2 * time.Minute
	ctx, cancel := context.WithTimeout(f.ctx, timeout)
	defer cancel()

	var seen []string
	for {
		list, err := f.clients.K8s.EventsV1().Events(w.GetWorkerNamespace()).List(ctx, metav1.ListOptions{})
		if err == nil {
			seen = seen[:0]
			for _, ev := range list.Items {
				if ev.Regarding.UID != podUID || ev.Reason != "SandboxRuntimeSyncFailed" {
					continue
				}
				if ev.Type == "Warning" && ev.Related != nil && ev.Related.Kind == "SandboxConfig" && ev.Related.Name == f.configName && strings.HasPrefix(ev.Note, wantNote) {
					return
				}
				seen = append(seen, ev.Type+" "+ev.Reason+" "+ev.Note)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out after %v waiting for a SandboxRuntimeSyncFailed Event on worker pod %s related to SandboxConfig %s with note %q (seen: %q, err=%v)",
				timeout, podUID, f.configName, wantNote, seen, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
