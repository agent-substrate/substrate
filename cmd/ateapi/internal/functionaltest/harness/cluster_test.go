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

package harness

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/installdefaults"
)

func TestWorker(t *testing.T) {
	t.Parallel()
	h := New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	if again := h.Node("a"); again != nodeA {
		t.Errorf("Node(a) returned %p the second time, want the first node %p", again, nodeA)
	}

	w := h.AddWorker(pool, nodeA, WithActorCapacity(3), WithResourceCapacity("2", "4Gi"))
	want := &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: w.Name},
		WorkerNamespace: w.Namespace,
		WorkerPool:      "pool1",
		WorkerPod:       w.Pod,
		WorkerPodUid:    w.Name,
		NodeName:        nodeA.Name,
		Labels:          map[string]string{"pool": "pool1"},
		Status: &ateapi.WorkerStatus{
			State:           ateapi.WorkerState_WORKER_STATE_ACTIVE,
			Capacity:        &ateapi.WorkerResources{Actors: 3, CpuMillicores: 2000, MemoryBytes: 4 << 30},
			Ips:             []string{w.IP},
			IssuedEpoch:     1,
			RegisteredEpoch: 1,
			ObservedEpoch:   1,
			SandboxClasses: []*ateapi.WorkerSandboxClass{{
				SandboxClass:   "gvisor",
				DefaultRuntime: &ateapi.WorkerSandboxRuntime{Version: SandboxRuntime().GetVersion()},
			}},
		},
	}
	ignoreServerMetadata := protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "uid", "version", "create_time", "update_time")
	if diff := cmp.Diff(want, h.GetWorker(w), protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("registered worker (-want +got):\n%s", diff)
	}

	other := h.AddWorker(pool, h.Node("b"))
	wantDefault := &ateapi.WorkerResources{Actors: 1, CpuMillicores: TemplateCPUMillicores, MemoryBytes: TemplateMemoryBytes}
	if diff := cmp.Diff(wantDefault, h.GetWorker(other).GetStatus().GetCapacity(), protocmp.Transform()); diff != "" {
		t.Errorf("default capacity (-want +got):\n%s", diff)
	}

	actor := h.NewActor(as, h.NewTemplate(as, pool))
	w.Drain()
	other.Remove()
	var err error
	h.Eventually("ResumeActor to find no worker", func() bool {
		_, err = h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ActorRef(actor)})
		return status.Code(err) == codes.ResourceExhausted
	})
}

func TestNode(t *testing.T) {
	t.Parallel()
	h := New(t)
	n := h.Node("a")
	if _, err := h.k8s.CoreV1().Nodes().Get(t.Context(), n.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("getting node %s: %v", n.Name, err)
	}

	n.StopAtelet()
	if _, err := h.dialAtelet(t.Context(), n.ateletAddr); !errors.Is(err, errNoAtelet) {
		t.Errorf("dialing the stopped atelet = %v, want %v", err, errNoAtelet)
	}
	if _, err := h.k8s.CoreV1().Pods(installdefaults.SystemNamespace).Get(t.Context(), n.ateletPod, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("getting the stopped atelet's pod = %v, want NotFound", err)
	}
	if _, err := h.k8s.CoreV1().Nodes().Get(t.Context(), n.Name, metav1.GetOptions{}); err != nil {
		t.Errorf("getting node %s after its atelet stopped: %v", n.Name, err)
	}

	n.Remove()
	if _, err := h.k8s.CoreV1().Nodes().Get(t.Context(), n.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("getting removed node %s = %v, want NotFound", n.Name, err)
	}
}

func TestWorkerSandboxRuntime(t *testing.T) {
	t.Parallel()
	h := New(t)
	rtA := SandboxRuntime()
	rtB := proto.CloneOf(rtA)
	rtB.Version.Attributes[1].Value = "hash-b"
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), h.Node("a"), WithSandboxRuntime(rtB, rtA))
	want := []*ateapi.WorkerSandboxClass{{
		SandboxClass:       "gvisor",
		DefaultRuntime:     &ateapi.WorkerSandboxRuntime{Version: rtB.GetVersion()},
		RestorableRuntimes: []*ateapi.WorkerSandboxRuntime{{Version: rtA.GetVersion()}},
	}}
	if diff := cmp.Diff(want, h.GetWorker(w).GetStatus().GetSandboxClasses(), protocmp.Transform()); diff != "" {
		t.Errorf("registered sandbox classes (-want +got):\n%s", diff)
	}

	w.SetSandboxRuntime(rtA)
	want = []*ateapi.WorkerSandboxClass{{SandboxClass: "gvisor", DefaultRuntime: &ateapi.WorkerSandboxRuntime{Version: rtA.GetVersion()}}}
	if diff := cmp.Diff(want, h.GetWorker(w).GetStatus().GetSandboxClasses(), protocmp.Transform()); diff != "" {
		t.Errorf("sandbox classes after the change (-want +got):\n%s", diff)
	}
}

func TestNodeFinishUpload(t *testing.T) {
	t.Parallel()
	h := New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.AsyncSuspendedActor(as, h.NewTemplate(as, pool))
	snaps := actor.GetStatus().GetSnapshots()
	local := snaps[len(snaps)-1]

	node.FinishUpload(actor)
	got := h.GetActor(ActorRef(actor)).GetStatus().GetSnapshots()
	if state := got[len(got)-1].GetDurableSnapshot().GetState(); state != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
		t.Errorf("snapshot after the upload is %v, want COMPLETED", state)
	}
	if names, err := h.ObjectStore.Snapshot(local.GetDurableSnapshot().GetObject().GetSnapshotUri()); err != nil || len(names) == 0 {
		t.Errorf("uploaded snapshot in object storage = %v, %v; want it present", names, err)
	}
	if uuid := node.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()); uuid != local.GetUuid() {
		t.Errorf("local snapshot after the upload = %q, want %q kept", uuid, local.GetUuid())
	}
}

func TestWorkerRestart(t *testing.T) {
	t.Parallel()
	h := New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	h.RunningActor(as, h.NewTemplate(as, pool))
	before := w.Epoch

	w.Restart()
	if w.Epoch <= before {
		t.Errorf("epoch after restart = %d, want above %d", w.Epoch, before)
	}
	if got := h.GetWorker(w).GetStatus().GetRegisteredEpoch(); got != w.Epoch {
		t.Errorf("registered epoch = %d, want %d", got, w.Epoch)
	}
	if diff := cmp.Diff([]Sandbox{}, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after restart (-want +got):\n%s", diff)
	}
}

func TestNodeDetachSnapshot(t *testing.T) {
	t.Parallel()
	h := New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.AsyncSuspendedActor(as, h.NewTemplate(as, pool))
	snaps := actor.GetStatus().GetSnapshots()
	local := snaps[len(snaps)-1]

	node.DetachSnapshot(actor)
	got := h.GetActor(ActorRef(actor)).GetStatus().GetSnapshots()
	detached := got[len(got)-1]
	if detached.GetDurableSnapshot().GetState() != ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED || detached.GetLocality() != "" {
		t.Errorf("snapshot after the detach is %v local to %q, want COMPLETED and not local", detached.GetDurableSnapshot().GetState(), detached.GetLocality())
	}
	if names, err := h.ObjectStore.Snapshot(local.GetDurableSnapshot().GetObject().GetSnapshotUri()); err != nil || len(names) == 0 {
		t.Errorf("detached snapshot in object storage = %v, %v; want it present", names, err)
	}
	if uuid := node.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()); uuid != "" {
		t.Errorf("local snapshot after the detach = %q, want none", uuid)
	}
}
