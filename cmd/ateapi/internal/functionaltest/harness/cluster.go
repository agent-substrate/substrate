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
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/atelet"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// ipCount hands out pod IPs unique within the process. Nothing routes to them:
// ate-api's dialer resolves an atelet's IP and the harness serves the
// connection in memory.
var ipCount atomic.Uint32

func nextPodIP() string {
	n := ipCount.Add(1)
	return fmt.Sprintf("10.%d.%d.%d", byte(n>>16), byte(n>>8), byte(n))
}

// Node is a k8s node with an atelet on it.
type Node struct {
	// Name is the node's name in the cluster, unique to the test.
	Name string
	// Atelet is the node's fake atelet.
	Atelet *Atelet

	// workerService is how the node's atelet reaches ate-api's
	// WorkerService, presenting the atelet's pod identity.
	workerService ateapi.WorkerServiceClient

	h          *Harness
	ateletPod  string
	ateletAddr string
	server     *grpc.Server
}

// WorkerService is a client of ate-api's WorkerService that authenticates as
// the node's atelet, as atelet's calls on behalf of the node's workers do.
func (n *Node) WorkerService() ateapi.WorkerServiceClient {
	return n.workerService
}

// Node returns the node called name in this test, creating it and its atelet
// on first use.
func (h *Harness) Node(name string) *Node {
	h.t.Helper()
	h.mu.Lock()
	n, ok := h.nodesByTestName[name]
	h.mu.Unlock()
	if ok {
		return n
	}

	nodeName := h.namespace + "-" + name
	podName := "atelet-" + nodeName
	ip := nextPodIP()
	addr := net.JoinHostPort(ip, strconv.Itoa(atelet.DefaultPort))
	n = &Node{Name: nodeName, Atelet: newAtelet(h.ObjectStore), h: h, ateletPod: podName, ateletAddr: addr}

	if _, err := h.k8s.CoreV1().Nodes().Create(h.ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}, metav1.CreateOptions{}); err != nil {
		h.t.Fatalf("creating node %s: %v", nodeName, err)
	}
	h.t.Cleanup(func() {
		_ = h.k8s.CoreV1().Nodes().Delete(context.Background(), nodeName, metav1.DeleteOptions{})
	})

	lis := bufconn.Listen(bufSize)
	n.server = grpc.NewServer()
	ateletpb.RegisterAteletServer(n.server, n.Atelet)
	go func() { _ = n.server.Serve(lis) }()
	h.t.Cleanup(n.server.Stop)
	h.mu.Lock()
	h.ateletListenersByAddr[addr] = lis
	h.mu.Unlock()

	pod := h.createRunningPod(installdefaults.SystemNamespace, podName, nodeName, ip, map[string]string{"app": "atelet"})
	h.t.Cleanup(func() {
		_ = h.k8s.CoreV1().Pods(installdefaults.SystemNamespace).Delete(context.Background(), podName, metav1.DeleteOptions{
			GracePeriodSeconds: ptr.To[int64](0),
		})
	})

	cert, err := h.pki.ateletCert(podName, string(pod.UID), nodeName)
	if err != nil {
		h.t.Fatal(err)
	}
	n.workerService = ateapi.NewWorkerServiceClient(h.dialAteAPI(h.pki.clientTLS(cert), preview.DialOptionsWithOptIn()...))

	h.mu.Lock()
	h.nodesByTestName[name] = n
	h.mu.Unlock()
	return n
}

// StopAtelet takes the node's atelet down and deletes its pod, as when atelet
// crashes or is evicted. ate-api's calls to it fail, and once ate-api sees the
// pod deleted it finds no atelet on the node. The node stays in the cluster.
func (n *Node) StopAtelet() {
	n.h.t.Helper()
	n.h.mu.Lock()
	delete(n.h.ateletListenersByAddr, n.ateletAddr)
	n.h.mu.Unlock()
	n.server.Stop()
	if err := n.h.k8s.CoreV1().Pods(installdefaults.SystemNamespace).Delete(n.h.ctx, n.ateletPod, metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To[int64](0),
	}); err != nil && !apierrors.IsNotFound(err) {
		n.h.t.Fatalf("deleting atelet pod %s: %v", n.ateletPod, err)
	}
}

// FinishUpload does what the node's atelet does once the deferred upload of
// actor's local snapshot completes: it writes the snapshot to object storage
// and reports it to ate-api as the actor's latest local and durable snapshot.
func (n *Node) FinishUpload(actor *ateapi.Actor) {
	n.h.t.Helper()
	uuid, err := n.Atelet.upload(actor.GetMetadata().GetUid())
	if err != nil {
		n.h.t.Fatalf("uploading the local snapshot of actor %s: %v", actor.GetMetadata().GetName(), err)
	}
	report := snapshotReport(n.h.t, actor, uuid)
	n.reportSnapshotState(actor, &ateapi.ActorSnapshotState{LatestLocal: report, LatestDurable: report})
}

// DetachSnapshot does what the node's atelet does when it uploads actor's
// local snapshot and drops it from the node on its own, as under disk
// pressure: it reports the snapshot to ate-api as durable and no longer local.
func (n *Node) DetachSnapshot(actor *ateapi.Actor) {
	n.h.t.Helper()
	state, err := n.Atelet.detach(actor.GetMetadata().GetUid())
	if err != nil {
		n.h.t.Fatalf("detaching the local snapshot of actor %s: %v", actor.GetMetadata().GetName(), err)
	}
	uuid := state.GetLatestDurable().GetSnapshot().GetSnapshotUuid()
	n.reportSnapshotState(actor, &ateapi.ActorSnapshotState{LatestDurable: snapshotReport(n.h.t, actor, uuid)})
}

// snapshotReport describes actor's snapshot uuid as atelet reports it.
func snapshotReport(t *testing.T, actor *ateapi.Actor, uuid string) *ateapi.SnapshotReport {
	t.Helper()
	for _, s := range actor.GetStatus().GetSnapshots() {
		if s.GetUuid() == uuid {
			return &ateapi.SnapshotReport{Uuid: uuid, Generation: s.GetGeneration(), Fidelity: s.GetFidelity(), Object: s.GetDurableSnapshot().GetObject()}
		}
	}
	t.Fatalf("actor %s records no snapshot %s", actor.GetMetadata().GetName(), uuid)
	return nil
}

// reportSnapshotState reports actor's snapshot state to ate-api as the node's
// atelet.
func (n *Node) reportSnapshotState(actor *ateapi.Actor, state *ateapi.ActorSnapshotState) {
	n.h.t.Helper()
	if err := RetryAborted(n.h.ctx, func() error {
		_, err := n.WorkerService().ReportActorSnapshotState(n.h.ctx, &ateapi.ReportActorSnapshotStateRequest{
			Actor:    &ateapi.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()},
			ActorUid: actor.GetMetadata().GetUid(),
			State:    state,
		})
		return err
	}); err != nil {
		n.h.t.Fatalf("reporting the snapshot state of actor %s: %v", actor.GetMetadata().GetName(), err)
	}
}

// Remove deletes the node from the cluster, and its atelet with it, as when
// the node is drained and scaled away. Remove the node's workers first.
func (n *Node) Remove() {
	n.h.t.Helper()
	n.StopAtelet()
	if err := n.h.k8s.CoreV1().Nodes().Delete(n.h.ctx, n.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		n.h.t.Fatalf("deleting node %s: %v", n.Name, err)
	}
}

// WorkerPool is the shape a group of workers shares. Templates select a pool
// by its labels.
type WorkerPool struct {
	Name   string
	Labels map[string]string
}

// CreateWorkerPool describes a pool of workers. Without labels, the pool is
// labeled pool=name.
func (h *Harness) CreateWorkerPool(name string, labels map[string]string) *WorkerPool {
	if labels == nil {
		labels = map[string]string{"pool": name}
	}
	return &WorkerPool{Name: name, Labels: labels}
}

// Worker is a worker pod and the Worker ate-api records for it.
type Worker struct {
	// Name is the Worker's name, which is its pod's UID.
	Name string
	// Pod is the worker pod's name.
	Pod string
	// Node is the node the worker runs on.
	Node *Node
	// Pool is the pool the worker belongs to.
	Pool *WorkerPool
	// Namespace is the k8s namespace of the worker pod.
	Namespace string
	// IP is the worker pod's IP.
	IP string
	// Epoch is the epoch the worker process registered with.
	Epoch int64

	capacity *ateapi.WorkerResources

	h *Harness
}

// Ref is the Worker's reference in the Control API.
func (w *Worker) Ref() *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Name: w.Name}
}

// WorkerOption customizes a worker added by AddWorker.
type WorkerOption func(*workerOptions)

type workerOptions struct {
	capacity   *ateapi.WorkerResources
	runtime    *ateapi.ActorSandboxRuntime
	restorable []*ateapi.ActorSandboxRuntime
}

// WithActorCapacity makes the worker report room for n actors. The default
// is one.
func WithActorCapacity(n int32) WorkerOption {
	return func(o *workerOptions) { o.capacity.Actors = n }
}

// WithResourceCapacity makes the worker report room for cpu and memory, given
// as Kubernetes quantities such as "2" and "4Gi". By default a worker reports
// room for as many actors of NewTemplate's size as its actor capacity.
func WithResourceCapacity(cpu, memory string) WorkerOption {
	return func(o *workerOptions) {
		c, m := resource.MustParse(cpu), resource.MustParse(memory)
		o.capacity.CpuMillicores = c.MilliValue()
		o.capacity.MemoryBytes = m.Value()
	}
}

// WithSandboxRuntime makes the worker run actors under rt, and restore
// snapshots taken under any of restorable at full fidelity. By default a
// worker runs SandboxRuntime and restores no other.
func WithSandboxRuntime(rt *ateapi.ActorSandboxRuntime, restorable ...*ateapi.ActorSandboxRuntime) WorkerOption {
	return func(o *workerOptions) { o.runtime, o.restorable = rt, restorable }
}

// SandboxRuntime is the runtime a harness worker runs actors under unless
// WithSandboxRuntime says otherwise, and so the one ate-api stamps on their
// assignments and snapshots.
func SandboxRuntime() *ateapi.ActorSandboxRuntime {
	return &ateapi.ActorSandboxRuntime{
		SandboxClass: "gvisor",
		Version: &ateapi.VersionedSandboxCompat{
			SchemaVersion: hardware.SchemaVersionV1,
			Attributes: []*ateapi.AttributeEntry{
				{Key: hardware.AttrArchitecture, Value: "amd64"},
				{Key: hardware.AttrSandboxBundleHash, Value: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
			},
		},
	}
}

// AddWorker does what the worker syncer and the worker's atelet do for a new
// worker pod: it creates the pod and the Worker, and registers the worker
// process with its IP, capacity and sandbox runtime over WorkerService.
func (h *Harness) AddWorker(pool *WorkerPool, node *Node, opts ...WorkerOption) *Worker {
	h.t.Helper()
	o := workerOptions{capacity: &ateapi.WorkerResources{Actors: 1}, runtime: SandboxRuntime()}
	for _, opt := range opts {
		opt(&o)
	}
	if o.capacity.CpuMillicores == 0 && o.capacity.MemoryBytes == 0 {
		o.capacity.CpuMillicores = int64(o.capacity.Actors) * TemplateCPUMillicores
		o.capacity.MemoryBytes = int64(o.capacity.Actors) * TemplateMemoryBytes
	}

	podName := fmt.Sprintf("%s-worker-%d", pool.Name, h.next())
	ip := nextPodIP()
	pod := h.createRunningPod(h.namespace, podName, node.Name, ip, map[string]string{"ate.dev/worker-pool": pool.Name})
	w := &Worker{Name: string(pod.UID), Pod: podName, Node: node, Pool: pool, Namespace: h.namespace, IP: ip, capacity: o.capacity, h: h}

	if _, err := h.Control.CreateWorker(h.ctx, &ateapi.CreateWorkerRequest{Worker: &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: w.Name},
		WorkerNamespace: h.namespace,
		WorkerPool:      pool.Name,
		WorkerPod:       podName,
		WorkerPodUid:    w.Name,
		NodeName:        node.Name,
		Labels:          pool.Labels,
	}}); err != nil {
		h.t.Fatalf("registering worker %s: %v", podName, err)
	}
	h.mu.Lock()
	h.workersByName[w.Name] = w
	h.mu.Unlock()

	if _, err := h.Control.MarkWorkerAvailable(h.ctx, &ateapi.MarkWorkerAvailableRequest{Worker: w.Ref()}); err != nil {
		h.t.Fatalf("marking worker %s available: %v", podName, err)
	}

	// The worker process registers through its atelet, which retries while
	// ate-api's reconciler holds the Worker.
	ws := node.WorkerService()
	acquired, err := ws.AcquireWorkerEpoch(h.ctx, &ateapi.AcquireWorkerEpochRequest{Worker: w.Ref()})
	if err != nil {
		h.t.Fatalf("acquiring an epoch for worker %s: %v", podName, err)
	}
	epoch := acquired.GetEpoch()
	if err := RetryAborted(h.ctx, func() error {
		_, err := ws.RegisterWorker(h.ctx, &ateapi.RegisterWorkerRequest{
			Worker: w.Ref(), Epoch: epoch, Ips: []string{ip}, Capacity: o.capacity,
		})
		return err
	}); err != nil {
		h.t.Fatalf("registering worker %s: %v", podName, err)
	}
	w.Epoch = epoch
	w.reportSandboxRuntimes(o.runtime, o.restorable)
	node.Atelet.registerWorker(w.Name, epoch)
	// ate-api's reconciler updates the Worker once it observes the epoch; a
	// test's own update of the Worker would race it.
	h.Eventually(fmt.Sprintf("ate-api to observe the epoch of worker %s", podName), func() bool {
		return h.GetWorker(w).GetStatus().GetObservedEpoch() >= epoch
	})
	return w
}

// Drain marks the worker DRAINING, as the worker syncer does when its pod
// starts terminating.
func (w *Worker) Drain() {
	h := w.h
	h.t.Helper()
	if _, err := h.Control.DrainWorker(h.ctx, &ateapi.DrainWorkerRequest{Worker: w.Ref()}); err != nil {
		h.t.Fatalf("draining worker %s: %v", w.Pod, err)
	}
}

// Restart replaces the worker's process, as when it crashes and its pod
// restarts it: the new process acquires a new epoch and registers, and the
// sandboxes the old one ran are gone. Like a real new process, it has not
// reported its sandbox runtimes; call SetSandboxRuntime for that. ate-api's
// reconciler observes the new epoch in the background, once no operation on
// the worker's actors holds them.
func (w *Worker) Restart() {
	h := w.h
	h.t.Helper()
	ws := w.Node.WorkerService()
	acquired, err := ws.AcquireWorkerEpoch(h.ctx, &ateapi.AcquireWorkerEpochRequest{Worker: w.Ref()})
	if err != nil {
		h.t.Fatalf("acquiring a new epoch for worker %s: %v", w.Pod, err)
	}
	epoch := acquired.GetEpoch()
	if err := RetryAborted(h.ctx, func() error {
		_, err := ws.RegisterWorker(h.ctx, &ateapi.RegisterWorkerRequest{
			Worker: w.Ref(), Epoch: epoch, Ips: []string{w.IP}, Capacity: w.capacity,
		})
		return err
	}); err != nil {
		h.t.Fatalf("registering worker %s again: %v", w.Pod, err)
	}
	w.Node.Atelet.restartWorker(w.Name, epoch)
	w.Epoch = epoch
}

// SetSandboxRuntime has the worker report rt as the runtime it runs actors
// under, and restorable as the runtimes it restores at full fidelity, as a
// worker does after its sandbox is upgraded. It waits until the Worker shows
// them.
func (w *Worker) SetSandboxRuntime(rt *ateapi.ActorSandboxRuntime, restorable ...*ateapi.ActorSandboxRuntime) {
	w.h.t.Helper()
	want := w.reportSandboxRuntimes(rt, restorable)
	w.h.Eventually(fmt.Sprintf("worker %s to report its new sandbox runtime", w.Pod), func() bool {
		return proto.Equal(want, w.h.GetWorker(w).GetStatus().GetSandboxClasses()[0])
	})
}

// reportSandboxRuntimes reports the worker's runtimes through its atelet, and
// returns the class it reported.
func (w *Worker) reportSandboxRuntimes(rt *ateapi.ActorSandboxRuntime, restorable []*ateapi.ActorSandboxRuntime) *ateapi.WorkerSandboxClass {
	w.h.t.Helper()
	class := &ateapi.WorkerSandboxClass{
		SandboxClass:   rt.GetSandboxClass(),
		DefaultRuntime: &ateapi.WorkerSandboxRuntime{Version: rt.GetVersion()},
	}
	for _, r := range restorable {
		class.RestorableRuntimes = append(class.RestorableRuntimes, &ateapi.WorkerSandboxRuntime{Version: r.GetVersion()})
	}
	if err := RetryAborted(w.h.ctx, func() error {
		_, err := w.Node.WorkerService().SetWorkerSandboxRuntimes(w.h.ctx, &ateapi.SetWorkerSandboxRuntimesRequest{
			Worker: w.Ref(), Epoch: w.Epoch, SandboxClasses: []*ateapi.WorkerSandboxClass{class},
		})
		return err
	}); err != nil {
		w.h.t.Fatalf("reporting the sandbox runtimes of worker %s: %v", w.Pod, err)
	}
	return class
}

// Remove deletes the worker pod and its Worker, as the worker syncer does once
// the pod is gone.
func (w *Worker) Remove() {
	h := w.h
	h.t.Helper()
	if err := h.k8s.CoreV1().Pods(h.namespace).Delete(h.ctx, w.Pod, metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To[int64](0),
	}); err != nil {
		h.t.Fatalf("deleting worker pod %s: %v", w.Pod, err)
	}
	if _, err := h.Control.DeleteWorker(h.ctx, &ateapi.DeleteWorkerRequest{Worker: w.Ref()}); err != nil {
		h.t.Fatalf("deleting worker %s: %v", w.Pod, err)
	}
}

// worker returns the Worker the test added under name, or nil.
func (h *Harness) worker(name string) *Worker {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.workersByName[name]
}

// createRunningPod creates a pod on nodeName and marks it running and ready
// at ip, as the kubelet would.
func (h *Harness) createRunningPod(namespace, name, nodeName, ip string, labels map[string]string) *corev1.Pod {
	h.t.Helper()
	created, err := h.k8s.CoreV1().Pods(namespace).Create(h.ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "main", Image: "harness"}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		h.t.Fatalf("creating pod %s/%s: %v", namespace, name, err)
	}
	created.Status.Phase = corev1.PodRunning
	created.Status.PodIP = ip
	created.Status.PodIPs = []corev1.PodIP{{IP: ip}}
	created.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	updated, err := h.k8s.CoreV1().Pods(namespace).UpdateStatus(h.ctx, created, metav1.UpdateOptions{})
	if err != nil {
		h.t.Fatalf("updating the status of pod %s/%s: %v", namespace, name, err)
	}
	return updated
}

// Eventually polls cond until it holds, failing the test after a timeout.
// Tests use it to wait for an outcome ate-api reaches asynchronously.
func (h *Harness) Eventually(what string, cond func() bool) {
	h.t.Helper()
	if err := wait.PollUntilContextTimeout(h.ctx, 10*time.Millisecond, waitTimeout, true, func(context.Context) (bool, error) {
		return cond(), nil
	}); err != nil {
		h.t.Fatalf("waiting for %s: %v", what, err)
	}
}
