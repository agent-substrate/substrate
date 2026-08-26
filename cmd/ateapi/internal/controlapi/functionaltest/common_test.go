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

package functionaltest

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

const (
	testAtespace = "test-atespace"
	testActorID  = "id1"

	// ateletNamespace and byNode mirror the unexported constants controlapi's
	// atelet informer is built with.
	ateletNamespace = "ate-system"
	byNode          = "by-node"
)

var (
	ignoreUID        = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid")
	ignoreVersion    = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "version")
	ignoreTimestamps = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "create_time", "update_time")

	// ignoreServerMetadata skips the ResourceMetadata fields the store assigns.
	ignoreServerMetadata = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid", "create_time", "update_time")
)

type testContext struct {
	service             *controlapi.RPCService
	client              ateapipb.ControlClient
	k8sClient           kubernetes.Interface
	substrateClient     versioned.Interface
	persistence         store.Interface
	workerCache         *workercache.Cache
	fakeAtelet          *FakeAteletServer
	cleanup             func()
	actorTemplateLister listersv1alpha1.ActorTemplateLister
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
	// ateletIndexer is the index DialForAteletOnNode looks up atelets in.
	// setupAteletOnNode waits on it so a test never dials a node whose atelet the
	// informer has not seen yet.
	ateletIndexer cache.Indexer
	// metricReader collects what the service's instruments recorded.
	metricReader *sdkmetric.ManualReader
}

// setupTest sets up a fully isolated test environment.
func setupTest(t *testing.T, ns string) *testContext {
	t.Helper()
	return setupTestWithVolumePlugins(t, ns, nil)
}

// setupTestWithVolumePlugins is setupTest with the default mock volume plugin
// replaced by plugins, keyed by driver name. Tests that need a failure-injecting
// plugin pass it here rather than swapping it into the running RPCService, so each
// test owns its own plugin set.
func setupTestWithVolumePlugins(t *testing.T, ns string, plugins map[string]volume.VolumePluginControlPlane) *testContext {
	t.Helper()
	// 1. Start an isolated PostgreSQL-backed store.
	persistence, cleanupStore := storetest.SetupTestStore(t)

	// 2. Initialize Clientsets using global cfg
	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		cleanupStore()
		t.Fatalf("failed to create k8s clientset: %v", err)
	}

	substrateClient, err := versioned.NewForConfig(cfg)
	if err != nil {
		cleanupStore()
		t.Fatalf("failed to create substrate clientset: %v", err)
	}

	// 3. Initialize Informers
	workerFactory, workerInformer := controlapi.WorkerPodInformer(k8sClient)
	ateletFactory, ateletInformer := controlapi.AteletInformer(k8sClient)
	scFactory := informers.NewSharedInformerFactory(k8sClient, 0)
	scLister := scFactory.Storage().V1().StorageClasses().Lister()

	substrateInformerFactory := externalversions.NewSharedInformerFactory(substrateClient, 0)
	actorTemplateLister := substrateInformerFactory.Api().V1alpha1().ActorTemplates().Lister()
	workerPoolLister := substrateInformerFactory.Api().V1alpha1().WorkerPools().Lister()
	sandboxConfigLister := substrateInformerFactory.Api().V1alpha1().SandboxConfigs().Lister()
	csiDriverConfigLister := substrateInformerFactory.Api().V1alpha1().CSIDriverConfigs().Lister()

	ctx, cancel := context.WithCancel(context.Background())

	workerFactory.Start(ctx.Done())
	ateletFactory.Start(ctx.Done())
	substrateInformerFactory.Start(ctx.Done())
	scFactory.Start(ctx.Done())

	workerFactory.WaitForCacheSync(ctx.Done())
	ateletFactory.WaitForCacheSync(ctx.Done())
	substrateInformerFactory.WaitForCacheSync(ctx.Done())
	scFactory.WaitForCacheSync(ctx.Done())

	// 4. Initialize Service
	wc := workercache.New(persistence, 5*time.Minute)
	if err := wc.Start(ctx); err != nil {
		cancel()
		cleanupStore()
		t.Fatalf("failed to start worker cache: %v", err)
	}

	// Dial the fake atelet over insecure transport instead of per-atelet mTLS,
	// so DialForWorker's real lookup/dial/cache path is exercised under test.
	dialer := controlapi.NewAteletDialer(workerInformer.GetIndexer(), ateletInformer.GetIndexer(), "", "",
		controlapi.WithDialCredentials(func(_ string) (credentials.TransportCredentials, error) {
			return insecure.NewCredentials(), nil
		}))

	metricReader := sdkmetric.NewManualReader()
	instruments, err := controlapi.NewInstruments(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)).Meter("ateapi"))
	if err != nil {
		cancel()
		cleanupStore()
		t.Fatalf("failed to create metric instruments: %v", err)
	}
	volPlugins := plugins
	if volPlugins == nil {
		mockPlugin := volume.NewMockVolumePlugin()
		mockDriverName, err := mockPlugin.DriverName(ctx)
		if err != nil {
			t.Fatalf("failed to get mock driver name: %v", err)
		}
		volPlugins = map[string]volume.VolumePluginControlPlane{
			mockDriverName: mockPlugin,
		}
	}
	service := controlapi.NewRPCService(persistence, wc, actorTemplateLister, workerPoolLister, sandboxConfigLister, csiDriverConfigLister, scLister, dialer, instruments, "", volPlugins)

	// 5. Start REAL gRPC Server for ATE API
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor))
	ateapipb.RegisterControlServer(grpcServer, service)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		cancel()
		cleanupStore()
		t.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("grpc server exited: %v", err)
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Stop()
		cancel()
		cleanupStore()
		t.Fatalf("failed to connect: %v", err)
	}

	client := ateapipb.NewControlClient(conn)

	// Call Reset on global mock
	fakeAtelet.Reset()

	// Create namespace
	_, err = k8sClient.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	if err != nil {
		conn.Close()
		grpcServer.Stop()
		cancel()
		cleanupStore()
		t.Fatalf("failed to create namespace %s: %v", ns, err)
	}

	// CreateActor now requires the atespace to exist first.
	if _, err := client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: testAtespace}}}); err != nil {
		conn.Close()
		grpcServer.Stop()
		cancel()
		cleanupStore()
		t.Fatalf("failed to seed test atespace %q: %v", testAtespace, err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		cancel()
		cleanupStore()
	}

	return &testContext{
		service:             service,
		client:              client,
		k8sClient:           k8sClient,
		substrateClient:     substrateClient,
		persistence:         persistence,
		workerCache:         wc,
		fakeAtelet:          fakeAtelet,
		cleanup:             cleanup,
		actorTemplateLister: actorTemplateLister,
		workerPoolLister:    workerPoolLister,
		sandboxConfigLister: sandboxConfigLister,
		ateletIndexer:       ateletInformer.GetIndexer(),
		metricReader:        metricReader,
	}
}

func namespaceForTest(baseName string) string {
	return fmt.Sprintf("%s-%d", baseName, time.Now().UnixNano())
}

func createTemplate(t *testing.T, tc *testContext, ns string) {
	t.Helper()
	createTemplateWithContainers(t, tc, ns, []atev1alpha1.Container{
		{
			Name:    "main",
			Image:   "main@sha256:abc",
			Command: []string{"/main"},
		},
	})
}

// createAtespace creates an atespace via the API.
func createAtespace(t *testing.T, tc *testContext, name string) {
	t.Helper()
	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}}); err != nil {
		t.Fatalf("CreateAtespace(%s) failed: %v", name, err)
	}
}

const poolLabelKey = "pool"

func createTemplateWithContainers(t *testing.T, tc *testContext, ns string, containers []atev1alpha1.Container) {
	createTemplateWithContainersAndVolumes(t, tc, ns, containers, nil)
}

func createTemplateWithVolumes(t *testing.T, tc *testContext, ns string, volumes []atev1alpha1.Volume, mounts []atev1alpha1.VolumeMount) {
	createTemplateWithContainersAndVolumes(t, tc, ns, []atev1alpha1.Container{
		{
			Name:         "main",
			Image:        "main@sha256:abc",
			Command:      []string{"/main"},
			VolumeMounts: mounts,
		},
	}, volumes)
}

func createTemplateWithContainersAndVolumes(t *testing.T, tc *testContext, ns string, containers []atev1alpha1.Container, volumes []atev1alpha1.Volume) {
	t.Helper()

	// Sandbox binaries now live on a (cluster-scoped) SandboxConfig resolved via
	// the actor's WorkerPool, not on the ActorTemplate. Create a default gvisor
	// SandboxConfig so a boot-from-spec Run can resolve its assets.
	ensureDefaultGvisorSandboxConfig(t, tc)
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})

	actorTemplate := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tmpl1",
			Namespace: ns,
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{
				Location: "gs://fake-fake-fake",
			},
			Containers: containers,
			Volumes:    volumes,
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{poolLabelKey: ns},
			},
		},
	}
	createdTemplate, err := tc.substrateClient.ApiV1alpha1().ActorTemplates(ns).Create(context.Background(), actorTemplate, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create actor template: %v", err)
	}

	const goldenSnapshot = "golden"
	storetest.MustCreateActorSnapshot(t, context.Background(), tc.persistence, &ateapipb.ActorSnapshot{
		Metadata: &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: goldenSnapshot},
		Status: &ateapipb.ActorSnapshotStatus{
			ActorTemplateNamespace: ns,
			ActorTemplateName:      createdTemplate.GetName(),
			ActorTemplateUid:       string(createdTemplate.GetUID()),
			ContentScope:           ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
			SnapshotUri:            "gs://fake-fake-fake/snapshots/" + resources.GoldenActorAtespace + "/" + goldenSnapshot,
		},
	})
	createdTemplate.Status = atev1alpha1.ActorTemplateStatus{
		GoldenSnapshot: goldenSnapshot,
	}

	_, err = tc.substrateClient.ApiV1alpha1().ActorTemplates(ns).UpdateStatus(context.Background(), createdTemplate, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	// Wait for Informer cache to sync
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		tmpl, err := tc.actorTemplateLister.ActorTemplates(ns).Get("tmpl1")
		if err != nil {
			return false, nil // Retry if not found in cache yet
		}
		return tmpl.Status.GoldenSnapshot != "", nil
	})
	if err != nil {
		t.Fatalf("failed to wait for template status update in informer: %v", err)
	}
}

// testPauseImage is the pause image the default test SandboxConfig carries;
// it is what a resolved WorkloadSpec's sandbox assets should name.
const testPauseImage = "pause@sha256:abc"

// ensureDefaultGvisorSandboxConfig creates the cluster-scoped default gvisor
// SandboxConfig (idempotently) and waits for it to appear in the lister.
func ensureDefaultGvisorSandboxConfig(t *testing.T, tc *testContext) {
	t.Helper()
	const name = "gvisor-default"
	sc := &atev1alpha1.SandboxConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: atev1alpha1.SandboxConfigSpec{
			SandboxClass: atev1alpha1.SandboxClassGvisor,
			Default:      true,
			PauseImage:   testPauseImage,
			Assets: map[string]map[string]atev1alpha1.AssetFile{
				"amd64": {"runsc": {
					URL:    "gs://gvisor/releases/nightly/2026-05-19/x86_64/runsc",
					SHA256: "a397be1abc2420d26bce6c70e6e2ff96c73aaaab929756c56f5e2089ea842b63",
				}},
				"arm64": {"runsc": {
					URL:    "gs://gvisor/releases/nightly/2026-05-19/aarch64/runsc",
					SHA256: "1ba2366ae2efceba166046f51a4104f9261c9cb72c6db8f5b3fe2dc57dea86b9",
				}},
			},
		},
	}
	if _, err := tc.substrateClient.ApiV1alpha1().SandboxConfigs().Create(context.Background(), sc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("failed to create default SandboxConfig: %v", err)
	}
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := tc.sandboxConfigLister.Get(name)
		return err == nil, nil
	}); err != nil {
		t.Fatalf("default SandboxConfig not synced into lister: %v", err)
	}
}

func createWorkerPool(t *testing.T, tc *testContext, ns string, name string, labels map[string]string) {
	t.Helper()
	wp := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			Replicas:   1,
			AteomImage: "ateom@sha256:abc",
		},
	}
	_, err := tc.substrateClient.ApiV1alpha1().WorkerPools(ns).Create(context.Background(), wp, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create WorkerPool: %v", err)
	}

	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := tc.workerPoolLister.WorkerPools(ns).Get(name)
		return err == nil, nil
	})
	if err != nil {
		t.Fatalf("failed to wait for WorkerPool %s/%s in informer: %v", ns, name, err)
	}
}

func createTemplateWithSelector(t *testing.T, tc *testContext, ns string, name string, selector *metav1.LabelSelector) {
	t.Helper()
	ensureDefaultGvisorSandboxConfig(t, tc)
	actorTemplate := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{
				Location: "gs://fake-fake-fake",
			},
			Containers: []atev1alpha1.Container{
				{Name: "main", Image: "main@sha256:abc", Command: []string{"/main"}},
			},
			WorkerSelector: selector,
		},
	}
	_, err := tc.substrateClient.ApiV1alpha1().ActorTemplates(ns).Create(context.Background(), actorTemplate, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create actor template: %v", err)
	}

	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := tc.actorTemplateLister.ActorTemplates(ns).Get(name)
		return err == nil, nil
	})
	if err != nil {
		t.Fatalf("failed to wait for template %s/%s in informer: %v", ns, name, err)
	}
}

// createWorkerPod creates a worker pod, registers the matching Worker, and
// waits for it to reach the worker cache. It returns the name of the resulting
// Worker, which is the key to look it up by.
//
// Both halves are needed: the Worker record is what the scheduler places actors
// on, and the pod is what the atelet dialer resolves to reach one. In a real
// cluster the syncer in ate-controller derives the first from the second; here
// the test writes both, building the Worker exactly as the syncer would.
func createWorkerPod(t *testing.T, tc *testContext, ns string, name string, nodeName string, poolName string) string {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"ate.dev/worker-pool": poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{Name: "main", Image: "nginx"},
			},
		},
	}
	createdPod, err := tc.k8sClient.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create worker pod: %v", err)
	}
	createdPod.Status.PodIPs = []corev1.PodIP{{IP: "127.0.0.1"}}
	createdPod.Status.Phase = corev1.PodRunning
	createdPod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}}
	_, err = tc.k8sClient.CoreV1().Pods(ns).UpdateStatus(context.Background(), createdPod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update worker pod status: %v", err)
	}

	// The WorkerPool supplies the fields the syncer copies off it. Read it from
	// the lister the service itself uses, so a pool the informer has not caught
	// up on fails here rather than producing a Worker the scheduler will not
	// match.
	pool, err := tc.workerPoolLister.WorkerPools(ns).Get(poolName)
	if err != nil {
		t.Fatalf("failed to get WorkerPool %s/%s: %v", ns, poolName, err)
	}
	if _, err := tc.client.CreateWorker(context.Background(), &ateapipb.CreateWorkerRequest{
		Worker: &ateapipb.Worker{
			// Workers are global-scoped and named after the pod UID.
			Metadata:        &ateapipb.ResourceMetadata{Name: string(createdPod.UID)},
			WorkerNamespace: ns,
			WorkerPool:      poolName,
			WorkerPod:       name,
			WorkerPodUid:    string(createdPod.UID),
			Ip:              "127.0.0.1",
			NodeName:        nodeName,
			SandboxClass:    string(pool.Spec.SandboxClass),
			Labels:          pool.GetLabels(),
		},
	}); err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// Wait for the worker to appear in worker cache.
	err = wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		workers, err := tc.workerCache.Workers()
		if err != nil {
			return false, nil // Cache not ready yet; retry.
		}
		for _, w := range workers {
			if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == name {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to wait for worker to appear in worker cache: %v", err)
	}
	return string(createdPod.UID)
}

// waitForWorkerAvailable waits for an assignment release to reach the worker
// cache. Lifecycle RPCs commit the release to the store before the cache's
// PostgreSQL watch processes the corresponding update.
func waitForWorkerAvailable(t *testing.T, tc *testContext, workerName string) {
	t.Helper()
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		worker, err := tc.workerCache.Worker(workerName)
		if err != nil {
			return false, nil
		}
		return worker.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE && worker.GetStatus().GetAssignment() == nil, nil
	})
	if err != nil {
		t.Fatalf("failed to wait for worker %s to become available: %v", workerName, err)
	}
}

// createAteletPod creates an atelet pod on nodeName and marks it Running with
// an IP, which DialForAteletOnNode requires. The pod carries the namespace and
// app=atelet label AteletInformer selects on, and is indexed by its node.
func createAteletPod(kc kubernetes.Interface, name, nodeName string) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ateletNamespace,
			Labels:    map[string]string{"app": "atelet"},
		},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
	}
	created, err := kc.CoreV1().Pods(ateletNamespace).Create(context.Background(), pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("creating atelet pod %s on %s: %w", name, nodeName, err)
	}
	created.Status.PodIPs = []corev1.PodIP{{IP: "127.0.0.1"}}
	created.Status.Phase = corev1.PodRunning
	if _, err := kc.CoreV1().Pods(ateletNamespace).UpdateStatus(context.Background(), created, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("updating atelet pod %s status: %w", name, err)
	}
	return nil
}

// setupAteletOnNode makes nodeName dialable for the duration of the test: it
// creates an atelet pod there, waits for the dialer's index to see it, and
// removes it on cleanup. The package fixture only creates one atelet, on
// node1, and the dialer resolves atelets per node, so a worker on any other
// node is unreachable without this.
func setupAteletOnNode(t *testing.T, tc *testContext, name, nodeName string) {
	t.Helper()
	if err := createAteletPod(tc.k8sClient, name, nodeName); err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() {
		_ = tc.k8sClient.CoreV1().Pods(ateletNamespace).Delete(context.Background(), name, metav1.DeleteOptions{
			GracePeriodSeconds: ptr.To[int64](0),
		})
	})

	// Wait for the dialer's index to see the newly created atlet
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		atelets, err := tc.ateletIndexer.ByIndex(byNode, nodeName)
		if err != nil {
			return false, nil
		}
		for _, obj := range atelets {
			p := obj.(*corev1.Pod)
			if p.Name == name && len(p.Status.PodIPs) > 0 {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to wait for atelet pod on %s to be indexed: %v", nodeName, err)
	}
}

func deleteWorkerPod(t *testing.T, tc *testContext, ns string, name string) {
	t.Helper()
	err := tc.k8sClient.CoreV1().Pods(ns).Delete(context.Background(), name, metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To[int64](0),
	})
	if err != nil {
		t.Fatalf("failed to delete worker pod %s: %v", name, err)
	}

	// Deregister the Worker, as the syncer would once it saw the pod go. The
	// Worker is named after the pod UID, but the caller only has the pod name,
	// so it is found by the pod fields it carries.
	resp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("failed to list workers: %v", err)
	}
	for _, w := range resp.GetWorkers() {
		if w.GetWorkerNamespace() != ns || w.GetWorkerPod() != name {
			continue
		}
		if _, err := tc.client.DeleteWorker(context.Background(), &ateapipb.DeleteWorkerRequest{
			Worker: &ateapipb.ObjectRef{Name: w.GetMetadata().GetName()},
		}); err != nil {
			t.Fatalf("failed to deregister worker %s: %v", w.GetMetadata().GetName(), err)
		}
	}

	err = wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		workers, err := tc.workerCache.Workers()
		if err != nil {
			return false, nil // Cache not ready yet; retry.
		}
		for _, w := range workers {
			if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == name {
				return false, nil // Still there
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("failed to wait for worker to be removed from worker cache: %v", err)
	}
}

func assertGrpcErrorRegex(t *testing.T, err error, wantCode codes.Code, wantMsg string) {
	t.Helper()
	fn := func(got string) (string, bool) {
		matched, matchErr := regexp.MatchString(wantMsg, got)
		if matchErr != nil {
			t.Fatalf("failed to compile regex %q: %v", wantMsg, matchErr)
		}
		return wantMsg, matched
	}
	assertGrpcErrorImpl(t, err, wantCode, fn)
}

func assertGrpcError(t *testing.T, err error, wantCode codes.Code, wantMsg string) {
	t.Helper()
	fn := func(got string) (string, bool) {
		return wantMsg, got == wantMsg
	}
	assertGrpcErrorImpl(t, err, wantCode, fn)
}

func assertGrpcErrorImpl(t *testing.T, err error, wantCode codes.Code, msgMatches func(got string) (string, bool)) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != wantCode {
		t.Errorf("expected status %v, got %v", wantCode, st.Code())
	}
	if want, ok := msgMatches(st.Message()); !ok {
		t.Errorf("expected message %q, got %q", want, st.Message())
	}
}

// recordRootSpanAttrs runs fn under a fresh recording root span from a local
// TracerProvider and returns that span's attributes, so a test can observe what
// the code under test stamps on the span carried in ctx. It never swaps the
// global provider (the code under test reads its span via trace.SpanFromContext,
// not the global provider), so span tests stay parallel-safe.
func recordRootSpanAttrs(t *testing.T, fn func(ctx context.Context)) map[attribute.Key]attribute.Value {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	ctx, root := tp.Tracer("test").Start(context.Background(), "root")
	fn(ctx)
	root.End()
	for _, s := range sr.Ended() {
		if s.Name() == "root" {
			m := make(map[attribute.Key]attribute.Value, len(s.Attributes()))
			for _, kv := range s.Attributes() {
				m[kv.Key] = kv.Value
			}
			return m
		}
	}
	t.Fatal("root span not recorded")
	return nil
}

func assertSpanStr(t *testing.T, attrs map[attribute.Key]attribute.Value, key attribute.Key, want string) {
	t.Helper()
	v, ok := attrs[key]
	if !ok {
		t.Errorf("missing %s", key)
		return
	}
	if v.AsString() != want {
		t.Errorf("%s = %q, want %q", key, v.AsString(), want)
	}
}
