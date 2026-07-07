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

package controlapi

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateredis"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/envtestbins"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	testEnv    *envtest.Environment
	cfg        *rest.Config
	fakeAtelet = &FakeAteletServer{}
)

const testAtespace = "test-atespace"

func TestMain(m *testing.M) {
	binaryAssetsDirectory, err := envtestbins.BinaryAssetsDir()
	if err != nil {
		log.Fatalf("%v", err)
	}

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../../../manifests/ate-install/generated"},
		BinaryAssetsDirectory: binaryAssetsDirectory,
	}

	cfg, err = testEnv.Start()
	if err != nil {
		log.Fatalf("testEnv.Start: %v", err)
	}

	// Create ate-system namespace
	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("kubernetes.NewForConfig: %v", err)
	}
	_, err = k8sClient.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ate-system"},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		log.Fatalf("create ate-system namespace: %v", err)
	}

	// Create shared Atelet Pod
	ateletPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "atelet-shared",
			Namespace: "ate-system",
			Labels: map[string]string{
				"app": "atelet",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{
				{Name: "main", Image: "nginx"},
			},
		},
	}
	createdAtelet, err := k8sClient.CoreV1().Pods("ate-system").Create(context.Background(), ateletPod, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		log.Fatalf("create atelet pod: %v", err)
	}
	if err == nil {
		createdAtelet.Status.PodIPs = []corev1.PodIP{{IP: "127.0.0.1"}}
		createdAtelet.Status.Phase = corev1.PodRunning
		_, err = k8sClient.CoreV1().Pods("ate-system").UpdateStatus(context.Background(), createdAtelet, metav1.UpdateOptions{})
		if err != nil {
			log.Fatalf("update atelet pod status: %v", err)
		}
	}

	// Start Fake Atelet Server on port 8085
	ateletGrpcServer := grpc.NewServer()
	ateletpb.RegisterAteomHerderServer(ateletGrpcServer, fakeAtelet)
	ateletLis, err := net.Listen("tcp", "127.0.0.1:8085")
	if err != nil {
		log.Fatalf("listen on 127.0.0.1:8085: %v", err)
	}
	go func() {
		if err := ateletGrpcServer.Serve(ateletLis); err != nil {
			fmt.Printf("atelet grpc server exited: %v\n", err)
		}
	}()

	code := m.Run()

	ateletGrpcServer.Stop()

	err = testEnv.Stop()
	if err != nil {
		log.Fatalf("testEnv.Stop: %v", err)
	}

	os.Exit(code)
}

// FakeAteletServer implements ateletpb.WorkersServer
type FakeAteletServer struct {
	ateletpb.UnimplementedAteomHerderServer

	Lock sync.Mutex

	RunCalled  bool
	RunRequest *ateletpb.RunRequest
	FailRun    error

	CheckpointCalled  bool
	CheckpointRequest *ateletpb.CheckpointRequest

	RestoreCalled  bool
	RestoreRequest *ateletpb.RestoreRequest
	FailRestore    error
	RestoreDelay   time.Duration
}

func (f *FakeAteletServer) Reset() {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.RunCalled = false
	f.RunRequest = nil
	f.FailRun = nil

	f.CheckpointCalled = false
	f.CheckpointRequest = nil

	f.RestoreCalled = false
	f.RestoreRequest = nil
	f.FailRestore = nil
	f.RestoreDelay = 0
}

func (f *FakeAteletServer) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.RunCalled = true
	f.RunRequest = proto.Clone(req).(*ateletpb.RunRequest)
	if f.FailRun != nil {
		return nil, f.FailRun
	}

	return &ateletpb.RunResponse{}, nil
}

func (f *FakeAteletServer) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.CheckpointCalled = true
	f.CheckpointRequest = proto.Clone(req).(*ateletpb.CheckpointRequest)

	return &ateletpb.CheckpointResponse{}, nil
}

func (f *FakeAteletServer) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.RestoreCalled = true
	f.RestoreRequest = proto.Clone(req).(*ateletpb.RestoreRequest)
	if f.RestoreDelay > 0 {
		time.Sleep(f.RestoreDelay)
	}
	if f.FailRestore != nil {
		return nil, f.FailRestore
	}
	return &ateletpb.RestoreResponse{}, nil
}

func (f *FakeAteletServer) lastRestoreRequest() *ateletpb.RestoreRequest {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	if f.RestoreRequest == nil {
		return nil
	}
	return proto.Clone(f.RestoreRequest).(*ateletpb.RestoreRequest)
}

type testContext struct {
	mr                  *miniredis.Miniredis
	service             *Service
	client              ateapipb.ControlClient
	k8sClient           kubernetes.Interface
	substrateClient     versioned.Interface
	persistence         *ateredis.Persistence
	workerCache         *workercache.Cache
	fakeAtelet          *FakeAteletServer
	cleanup             func()
	actorTemplateLister listersv1alpha1.ActorTemplateLister
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
}

// setupTest sets up a fully isolated test environment.
func setupTest(t *testing.T, ns string) *testContext {
	t.Helper()
	// 1. Start Miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{mr.Addr()},
	})
	persistence := ateredis.NewPersistence(rdb)

	// 2. Initialize Clientsets using global cfg
	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		mr.Close()
		t.Fatalf("failed to create k8s clientset: %v", err)
	}

	substrateClient, err := versioned.NewForConfig(cfg)
	if err != nil {
		mr.Close()
		t.Fatalf("failed to create substrate clientset: %v", err)
	}

	// 3. Initialize Informers
	workerFactory, workerInformer := WorkerPodInformer(k8sClient)
	ateletFactory, ateletInformer := AteletInformer(k8sClient)

	substrateInformerFactory := externalversions.NewSharedInformerFactory(substrateClient, 0)
	actorTemplateLister := substrateInformerFactory.Api().V1alpha1().ActorTemplates().Lister()
	workerPoolLister := substrateInformerFactory.Api().V1alpha1().WorkerPools().Lister()
	sandboxConfigLister := substrateInformerFactory.Api().V1alpha1().SandboxConfigs().Lister()

	ctx, cancel := context.WithCancel(context.Background())

	syncer := NewWorkerPoolSyncer(persistence, workerInformer, workerPoolLister)
	syncer.Start(ctx)

	workerFactory.Start(ctx.Done())
	ateletFactory.Start(ctx.Done())
	substrateInformerFactory.Start(ctx.Done())

	workerFactory.WaitForCacheSync(ctx.Done())
	ateletFactory.WaitForCacheSync(ctx.Done())
	substrateInformerFactory.WaitForCacheSync(ctx.Done())

	// 4. Initialize Service
	wc := workercache.New(persistence, 5*time.Minute)
	if err := wc.Start(ctx); err != nil {
		cancel()
		mr.Close()
		t.Fatalf("failed to start worker cache: %v", err)
	}

	dialer := NewAteletDialer(workerInformer.GetIndexer(), ateletInformer.GetIndexer())
	service := NewService(persistence, wc, actorTemplateLister, workerPoolLister, sandboxConfigLister, dialer, k8sClient)

	// 5. Start REAL gRPC Server for ATE API
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor))
	ateapipb.RegisterControlServer(grpcServer, service)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		cancel()
		mr.Close()
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
		mr.Close()
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
		mr.Close()
		t.Fatalf("failed to create namespace %s: %v", ns, err)
	}

	// CreateActor now requires the atespace to exist first.
	if _, err := client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: testAtespace}); err != nil {
		conn.Close()
		grpcServer.Stop()
		cancel()
		mr.Close()
		t.Fatalf("failed to seed test atespace %q: %v", testAtespace, err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
		cancel()
		mr.Close()
	}

	return &testContext{
		mr:                  mr,
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
	}
}

func namespaceForTest(baseName string) string {
	return fmt.Sprintf("%s-%d", baseName, time.Now().UnixNano())
}

func selectorLabelsOfSize(n int) map[string]string {
	labels := make(map[string]string, n)
	for i := 0; i < n; i++ {
		labels[fmt.Sprintf("k%d", i)] = "v"
	}
	return labels
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
	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: name}); err != nil {
		t.Fatalf("CreateAtespace(%s) failed: %v", name, err)
	}
}

const poolLabelKey = "pool"

func createTemplateWithContainers(t *testing.T, tc *testContext, ns string, containers []atev1alpha1.Container) {
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
			PauseImage: "pause@sha256:abc",
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{
				Location: "gs://fake-fake-fake",
			},
			Containers: containers,
			WorkerSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{poolLabelKey: ns},
			},
		},
	}
	createdTemplate, err := tc.substrateClient.ApiV1alpha1().ActorTemplates(ns).Create(context.Background(), actorTemplate, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create actor template: %v", err)
	}

	createdTemplate.Status = atev1alpha1.ActorTemplateStatus{
		GoldenSnapshot: "gs://my-bucket/my-folder",
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
			PauseImage: "pause@sha256:abc",
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

func createWorkerPod(t *testing.T, tc *testContext, ns string, name string, nodeName string, poolName string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       "08675309-4a65-6e6e-7973-6e756d626572",
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
	/*
			   pod := &corev1.Pod{
		          ObjectMeta: metav1.ObjectMeta{
		              Name:      podName,
		              Namespace: ns,
		              UID:       "08675309-4a65-6e6e-7973-6e756d626572",
		              Labels: map[string]string{
		                  workerPodLabel: poolName,
		              },
		          },
		          Spec: corev1.PodSpec{
		              NodeName:   "node1",
		              Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		          },
		      }

	*/
	createdPod, err := tc.k8sClient.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create worker pod: %v", err)
	}
	createdPod.Status.PodIPs = []corev1.PodIP{{IP: "127.0.0.1"}}
	createdPod.Status.Phase = corev1.PodRunning
	_, err = tc.k8sClient.CoreV1().Pods(ns).UpdateStatus(context.Background(), createdPod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update worker pod status: %v", err)
	}

	// Wait for worker to be registered via API
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		resp, err := tc.client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
		if err != nil {
			return false, nil // Retry on API error
		}
		for _, w := range resp.GetWorkers() {
			if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == name {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to wait for worker to be registered: %v", err)
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
}

func deleteWorkerPod(t *testing.T, tc *testContext, ns string, name string) {
	t.Helper()
	err := tc.k8sClient.CoreV1().Pods(ns).Delete(context.Background(), name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete worker pod %s: %v", name, err)
	}

	// Wait for worker to be removed from API
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		resp, err := tc.client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
		if err != nil {
			return false, nil // Retry on API error
		}
		for _, w := range resp.GetWorkers() {
			if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == name {
				return false, nil // Still there
			}
		}
		return true, nil // Gone!
	})
	if err != nil {
		t.Fatalf("failed to wait for worker to be removed: %v", err)
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

// TestCreateActor_Success tests the happy path for creating an actor.
// Workflow:
// 1. Creates a mock ActorTemplate in the test namespace.
// 2. Calls CreateActor RPC.
// 3. Verifies that the actor is successfully created and returned in the response with a generated ID.
func TestCreateActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-create-success")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	want := &ateapipb.CreateActorResponse{
		Actor: &ateapipb.Actor{
			ActorId:                "id1",
			Version:                1,
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
			WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}},
			Atespace:               testAtespace,
		},
	}

	if diff := cmp.Diff(want, createResp, protocmp.Transform()); diff != "" {
		t.Errorf("CreateActor response mismatch (-want +got):\n%s", diff)
	}
}

// TestCreateActor_TemplateNotFound tests that creating an actor with a non-existent template fails with FailedPrecondition.
func TestCreateActor_TemplateNotFound(t *testing.T) {
	ns := namespaceForTest("ns-create-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "non-existent",
	})
	assertGrpcError(t, err, codes.FailedPrecondition, fmt.Sprintf("ActorTemplate %s/non-existent not found", ns))
}

// TestCreateActor_Duplicate tests that creating an actor with an existing ID fails.
func TestCreateActor_Duplicate(t *testing.T) {
	ns := namespaceForTest("ns-create-dup")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("first CreateActor failed: %v", err)
	}

	_, err = tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	assertGrpcError(t, err, codes.AlreadyExists, "Actor id1 already exists")
}

// TestGetActor_Found tests that an existing actor can be retrieved.
func TestGetActor_Found(t *testing.T) {
	ns := namespaceForTest("ns-get-found")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	id := createResp.GetActor().GetActorId()

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	want := &ateapipb.GetActorResponse{
		Actor: createResp.GetActor(),
	}

	if diff := cmp.Diff(want, getResp, protocmp.Transform()); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
}

// TestGetActor_NotFound tests that retrieving a non-existent actor fails.
// Workflow:
// 1. Calls GetActor RPC with a non-existent ID.
// 2. Verifies that it returns an error (NotFound).
func TestGetActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-get-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "non-existent"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor non-existent not found")
}

// TestListActors tests that all created actors can be listed.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Calls CreateActor twice to create two actors.
// 3. Calls ListActors RPC.
// 4. Verifies that both actors are returned in the list.
func TestListActors(t *testing.T) {
	ns := namespaceForTest("ns-list-actors")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	resp1, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor 1 failed: %v", err)
	}
	resp2, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id2"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor 2 failed: %v", err)
	}

	listResp, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListActors failed: %v", err)
	}

	if len(listResp.Actors) != 2 {
		t.Fatalf("expected 2 actors, got %d", len(listResp.Actors))
	}

	want := []*ateapipb.Actor{
		resp1.GetActor(),
		resp2.GetActor(),
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool {
			return a.ActorId < b.ActorId
		}),
	}

	if diff := cmp.Diff(want, listResp.Actors, opts...); diff != "" {
		t.Errorf("ListActors response mismatch (-want +got):\n%s", diff)
	}
}

// TestListActors_ByAtespace verifies create + list are scoped by atespace end to
// end through the RPC surface: an actor created with a given atespace is only
// returned by ListActors(atespace=X) and only fetched by GetActor(atespace=X).
func TestListActors_ByAtespace(t *testing.T) {
	ns := namespaceForTest("ns-list-by-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createAtespace(t, tc, "team-a")
	createAtespace(t, tc, "team-b")

	create := func(atespace, id string) *ateapipb.Actor {
		resp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
			ActorRef:               &ateapipb.ActorRef{Atespace: atespace, Name: id},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		})
		if err != nil {
			t.Fatalf("CreateActor(%s, atespace=%q) failed: %v", id, atespace, err)
		}
		return resp.GetActor()
	}
	a1 := create("team-a", "id1")
	a2 := create("team-a", "id2")
	b1 := create("team-b", "id3")

	sortByID := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool { return a.ActorId < b.ActorId }),
	}

	// List scoped to team-a returns only its actors.
	listA, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{Atespace: "team-a"})
	if err != nil {
		t.Fatalf("ListActors(team-a) failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.Actor{a1, a2}, listA.GetActors(), sortByID...); diff != "" {
		t.Errorf("ListActors(team-a) mismatch (-want +got):\n%s", diff)
	}

	// List scoped to team-b returns only its actor.
	listB, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{Atespace: "team-b"})
	if err != nil {
		t.Fatalf("ListActors(team-b) failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.Actor{b1}, listB.GetActors(), sortByID...); diff != "" {
		t.Errorf("ListActors(team-b) mismatch (-want +got):\n%s", diff)
	}

	// Get is scoped: the right atespace hits, the empty atespace misses (deny-across by key).
	if _, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: "team-a", Name: "id1"}}); err != nil {
		t.Errorf("GetActor(id1, team-a) failed: %v", err)
	}
	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"}})
	assertGrpcError(t, err, codes.NotFound, "Actor id1 not found")
}

// TestListActors_AllAtespaces verifies that an empty atespace lists actors across
// all atespaces (the `-A` / admin view), unlike the scoped single-atespace listing.
func TestListActors_AllAtespaces(t *testing.T) {
	ns := namespaceForTest("ns-list-all-atespaces")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createAtespace(t, tc, "team-a")
	createAtespace(t, tc, "team-b")

	create := func(atespace, id string) {
		if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
			ActorRef:               &ateapipb.ActorRef{Atespace: atespace, Name: id},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		}); err != nil {
			t.Fatalf("CreateActor(%s, atespace=%q) failed: %v", id, atespace, err)
		}
	}
	create("team-a", "id1")
	create("team-b", "id2")

	// Empty atespace lists across all atespaces; returned actors carry their atespace.
	resp, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{})
	if err != nil {
		t.Fatalf("ListActors(all) failed: %v", err)
	}
	got := map[string]string{}
	for _, a := range resp.GetActors() {
		got[a.GetActorId()] = a.GetAtespace()
	}
	if got["id1"] != "team-a" {
		t.Errorf("ListActors(all): got[id1]=%q, want team-a", got["id1"])
	}
	if got["id2"] != "team-b" {
		t.Errorf("ListActors(all): got[id2]=%q, want team-b", got["id2"])
	}
}

// TestListActors_Pagination tests that ListActors correctly paginates results.
func TestListActors_Pagination(t *testing.T) {
	ns := namespaceForTest("ns-list-actors-pagination")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	var want []*ateapipb.Actor
	for i := 0; i < 5; i++ {
		resp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
			ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: fmt.Sprintf("id%d", i)},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		})
		if err != nil {
			t.Fatalf("CreateActor %d failed: %v", i, err)
		}
		want = append(want, resp.GetActor())
	}

	var allActors []*ateapipb.Actor
	pageToken := ""

	for {
		listResp, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{
			Atespace:  testAtespace,
			PageSize:  2,
			PageToken: pageToken,
		})
		if err != nil {
			t.Fatalf("ListActors failed: %v", err)
		}

		allActors = append(allActors, listResp.Actors...)
		pageToken = listResp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	if len(allActors) != 5 {
		t.Fatalf("expected 5 actors total, got %d", len(allActors))
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool {
			return a.ActorId < b.ActorId
		}),
	}

	if diff := cmp.Diff(want, allActors, opts...); diff != "" {
		t.Errorf("ListActors pagination response mismatch (-want +got):\n%s", diff)
	}
}

func TestListActors_PageSizeValidation(t *testing.T) {
	ns := namespaceForTest("ns-list-actors-validation")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	// 1. Negative page size
	_, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{
		Atespace: testAtespace,
		PageSize: -1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument error for negative page_size, got: %v", err)
	}

	// 2. Page size exceeding maxPageSize (1000)
	_, err = tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{
		Atespace: testAtespace,
		PageSize: 1001,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument error for page_size > 1000, got: %v", err)
	}
}

// TestListWorkers tests that workers mirrored to Redis are listed.
// Workflow:
// 1. Creates a mock WorkerPool in Kubernetes.
// 2. Creates a mock worker Pod in Kubernetes belonging to that pool.
// 3. Waits for the background WorkerPoolSyncer to mirror it to Redis.
// 4. Calls ListWorkers RPC.
// 5. Verifies that the worker appears in the response.
func TestListWorkers(t *testing.T) {
	ns := namespaceForTest("ns-list-workers")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool1", map[string]string{"foo": "bar"})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	listResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}

	var filteredWorkers []*ateapipb.Worker
	for _, w := range listResp.GetWorkers() {
		if w.GetWorkerNamespace() == ns {
			filteredWorkers = append(filteredWorkers, w)
		}
	}

	want := []*ateapipb.Worker{
		{
			WorkerNamespace: ns,
			WorkerPool:      "pool1",
			WorkerPod:       "worker-1",
			NodeName:        "node1",
			Ip:              "127.0.0.1",
			Version:         1,
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"foo": "bar"},
		},
	}

	if diff := cmp.Diff(want, filteredWorkers, protocmp.Transform(), protocmp.IgnoreFields(&ateapipb.Worker{}, "worker_pod_uid")); diff != "" {
		t.Errorf("ListWorkers response mismatch (-want +got):\n%s", diff)
	}
}

// TestResumeActor tests the full workflow of resuming a suspended actor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod in 'ate-system' namespace on 'node1'.
// 3. Creates a mock worker Pod in the test namespace on 'node1'.
// 4. Waits for the WorkerPoolSyncer to mirror the worker to Redis.
// 5. Creates an actor (starts as SUSPENDED).
// 6. Calls ResumeActor RPC.
// 7. Verifies that the fake Atelet received the Restore call.
// 8. Verifies that the actor status is updated to RUNNING.
func TestResumeActor(t *testing.T) {
	ns := namespaceForTest("ns-resume")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	id := "id1"

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	if !tc.fakeAtelet.RestoreCalled {
		t.Errorf("expected Restore to be called")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.GetActorResponse{
		Actor: &ateapipb.Actor{
			ActorId:                id,
			Atespace:               testAtespace,
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_RUNNING,
			AteomPodNamespace:      ns,
			AteomPodName:           "worker-1",
			AteomPodIp:             "127.0.0.1",
			WorkerPoolName:         "pool1",
		},
	}
	if diff := cmp.Diff(want, getResp, protocmp.Transform(), protocmp.IgnoreFields(&ateapipb.Actor{}, "version"), protocmp.IgnoreFields(&ateapipb.Actor{}, "ateom_pod_uid")); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}

	// Verify that the worker record also has the assigned actor details
	listWorkersResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	var actorWorker *ateapipb.Worker
	for _, w := range listWorkersResp.GetWorkers() {
		if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == "worker-1" {
			actorWorker = w
			break
		}
	}
	if actorWorker == nil {
		t.Fatalf("expected worker-1 in namespace %s not found in ListWorkers", ns)
	}

	wantWorker := &ateapipb.Worker{
		WorkerNamespace: ns,
		WorkerPool:      "pool1",
		WorkerPod:       "worker-1",
		Assignment: &ateapipb.Assignment{
			ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
				Namespace: ns,
				Name:      "tmpl1",
			},
			Actor: &ateapipb.ActorRef{
				Name:     id,
				Atespace: testAtespace,
			},
		},
		Ip:           "127.0.0.1",
		NodeName:     "node1",
		SandboxClass: "gvisor",
		Labels:       map[string]string{poolLabelKey: ns},
	}

	if diff := cmp.Diff(wantWorker, actorWorker, protocmp.Transform(), protocmp.IgnoreFields(&ateapipb.Worker{}, "version"), protocmp.IgnoreFields(&ateapipb.Worker{}, "worker_pod_uid")); diff != "" {
		t.Errorf("Worker state mismatch (-want +got):\n%s", diff)
	}
}

func TestResumeActorResolvesValueFromEnv(t *testing.T) {
	ns := namespaceForTest("ns-resume-secret-env")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.k8sClient.CoreV1().Secrets(ns).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-keys",
			Namespace: ns,
		},
		Data: map[string][]byte{
			"anthropic": []byte("sk-test"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create secret: %v", err)
	}

	createTemplateWithContainers(t, tc, ns, []atev1alpha1.Container{
		{
			Name:    "main",
			Image:   "main@sha256:abc",
			Command: []string{"/main"},
			Env: []atev1alpha1.EnvVar{
				{
					Name:  "LITERAL",
					Value: ptr.To("plain"),
				},
				{
					Name: "ANTHROPIC_API_KEY",
					ValueFrom: &atev1alpha1.EnvVarSource{
						SecretKeyRef: &atev1alpha1.SecretKeySelector{
							Name: "api-keys",
							Key:  "anthropic",
						},
					},
				},
			},
		},
	})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err = tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	restoreReq := tc.fakeAtelet.lastRestoreRequest()
	if restoreReq == nil {
		t.Fatalf("expected Restore to be called")
	}
	if len(restoreReq.GetSpec().GetContainers()) != 1 {
		t.Fatalf("expected one container in restore request, got %d", len(restoreReq.GetSpec().GetContainers()))
	}
	gotEnv := map[string]string{}
	for _, env := range restoreReq.GetSpec().GetContainers()[0].GetEnv() {
		gotEnv[env.GetName()] = env.GetValue()
	}
	wantEnv := map[string]string{
		"LITERAL":           "plain",
		"ANTHROPIC_API_KEY": "sk-test",
	}
	if diff := cmp.Diff(wantEnv, gotEnv); diff != "" {
		t.Errorf("resolved env mismatch (-want +got):\n%s", diff)
	}
}

// TestResumeActor_NoWorkers tests that resuming an actor fails when no free workers are available.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates an actor.
// 3. Calls ResumeActor RPC without creating any workers.
// 4. Verifies that ResumeActor fails with FailedPrecondition status.
// TestResumeActor_NoWorkers tests that resuming an actor fails when no free workers are available.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates an actor.
// 3. Calls ResumeActor RPC without creating any workers.
// 4. Verifies that ResumeActor fails with FailedPrecondition status.
func TestResumeActor_NoWorkers(t *testing.T) {
	ns := namespaceForTest("ns-resume-no-workers")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	id := createResp.GetActor().GetActorId()

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "no free workers available")
}

// TestResumeActor_MultiPoolSelector exercises the AND-of-two-selectors path
// end to end: a template's WorkerSelector gates two pools, and the actor's
// worker_selector narrows to just one of them.
func TestResumeActor_MultiPoolSelector(t *testing.T) {
	ns := namespaceForTest("ns-multi-pool")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-a", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-b", map[string]string{"group": ns, "tier": "b"})
	createTemplateWithSelector(t, tc, ns, "tmpl1", &metav1.LabelSelector{
		MatchLabels: map[string]string{"group": ns},
	})

	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool-a")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool-b")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "b"},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetActor().GetAteomPodName(); got != "worker-b" {
		t.Errorf("expected actor to be assigned to worker-b (pool-b, matching narrowed selector), got %q", got)
	}
	if got := getResp.GetActor().GetWorkerPoolName(); got != "pool-b" {
		t.Errorf("expected actor's worker_pool_name to be pool-b, got %q", got)
	}
}

// TestResumeActor_RequiresBothSelectorsToMatch proves eligibility is the AND
// of the template's WorkerSelector and the actor's worker_selector, not
// either one alone: a pool matching only the template selector and a pool
// matching only the actor selector must both be rejected, end to end
// through CreateActor/ResumeActor (not just the eligibleWorkerPools unit
// test), while a pool matching both is the one actually used.
func TestResumeActor_RequiresBothSelectorsToMatch(t *testing.T) {
	ns := namespaceForTest("ns-resume-and-selectors")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-both", map[string]string{"group": ns, "tier": "b"})
	createWorkerPool(t, tc, ns, "pool-template-only", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-actor-only", map[string]string{"tier": "b"})
	createTemplateWithSelector(t, tc, ns, "tmpl1", &metav1.LabelSelector{
		MatchLabels: map[string]string{"group": ns},
	})

	createWorkerPod(t, tc, ns, "worker-both", "node1", "pool-both")
	createWorkerPod(t, tc, ns, "worker-template-only", "node1", "pool-template-only")
	createWorkerPod(t, tc, ns, "worker-actor-only", "node1", "pool-actor-only")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "b"},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"}}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetActor().GetWorkerPoolName(); got != "pool-both" {
		t.Errorf("expected actor to be assigned to pool-both (the only pool matching both selectors), got worker_pool_name=%q", got)
	}
}

// TestResumeActor_Reentrancy tests the failure recovery and re-entrancy of ResumeActor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod and a mock Worker Pod.
// 3. Waits for the WorkerPoolSyncer to mirror the worker to store.
// 4. Creates an actor in SUSPENDED state.
// 5. Configures fake Atelet to FAIL on Restore.
// 6. Calls ResumeActor and verifies it fails, but actor status becomes RESUMING.
// 7. Configures fake Atelet to SUCCEED on Restore.
// 8. Calls ResumeActor again and verifies it succeeds and actor status becomes RUNNING.
func TestResumeActor_Reentrancy(t *testing.T) {
	ns := namespaceForTest("ns-resume-reentrancy")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	// Create Worker Pod
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	id := "id1"

	// STEP 1: Make Atelet FAIL on Restore!
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to atelet error")
	}

	// Verify actor state is RESUMING in Redis!
	actor, err := tc.persistence.GetActor(context.Background(), testAtespace, id)
	if err != nil {
		t.Fatalf("failed to get actor from store: %v", err)
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_RESUMING {
		t.Errorf("expected status RESUMING, got %v", actor.GetStatus())
	}

	// STEP 2: Make Atelet SUCCEED!
	tc.fakeAtelet.FailRestore = nil
	tc.fakeAtelet.RestoreCalled = false // reset for verification

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed on retry: %v", err)
	}

	if !tc.fakeAtelet.RestoreCalled {
		t.Errorf("expected Restore to be called on retry")
	}

	// Verify actor state is RUNNING!
	actor, err = tc.persistence.GetActor(context.Background(), testAtespace, id)
	if err != nil {
		t.Fatalf("failed to get actor from store: %v", err)
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("expected status RUNNING, got %v", actor.GetStatus())
	}
}

// TestSuspendActor tests the full workflow of suspending a running actor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod on 'node1'.
// 3. Creates a mock worker Pod on 'node1'.
// 4. Waits for the WorkerPoolSyncer to mirror the worker to Redis.
// 5. Creates an actor.
// 6. Calls ResumeActor to transition it to RUNNING.
// 7. Calls SuspendActor RPC.
// 8. Verifies that the fake Atelet received the Suspend call.
func TestSuspendActor(t *testing.T) {
	ns := namespaceForTest("ns-suspend")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	id := "id1"

	// Resume first to make it running
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	// Suspend
	_, err = tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	if !tc.fakeAtelet.CheckpointCalled {
		t.Errorf("expected atelet Checkpoint to be called")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.GetActorResponse{
		Actor: &ateapipb.Actor{
			ActorId:                id,
			Atespace:               testAtespace,
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
			LatestSnapshotInfo: &ateapipb.SnapshotInfo{
				Data: &ateapipb.SnapshotInfo_External{
					External: &ateapipb.ExternalSnapshotInfo{
						SnapshotUriPrefix: fmt.Sprintf("gs://fake-fake-fake/%s/", id),
					},
				},
			},
		},
	}

	if diff := cmp.Diff(want, getResp,
		protocmp.Transform(),
		protocmp.IgnoreFields(&ateapipb.Actor{}, "version"),
		protocmp.IgnoreFields(&ateapipb.Actor{}, "ateom_pod_uid"),
		protocmp.FilterField(&ateapipb.ExternalSnapshotInfo{}, "snapshot_uri_prefix", cmp.Comparer(func(x, y string) bool {
			return strings.HasPrefix(y, x)
		})),
	); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
}

// TestPauseActor tests the full workflow of pausing a running actor.
// Workflow:
// 1. Creates a mock ActorTemplate.
// 2. Creates a mock Atelet Pod on 'node1'.
// 3. Creates a mock worker Pod on 'node1'.
// 4. Waits for the WorkerPoolSyncer to mirror the worker to Redis.
// 5. Creates an actor.
// 6. Calls ResumeActor to transition it to RUNNING.
// 7. Calls PauseActor RPC.
// 8. Verifies that the fake Atelet received the Pause call.
func TestPauseActor(t *testing.T) {
	ns := namespaceForTest("ns-pause")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	id := "id1"

	// Resume first to make it running
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	// Pause
	_, err = tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}

	if !tc.fakeAtelet.CheckpointCalled {
		t.Errorf("expected atelet Checkpoint to be called")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.GetActorResponse{
		Actor: &ateapipb.Actor{
			ActorId:                id,
			Atespace:               testAtespace,
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
			Status:                 ateapipb.Actor_STATUS_PAUSED,
			LatestSnapshotInfo: &ateapipb.SnapshotInfo{
				Data: &ateapipb.SnapshotInfo_Local{
					Local: &ateapipb.LocalSnapshotInfo{
						SnapshotPrefix:            "id1",
						NodeVmsWithLocalSnapshots: []string{"node1"},
					},
				},
			},
		},
	}

	if diff := cmp.Diff(want, getResp,
		protocmp.Transform(),
		protocmp.IgnoreFields(&ateapipb.Actor{}, "version"),
		protocmp.IgnoreFields(&ateapipb.Actor{}, "ateom_pod_uid"),
		protocmp.FilterField(&ateapipb.LocalSnapshotInfo{}, "snapshot_prefix", cmp.Comparer(func(x, y string) bool {
			return strings.HasPrefix(y, x)
		})),
	); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
}

// TestUpdateActor_Success verifies UpdateActor replaces the actor's
// worker_selector and that the change is durably persisted.
func TestUpdateActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-update-actor")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "free"},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	updateResp, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "paid"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	wantActor := &ateapipb.Actor{
		ActorId:                "id1",
		Version:                2,
		Atespace:               testAtespace,
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "paid"},
		},
	}
	wantUpdateResp := &ateapipb.UpdateActorResponse{Actor: wantActor}
	if diff := cmp.Diff(wantUpdateResp, updateResp, protocmp.Transform()); diff != "" {
		t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	wantGetResp := &ateapipb.GetActorResponse{Actor: wantActor}
	if diff := cmp.Diff(wantGetResp, getResp, protocmp.Transform()); diff != "" {
		t.Errorf("GetActor response mismatch after UpdateActor (-want +got):\n%s", diff)
	}
}

func TestUpdateActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-update-actor-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "does-not-exist"}})
	assertGrpcError(t, err, codes.NotFound, "Actor does-not-exist not found")
}

// TestResumeActor_ReleasesStaleWorkerWhenPoolBecomesIneligible verifies that
// a worker claimed by a failed resume attempt is released back to the free
// pool if, by the next resume attempt, the actor's worker_selector has
// changed such that the worker's pool is no longer eligible.
// Workflow:
//  1. Creates pool-a (tier=a) and pool-b (tier=b), and an actor narrowed to
//     tier=a.
//  2. Makes the fake atelet fail Run, then resumes: the actor gets assigned
//     to worker-a (the only eligible pool) and the resume fails after the
//     worker is claimed, leaving worker-a's actor_id set and the actor
//     stuck in RESUMING.
//  3. Updates the actor's selector to tier=b, making pool-a ineligible.
//  4. Resumes again; asserts it succeeds onto worker-b, and that worker-a
//     has been released (actor_id cleared) rather than left dangling.
func TestResumeActor_ReleasesStaleWorkerWhenPoolBecomesIneligible(t *testing.T) {
	ns := namespaceForTest("ns-resume-release-stale")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-a", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-b", map[string]string{"group": ns, "tier": "b"})
	createTemplateWithSelector(t, tc, ns, "tmpl1", &metav1.LabelSelector{
		MatchLabels: map[string]string{"group": ns},
	})
	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool-a")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool-b")

	id := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "a"}},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	tc.fakeAtelet.FailRun = fmt.Errorf("mock atelet failure")
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}})
	if err == nil {
		t.Fatalf("expected first ResumeActor (onto worker-a) to fail")
	}
	tc.fakeAtelet.FailRun = nil

	if _, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		ActorRef:       &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "b"}},
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}}); err != nil {
		t.Fatalf("second ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetActor().GetWorkerPoolName(); got != "pool-b" {
		t.Errorf("expected actor to land on pool-b, got worker_pool_name=%q", got)
	}
	if got := getResp.GetActor().GetStatus(); got != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("expected actor status RUNNING, got %v", got)
	}

	listResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	for _, w := range listResp.GetWorkers() {
		if w.GetWorkerNamespace() != ns {
			continue
		}
		switch w.GetWorkerPool() {
		case "pool-a":
			if wass := w.Assignment; wass != nil {
				got := "<nil-actor>"
				if wass.Actor != nil {
					got = wass.Actor.Name
				}
				t.Errorf("expected worker-a (now-ineligible pool-a) to be released, got actor_id=%q", got)
			}
		case "pool-b":
			if wass := w.Assignment; wass == nil {
				t.Errorf("expected worker-b to be claimed by %q, got nil assignment", id)
			} else {
				if wact := wass.Actor; wact == nil {
					t.Errorf("expected worker-b to be claimed by %q, got nil assignment.actor", id)
				} else {
					if got := wact.Name; got != id {
						t.Errorf("expected worker-b to be claimed by %q, got actor_id=%q", id, got)
					}
				}
			}
		}
	}
}

// TestUpdateActor_ReassignsPoolAcrossSuspendResume verifies that updating an
// actor's worker_selector moves it onto a different eligible pool not just
// on the next fresh resume, but also across a full suspend/resume cycle of
// an already-running actor.
// Workflow:
//  1. Creates two WorkerPools, pool-a (tier=a) and pool-b (tier=b), both
//     under the template's gating selector.
//  2. Creates an actor narrowed to tier=a and resumes it; asserts it lands on
//     pool-a/worker-a.
//  3. Updates the actor's selector to tier=b while it's still running.
//  4. Suspends then resumes the actor; asserts it now lands on
//     pool-b/worker-b, proving the updated selector — not the one in effect
//     when it was first scheduled — governs the new placement.
func TestUpdateActor_ReassignsPoolAcrossSuspendResume(t *testing.T) {
	ns := namespaceForTest("ns-update-actor-suspend-resume")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createWorkerPool(t, tc, ns, "pool-a", map[string]string{"group": ns, "tier": "a"})
	createWorkerPool(t, tc, ns, "pool-b", map[string]string{"group": ns, "tier": "b"})
	createTemplateWithSelector(t, tc, ns, "tmpl1", &metav1.LabelSelector{
		MatchLabels: map[string]string{"group": ns},
	})

	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool-a")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool-b")

	id := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "a"},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}}); err != nil {
		t.Fatalf("first ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetActor().GetWorkerPoolName(); got != "pool-a" {
		t.Fatalf("expected actor to first resume onto pool-a, got worker_pool_name=%q", got)
	}
	if got := getResp.GetActor().GetAteomPodName(); got != "worker-a" {
		t.Fatalf("expected actor to first resume onto worker-a, got ateom_pod_name=%q", got)
	}

	if _, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "b"},
		},
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if _, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}}); err != nil {
		t.Fatalf("second ResumeActor failed: %v", err)
	}

	getResp, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetActor().GetWorkerPoolName(); got != "pool-b" {
		t.Errorf("expected actor to resume onto pool-b after selector update, got worker_pool_name=%q", got)
	}
	if got := getResp.GetActor().GetAteomPodName(); got != "worker-b" {
		t.Errorf("expected actor to resume onto worker-b after selector update, got ateom_pod_name=%q", got)
	}
	if got := getResp.GetActor().GetStatus(); got != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("expected actor status RUNNING after second resume, got %v", got)
	}
}

// TestValidation tests the negative validation cases for all gRPC methods.
// Workflow:
// 1. Uses table-driven tests for each RPC method (CreateActor, GetActor, ResumeActor, SuspendActor).
// 2. Passes invalid requests (missing required fields).
// 3. Verifies that all requests fail with an error.
func TestValidation(t *testing.T) {
	ns := namespaceForTest("ns-validation")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	t.Run("CreateActor", func(t *testing.T) {
		tests := []struct {
			name    string
			req     *ateapipb.CreateActorRequest
			wantMsg string
		}{{
			"missing actor_template_namespace",
			&ateapipb.CreateActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"}, ActorTemplateName: "tmpl1"},
			"actor_template_namespace: Required value",
		}, {
			"invalid actor_template_namespace",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				ActorTemplateNamespace: "invalid value",
				ActorTemplateName:      "tmpl1",
			},
			"actor_template_namespace: Invalid value",
		}, {
			"missing actor_template_name",
			&ateapipb.CreateActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"}, ActorTemplateNamespace: "ns1"},
			"actor_template_name: Required value",
		}, {
			"invalid actor_template_name",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "invalid value",
			},
			"actor_template_name: Invalid value",
		}, {
			"missing actor_ref",
			&ateapipb.CreateActorRequest{ActorTemplateNamespace: "ns1", ActorTemplateName: "tmpl1"},
			"actor_ref: Required value",
		}, {
			"missing actor_ref.atespace",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Name: "id1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
			},
			"actor_ref.atespace: Required value",
		}, {
			"invalid actor_ref.atespace",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "NS1", Name: "id1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
			},
			"actor_ref.atespace: Invalid value",
		}, {
			"missing actor_ref.name",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "ns1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
			},
			"actor_ref.name: Required value",
		}, {
			"invalid actor_ref.name",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "ns1", Name: "ID1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
			},
			"actor_ref.name: Invalid value",
		}, {
			"invalid worker_selector label key",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
				WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "x"}},
			},
			`worker_selector.match_labels\[bad key!\]: Invalid value`,
		}, {
			"invalid worker_selector label value",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
				WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "not valid!"}},
			},
			`worker_selector.match_labels\[tier\]: Invalid value`,
		}, {
			"too many worker_selector.match_labels",
			&ateapipb.CreateActorRequest{
				ActorRef:               &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				ActorTemplateNamespace: "ns1",
				ActorTemplateName:      "tmpl1",
				WorkerSelector:         &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(11)}},
			"worker_selector.match_labels: Too many",
		}}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.client.CreateActor(context.Background(), tt.req)
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, tt.wantMsg)
			})
		}
	})

	t.Run("GetActor", func(t *testing.T) {
		tests := []struct {
			name    string
			req     *ateapipb.GetActorRequest
			wantMsg string
		}{{
			"missing actor_ref",
			&ateapipb.GetActorRequest{},
			"actor_ref: Required value",
		}, {
			"missing actor_ref.atespace",
			&ateapipb.GetActorRequest{
				ActorRef: &ateapipb.ActorRef{Name: "id1"},
			},
			"actor_ref.atespace: Required value",
		}, {
			"invalid actor_ref.atespace",
			&ateapipb.GetActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "NS1", Name: "id1"},
			},
			"actor_ref.atespace: Invalid value",
		}, {
			"missing actor_ref.name",
			&ateapipb.GetActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1"},
			},
			"actor_ref.name: Required value",
		}, {
			"invalid actor_ref.name",
			&ateapipb.GetActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "ID1"},
			},
			"actor_ref.name: Invalid value",
		}}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.client.GetActor(context.Background(), tt.req)
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, tt.wantMsg)
			})
		}
	})

	t.Run("ResumeActor", func(t *testing.T) {
		tests := []struct {
			name    string
			req     *ateapipb.ResumeActorRequest
			wantMsg string
		}{{
			"missing actor_ref",
			&ateapipb.ResumeActorRequest{},
			"actor_ref: Required value",
		}, {
			"missing actor_ref.atespace",
			&ateapipb.ResumeActorRequest{
				ActorRef: &ateapipb.ActorRef{Name: "id1"},
			},
			"actor_ref.atespace: Required value",
		}, {
			"invalid actor_ref.atespace",
			&ateapipb.ResumeActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "NS1", Name: "id1"},
			},
			"actor_ref.atespace: Invalid value",
		}, {
			"missing actor_ref.name",
			&ateapipb.ResumeActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1"},
			},
			"actor_ref.name: Required value",
		}, {
			"invalid actor_ref.name",
			&ateapipb.ResumeActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "ID1"},
			},
			"actor_ref.name: Invalid value",
		}}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.client.ResumeActor(context.Background(), tt.req)
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, tt.wantMsg)
			})
		}
	})

	t.Run("PauseActor", func(t *testing.T) {
		tests := []struct {
			name    string
			req     *ateapipb.PauseActorRequest
			wantMsg string
		}{{
			"missing actor_ref",
			&ateapipb.PauseActorRequest{},
			"actor_ref: Required value",
		}, {
			"missing actor_ref.atespace",
			&ateapipb.PauseActorRequest{
				ActorRef: &ateapipb.ActorRef{Name: "id1"},
			},
			"actor_ref.atespace: Required value",
		}, {
			"invalid actor_ref.atespace",
			&ateapipb.PauseActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "NS1", Name: "id1"},
			},
			"actor_ref.atespace: Invalid value",
		}, {
			"missing actor_ref.name",
			&ateapipb.PauseActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1"},
			},
			"actor_ref.name: Required value",
		}, {
			"invalid actor_ref.name",
			&ateapipb.PauseActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "ID1"},
			},
			"actor_ref.name: Invalid value",
		}}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.client.PauseActor(context.Background(), tt.req)
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, tt.wantMsg)
			})
		}
	})

	t.Run("SuspendActor", func(t *testing.T) {
		tests := []struct {
			name    string
			req     *ateapipb.SuspendActorRequest
			wantMsg string
		}{{
			"missing actor_ref",
			&ateapipb.SuspendActorRequest{},
			"actor_ref: Required value",
		}, {
			"missing actor_ref.atespace",
			&ateapipb.SuspendActorRequest{
				ActorRef: &ateapipb.ActorRef{Name: "id1"},
			},
			"actor_ref.atespace: Required value",
		}, {
			"invalid actor_ref.atespace",
			&ateapipb.SuspendActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "NS1", Name: "id1"},
			},
			"actor_ref.atespace: Invalid value",
		}, {
			"missing actor_ref.name",
			&ateapipb.SuspendActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1"},
			},
			"actor_ref.name: Required value",
		}, {
			"invalid actor_ref.name",
			&ateapipb.SuspendActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "ID1"},
			},
			"actor_ref.name: Invalid value",
		}}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.client.SuspendActor(context.Background(), tt.req)
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, tt.wantMsg)
			})
		}
	})

	t.Run("UpdateActor", func(t *testing.T) {
		tests := []struct {
			name    string
			req     *ateapipb.UpdateActorRequest
			wantMsg string
		}{{
			"missing actor_ref",
			&ateapipb.UpdateActorRequest{},
			"actor_ref: Required value",
		}, {
			"missing actor_ref.atespace",
			&ateapipb.UpdateActorRequest{
				ActorRef: &ateapipb.ActorRef{Name: "id1"},
			},
			"actor_ref.atespace: Required value",
		}, {
			"invalid actor_ref.atespace",
			&ateapipb.UpdateActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "NS1", Name: "id1"},
			},
			"actor_ref.atespace: Invalid value",
		}, {
			"missing actor_ref.name",
			&ateapipb.UpdateActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1"},
			},
			"actor_ref.name: Required value",
		}, {
			"invalid actor_ref.name",
			&ateapipb.UpdateActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "ID1"},
			},
			"actor_ref.name: Invalid value",
		}, {
			"invalid worker_selector label key",
			&ateapipb.UpdateActorRequest{
				ActorRef:       &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"bad key!": "x"}},
			},
			`worker_selector.match_labels\[bad key!\]: Invalid value`,
		}, {
			"invalid worker_selector label value",
			&ateapipb.UpdateActorRequest{
				ActorRef:       &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "not valid!"}},
			},
			`worker_selector.match_labels\[tier\]: Invalid value`,
		}, {
			"too many worker_selector.match_labels",
			&ateapipb.UpdateActorRequest{
				ActorRef:       &ateapipb.ActorRef{Atespace: "ns1", Name: "id1"},
				WorkerSelector: &ateapipb.Selector{MatchLabels: selectorLabelsOfSize(11)}},
			"worker_selector.match_labels: Too many",
		}}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.client.UpdateActor(context.Background(), tt.req)
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, tt.wantMsg)
			})
		}
	})

	t.Run("DeleteActor", func(t *testing.T) {
		tests := []struct {
			name    string
			req     *ateapipb.DeleteActorRequest
			wantMsg string
		}{{
			"missing actor_ref",
			&ateapipb.DeleteActorRequest{},
			"actor_ref: Required value",
		}, {
			"missing actor_ref.atespace",
			&ateapipb.DeleteActorRequest{
				ActorRef: &ateapipb.ActorRef{Name: "id1"},
			},
			"actor_ref.atespace: Required value",
		}, {
			"invalid actor_ref.atespace",
			&ateapipb.DeleteActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "NS1", Name: "id1"},
			},
			"actor_ref.atespace: Invalid value",
		}, {
			"missing actor_ref.name",
			&ateapipb.DeleteActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1"},
			},
			"actor_ref.name: Required value",
		}, {
			"invalid actor_ref.name",
			&ateapipb.DeleteActorRequest{
				ActorRef: &ateapipb.ActorRef{Atespace: "ns1", Name: "ID1"},
			},
			"actor_ref.name: Invalid value",
		}}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := tc.client.DeleteActor(context.Background(), tt.req)
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, tt.wantMsg)
			})
		}
	})
}

func TestResumeActor_LockConflict(t *testing.T) {
	ns := namespaceForTest("ns-resume-conflict")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	id := "id1"

	// Set a delay on the fake Atelet to hold the lock
	tc.fakeAtelet.RestoreDelay = 1 * time.Second

	// Launch Request A in a goroutine
	errChan := make(chan error, 1)
	go func() {
		_, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
			ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
		})
		errChan <- err
	}()

	// Sleep a bit to ensure Request A acquired the lock
	time.Sleep(200 * time.Millisecond)

	// Launch Request B (should fail due to lock conflict)
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	assertGrpcError(t, err, codes.Aborted, "another operation is in progress for this actor")

	// Wait for Request A to finish
	if errA := <-errChan; errA != nil {
		t.Fatalf("Request A failed: %v", errA)
	}
}

func TestResumeActor_DanglingWorker(t *testing.T) {
	ns := namespaceForTest("ns-resume-dangling")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	// 1. Create Worker Pod A
	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	id := "id1"

	// 2. Configure fake Atelet to FAIL on Restore!
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")

	// 3. Call ResumeActor -> Expect failure
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to atelet error")
	}

	// Verify actor state is RESUMING with worker A assigned
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	actor := getResp.GetActor()
	if actor.GetStatus() != ateapipb.Actor_STATUS_RESUMING {
		t.Fatalf("expected status RESUMING, got %v", actor.GetStatus())
	}
	if actor.GetAteomPodName() != "worker-a" {
		t.Fatalf("expected worker-a assigned, got %v", actor.GetAteomPodName())
	}

	deleteWorkerPod(t, tc, ns, "worker-a")

	// 6. Create Worker Pod B
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool1")

	// 7. Configure fake Atelet to SUCCEED on Restore
	tc.fakeAtelet.FailRestore = nil
	tc.fakeAtelet.RestoreCalled = false // reset

	// 8. Call ResumeActor again -> Expect success and picking Worker B!
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed on retry: %v", err)
	}

	if !tc.fakeAtelet.RestoreCalled {
		t.Errorf("expected Restore to be called on retry")
	}

	// Verify actor state is RUNNING with worker B assigned
	actor, err = tc.persistence.GetActor(context.Background(), testAtespace, id)
	if err != nil {
		t.Fatalf("failed to get actor from store: %v", err)
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("expected status RUNNING, got %v", actor.GetStatus())
	}
	if actor.GetAteomPodName() != "worker-b" {
		t.Errorf("expected worker-b assigned, got %v", actor.GetAteomPodName())
	}
}

func TestSuspendActor_DanglingWorker(t *testing.T) {
	ns := namespaceForTest("ns-sd")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	// 1. Create Worker Pod
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	id := "id1"

	// Resume first to make it running
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	deleteWorkerPod(t, tc, ns, "worker-1")

	// 3. Call SuspendActor -> Should succeed (our fix skips missing pod execution)
	actors, _, _ := tc.persistence.ListActors(context.Background(), testAtespace, maxPageSize, "")
	t.Logf("Actors in Redis before Suspend: %d", len(actors))
	for _, a := range actors {
		t.Logf("  Actor: %s/%s/%s", a.GetActorTemplateNamespace(), a.GetActorTemplateName(), a.GetActorId())
	}

	_, err = tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	// 4. Verify it becomes SUSPENDED in Redis
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: id},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if getResp.GetActor().GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("expected status SUSPENDED, got %v", getResp.GetActor().GetStatus())
	}
	if getResp.GetActor().GetAteomPodNamespace() != "" {
		t.Errorf("expected ateom_pod_namespace to be empty, got %v", getResp.GetActor().GetAteomPodNamespace())
	}
}

func TestDeleteActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-delete-success")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor id1 not found")
}

func TestDeleteActor_NotSuspended(t *testing.T) {
	ns := namespaceForTest("ns-delete-notsuspended")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "Actor id1 is not suspended (status: STATUS_RUNNING)")
}

func TestDeleteActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-delete-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: testAtespace, Name: "non-existent"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor non-existent not found")
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

func TestCreateActor_AtespaceNotFound(t *testing.T) {
	ns := namespaceForTest("ns-create-actor-no-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	// The template exists, but "missing-as" was never created. The template
	// check fires first, so reaching this error proves the atespace check ran.
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: "missing-as", Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace missing-as not found")
}

func TestCreateAtespace_Success(t *testing.T) {
	ns := namespaceForTest("ns-create-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	resp, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: "team-a"})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	got := resp.GetAtespace()
	if got.GetName() != "team-a" {
		t.Errorf("Name = %q, want team-a", got.GetName())
	}

	// An actor can now be created into the new atespace.
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: "team-a", Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}); err != nil {
		t.Errorf("CreateActor into freshly created atespace failed: %v", err)
	}
}

func TestCreateAtespace_AlreadyExists(t *testing.T) {
	ns := namespaceForTest("ns-create-atespace-dup")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: "team-a"}); err != nil {
		t.Fatalf("first CreateAtespace failed: %v", err)
	}
	_, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: "team-a"})
	assertGrpcError(t, err, codes.AlreadyExists, "Atespace team-a already exists")
}

func TestCreateAtespace_Validation(t *testing.T) {
	ns := namespaceForTest("ns-create-atespace-validation")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: ""})
	assertGrpcError(t, err, codes.InvalidArgument, "name is required")

	// Invalid names — uppercase/underscore plus Redis-key/SCAN metacharacters —
	// are rejected by ValidateAtespace before any key is built (injection guard).
	for _, bad := range []string{"Team_A", "a*", "a:b", "a/b"} {
		_, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: bad})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("CreateAtespace(%q): got code %v, want InvalidArgument (err=%v)", bad, status.Code(err), err)
		}
	}
}

func TestGetAtespace_Found(t *testing.T) {
	ns := namespaceForTest("ns-get-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	created, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: "team-a"})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	resp, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Name: "team-a"})
	if err != nil {
		t.Fatalf("GetAtespace failed: %v", err)
	}
	if diff := cmp.Diff(created.GetAtespace(), resp.GetAtespace(), protocmp.Transform()); diff != "" {
		t.Errorf("GetAtespace mismatch (-created +got):\n%s", diff)
	}
}

func TestGetAtespace_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-get-atespace-missing")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Name: "nope"})
	assertGrpcError(t, err, codes.NotFound, "Atespace nope not found")
}

func TestListAtespaces(t *testing.T) {
	ns := namespaceForTest("ns-list-atespaces")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	for _, n := range []string{"team-a", "team-b"} {
		if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: n}); err != nil {
			t.Fatalf("CreateAtespace(%s) failed: %v", n, err)
		}
	}
	resp, err := tc.client.ListAtespaces(context.Background(), &ateapipb.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("ListAtespaces failed: %v", err)
	}
	got := map[string]bool{}
	for _, a := range resp.GetAtespaces() {
		got[a.GetName()] = true
	}
	// setupTest seeds testAtespace; team-a and team-b were created above.
	for _, n := range []string{testAtespace, "team-a", "team-b"} {
		if !got[n] {
			t.Errorf("ListAtespaces missing %q; got %v", n, got)
		}
	}
}

func TestDeleteAtespace_Empty_Success(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-empty")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: "team-a"}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Name: "team-a"}); err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	_, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Name: "team-a"})
	assertGrpcError(t, err, codes.NotFound, "Atespace team-a not found")
}

func TestDeleteAtespace_NonEmpty_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-nonempty")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Name: "team-a"}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: "team-a", Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Name: "team-a"})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace team-a is not empty")
	// The atespace must survive a rejected delete.
	if _, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Name: "team-a"}); err != nil {
		t.Errorf("atespace should survive a rejected delete, got %v", err)
	}
}

// TestDeleteAtespace_ScopedToTargetAtespace pins (at the RPC layer) that the
// emptiness check is scoped to the target atespace: deleting an empty atespace
// succeeds even when a different atespace holds actors.
func TestDeleteAtespace_ScopedToTargetAtespace(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-scoped")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)
	createAtespace(t, tc, "team-a")
	createAtespace(t, tc, "team-b")

	// Actor only in team-b.
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: "team-b", Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Empty team-a deletes fine despite team-b holding an actor.
	if _, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Name: "team-a"}); err != nil {
		t.Errorf("DeleteAtespace(team-a, empty) failed: %v", err)
	}
	// team-b is still non-empty → rejected.
	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Name: "team-b"})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace team-b is not empty")
}

func TestDeleteAtespace_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-missing")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Name: "nope"})
	assertGrpcError(t, err, codes.NotFound, "Atespace nope not found")
}

func TestDeleteAtespace_Validation(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-validation")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Name: ""})
	assertGrpcError(t, err, codes.InvalidArgument, "name is required")

	// Metacharacter names are rejected before the emptiness glob scan ever runs.
	for _, bad := range []string{"a*", "a:b"} {
		_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Name: bad})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("DeleteAtespace(%q): got code %v, want InvalidArgument", bad, status.Code(err))
		}
	}
}
