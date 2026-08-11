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
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/testenv"
	"github.com/agent-substrate/substrate/internal/volume"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/redis/go-redis/v9"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

var (
	cfg        *rest.Config
	fakeAtelet = &FakeAteletServer{}
)

const (
	testAtespace = "test-atespace"
	testActorID  = "id1"
)

var (
	ignoreUID        = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid")
	ignoreVersion    = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "version")
	ignoreTimestamps = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "create_time", "update_time")
)

func TestMain(m *testing.M) {
	var stopEnv func()
	cfg, stopEnv = testenv.Start()

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

	// Create StorageClasses for volume tests
	_, err = k8sClient.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
		Provisioner: "substrate.io/mock",
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		log.Fatalf("create standard storage class: %v", err)
	}
	_, err = k8sClient.StorageV1().StorageClasses().Create(context.Background(), &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "fast"},
		Provisioner: "substrate.io/mock",
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		log.Fatalf("create fast storage class: %v", err)
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

	stopEnv()

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

	UploadCalled  bool
	UploadRequest *ateletpb.UploadPausedCheckpointRequest
	FailUpload    error
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

	f.UploadCalled = false
	f.UploadRequest = nil
	f.FailUpload = nil
}

func (f *FakeAteletServer) UploadPausedCheckpoint(ctx context.Context, req *ateletpb.UploadPausedCheckpointRequest) (*ateletpb.UploadPausedCheckpointResponse, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.UploadCalled = true
	f.UploadRequest = proto.Clone(req).(*ateletpb.UploadPausedCheckpointRequest)
	if f.FailUpload != nil {
		return nil, f.FailUpload
	}
	return &ateletpb.UploadPausedCheckpointResponse{}, nil
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
	scFactory := informers.NewSharedInformerFactory(k8sClient, 0)
	scLister := scFactory.Storage().V1().StorageClasses().Lister()

	substrateInformerFactory := externalversions.NewSharedInformerFactory(substrateClient, 0)
	actorTemplateLister := substrateInformerFactory.Api().V1alpha1().ActorTemplates().Lister()
	workerPoolLister := substrateInformerFactory.Api().V1alpha1().WorkerPools().Lister()
	sandboxConfigLister := substrateInformerFactory.Api().V1alpha1().SandboxConfigs().Lister()
	csiDriverConfigLister := substrateInformerFactory.Api().V1alpha1().CSIDriverConfigs().Lister()

	ctx, cancel := context.WithCancel(context.Background())

	syncer := NewWorkerPoolSyncer(persistence, workerInformer, workerPoolLister)
	syncer.Start(ctx)

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
		mr.Close()
		t.Fatalf("failed to start worker cache: %v", err)
	}

	dialer := NewAteletDialer(workerInformer.GetIndexer(), ateletInformer.GetIndexer(), "", "")
	// Dial the fake atelet over insecure transport instead of per-atelet mTLS,
	// so DialForWorker's real lookup/dial/cache path is exercised under test.
	dialer.dialCredentials = func(_ string) (credentials.TransportCredentials, error) {
		return insecure.NewCredentials(), nil
	}

	instruments, err := NewInstruments(sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader())).Meter("ateapi"))
	if err != nil {
		cancel()
		mr.Close()
		t.Fatalf("failed to create metric instruments: %v", err)
	}
	mockPlugin := volume.NewMockVolumePlugin()
	mockDriverName, err := mockPlugin.DriverName(ctx)
	if err != nil {
		t.Fatalf("failed to get mock driver name: %v", err)
	}
	volPlugins := map[string]volume.VolumePluginControlPlane{
		mockDriverName: mockPlugin,
	}
	service := NewService(persistence, wc, actorTemplateLister, workerPoolLister, sandboxConfigLister, csiDriverConfigLister, scLister, dialer, k8sClient, instruments, "", volPlugins)

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
	if _, err := client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: testAtespace}}}); err != nil {
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
		rdb.Close()
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
	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: name}}}); err != nil {
		t.Fatalf("CreateAtespace(%s) failed: %v", name, err)
	}
}

// createActorSnapshot seeds an ActorSnapshot in testAtespace directly through
// the store, so tag tests do not need a full resume/suspend lifecycle.
func createActorSnapshot(t *testing.T, tc *testContext, name string) *ateapipb.ObjectRef {
	t.Helper()
	if _, err := tc.persistence.CreateActorSnapshot(context.Background(), &ateapipb.ActorSnapshot{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		SnapshotUri: "gs://my-bucket/snapshots/" + testAtespace + "/" + name,
	}); err != nil {
		t.Fatalf("CreateActorSnapshot(%s) failed: %v", name, err)
	}
	return &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}
}

// tagActorSnapshot points tagName at snapshotRef with atespace scope.
func tagActorSnapshot(t *testing.T, tc *testContext, snapshotRef *ateapipb.ObjectRef, tagName string) *ateapipb.ActorSnapshotTag {
	t.Helper()
	tag, err := tc.client.TagActorSnapshot(context.Background(), &ateapipb.TagActorSnapshotRequest{
		Snapshot: &ateapipb.ActorSnapshotRef{Reference: &ateapipb.ActorSnapshotRef_Snapshot{Snapshot: snapshotRef}},
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if err != nil {
		t.Fatalf("TagActorSnapshot(%s) failed: %v", tagName, err)
	}
	return tag
}

// updateActorSnapshotTagScope sets tagName's scope, carrying meta as the
// optional uid/version preconditions.
func updateActorSnapshotTagScope(tc *testContext, tagName string, meta *ateapipb.ResourceMetadata, scope ateapipb.ActorSnapshotTagScope) (*ateapipb.ActorSnapshotTag, error) {
	meta.Atespace, meta.Name = testAtespace, tagName
	return tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
		Tag:        &ateapipb.ActorSnapshotTag{Metadata: meta, Scope: scope},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
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
	if _, err := tc.persistence.CreateActorSnapshot(context.Background(), &ateapipb.ActorSnapshot{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: goldenSnapshot},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      createdTemplate.GetName(),
		ActorTemplateUid:       string(createdTemplate.GetUID()),
		ContentScope:           ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		SnapshotUri:            "gs://fake-fake-fake/snapshots/" + resources.GoldenActorAtespace + "/" + goldenSnapshot,
	}); err != nil {
		t.Fatalf("failed to create golden ActorSnapshot: %v", err)
	}
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
	err := tc.k8sClient.CoreV1().Pods(ns).Delete(context.Background(), name, metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To[int64](0),
	})
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

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   testAtespace,
			Name:       "id1",
			Uid:        "caller-supplied-uid",
			Version:    999,
			CreateTime: timestamppb.New(time.Unix(1, 0)),
			UpdateTime: timestamppb.New(time.Unix(1, 0)),
		},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}},
		Status:                 ateapipb.Actor_STATUS_RUNNING,
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	want := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace, Version: 1},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}},
	}

	// The diff below ignores the server-assigned uid/timestamps (non-deterministic),
	// so assert they are populated separately — and that uid is server-generated,
	// not the caller-supplied value.
	md := createResp.GetMetadata()
	if md.GetUid() == "" {
		t.Errorf("CreateActor response missing server-assigned uid")
	}
	if md.GetUid() == "caller-supplied-uid" {
		t.Errorf("CreateActor echoed caller-supplied uid instead of generating one")
	}
	if md.GetCreateTime() == nil {
		t.Errorf("CreateActor response missing create_time")
	}
	if md.GetUpdateTime() == nil {
		t.Errorf("CreateActor response missing update_time")
	}

	if diff := cmp.Diff(want, createResp, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("CreateActor response mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateActor_WithExternalVolumes(t *testing.T) {
	ns := namespaceForTest("ns-create-ext-vols")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	volumes := []atev1alpha1.Volume{
		{
			Name: "ext-vol-1",
			VolumeSource: atev1alpha1.VolumeSource{
				ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					StorageClassName: "standard",
					Capacity:         resource.MustParse("10Gi"),
				},
			},
		},
	}
	mounts := []atev1alpha1.VolumeMount{
		{
			Name:      "ext-vol-1",
			MountPath: "/data",
		},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "vol-actor-1"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if len(createResp.GetActorVolumes()) != 1 {
		t.Fatalf("expected 1 volume in CreateActor response, got %d", len(createResp.GetActorVolumes()))
	}
	vol := createResp.GetActorVolumes()[0]
	if vol.GetVolumeName() != "ext-vol-1" {
		t.Errorf("volume name = %q, want %q", vol.GetVolumeName(), "ext-vol-1")
	}
	if vol.GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("volume status = %v, want %v", vol.GetStatus(), ateapipb.ExternalVolume_STATUS_PENDING)
	}
	if vol.GetStorageVolumeId() != "" {
		t.Errorf("expected empty storageVolumeId before resume, got %q", vol.GetStorageVolumeId())
	}

	// Verify GetActor returns the same external volume state
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "vol-actor-1"},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if len(getResp.GetActorVolumes()) != 1 {
		t.Fatalf("expected 1 volume in GetActor response, got %d", len(getResp.GetActorVolumes()))
	}
	if getResp.GetActorVolumes()[0].GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("GetActor status = %v, want %v", getResp.GetActorVolumes()[0].GetStatus(), ateapipb.ExternalVolume_STATUS_PENDING)
	}
}

func TestActorLifecycle_WithExternalVolumes(t *testing.T) {
	ns := namespaceForTest("ns-lifecycle-ext-vols")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	volumes := []atev1alpha1.Volume{
		{
			Name: "data-vol",
			VolumeSource: atev1alpha1.VolumeSource{
				ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					StorageClassName: "fast",
					Capacity:         resource.MustParse("20Gi"),
				},
			},
		},
	}
	mounts := []atev1alpha1.VolumeMount{
		{
			Name:      "data-vol",
			MountPath: "/mnt/data",
		},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	// 1. CreateActor
	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "actor-vol-lc"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if createResp.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Fatalf("expected initial status STATUS_SUSPENDED, got %v", createResp.GetStatus())
	}
	if len(createResp.GetActorVolumes()) != 1 || createResp.GetActorVolumes()[0].GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Fatalf("expected 1 pending volume after CreateActor, got %v", createResp.GetActorVolumes())
	}

	// 2. ResumeActor
	resumeResp, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if resumeResp.GetActor().GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Fatalf("expected status STATUS_RUNNING after resume, got %v", resumeResp.GetActor().GetStatus())
	}
	if len(resumeResp.GetActor().GetActorVolumes()) != 1 || resumeResp.GetActor().GetActorVolumes()[0].GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED {
		t.Fatalf("expected 1 created volume after ResumeActor, got %v", resumeResp.GetActor().GetActorVolumes())
	}
	if resumeResp.GetActor().GetActorVolumes()[0].GetStorageVolumeId() == "" {
		t.Fatalf("expected non-empty storageVolumeId after ResumeActor")
	}

	// 3. PauseActor
	pauseResp, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}
	if pauseResp.GetActor().GetStatus() != ateapipb.Actor_STATUS_PAUSED {
		t.Fatalf("expected status STATUS_PAUSED after pause, got %v", pauseResp.GetActor().GetStatus())
	}

	// 4. ResumeActor from paused
	resumeResp2, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("ResumeActor from paused failed: %v", err)
	}
	if resumeResp2.GetActor().GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Fatalf("expected status STATUS_RUNNING after second resume, got %v", resumeResp2.GetActor().GetStatus())
	}

	// 5. SuspendActor
	suspendResp, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if suspendResp.GetActor().GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Fatalf("expected status STATUS_SUSPENDED after suspend, got %v", suspendResp.GetActor().GetStatus())
	}

	// 6. DeleteActor
	deleteResp, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if deleteResp.GetMetadata().GetName() != "actor-vol-lc" {
		t.Errorf("deleted actor name = %q, want %q", deleteResp.GetMetadata().GetName(), "actor-vol-lc")
	}

	// Confirm GetActor returns NotFound after deletion
	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "actor-vol-lc"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete err = %v, want NotFound", err)
	}
}

type partialFailVolumePlugin struct {
	volume.VolumePluginControlPlane
	deleted []string
}

func (f *partialFailVolumePlugin) CreateVolume(ctx context.Context, name, capacity, driverName string, parameters map[string]string) (string, map[string]string, error) {
	if strings.HasSuffix(name, "fail-vol2") {
		return "", nil, fmt.Errorf("simulated volume creation failure")
	}
	return "storage-" + name, parameters, nil
}

func (f *partialFailVolumePlugin) AttachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (f *partialFailVolumePlugin) DetachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (f *partialFailVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	f.deleted = append(f.deleted, volumeID)
	return nil
}

// TestResumeActor_VolumeCreationFailure tests that when volume provisioning fails during ResumeActor,
// successfully created volumes are saved, the actor remains in STATUS_SUSPENDED,
// and that calling DeleteActor on the suspended actor cleans up all partially created volumes.
func TestResumeActor_VolumeCreationFailure(t *testing.T) {
	ns := namespaceForTest("ns-resume-vol-fail")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	volumes := []atev1alpha1.Volume{
		{
			Name: "succ-vol1",
			VolumeSource: atev1alpha1.VolumeSource{
				ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					StorageClassName: "standard",
					Capacity:         resource.MustParse("10Gi"),
				},
			},
		},
		{
			Name: "fail-vol2",
			VolumeSource: atev1alpha1.VolumeSource{
				ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					StorageClassName: "standard",
					Capacity:         resource.MustParse("10Gi"),
				},
			},
		},
	}
	mounts := []atev1alpha1.VolumeMount{
		{Name: "succ-vol1", MountPath: "/mnt/vol1"},
		{Name: "fail-vol2", MountPath: "/mnt/vol2"},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)

	// Inject a custom partial-failing VolumePlugin into global scope
	// TODO this doesn't support parallelism of test cases
	plugin := &partialFailVolumePlugin{}
	tc.service.volumePlugins = map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	}

	// Call CreateActor RPC directly
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "fail-actor"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
	})
	if err != nil {
		t.Fatalf("expected CreateActor to succeed, got: %v", err)
	}

	// Call ResumeActor RPC, which should trigger volume provisioning and fail on fail-vol2
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to volume creation error, but it succeeded")
	}

	// Verify GetActor returns the actor in STATUS_SUSPENDED status
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if getResp.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("actor status = %v, want %v", getResp.GetStatus(), ateapipb.Actor_STATUS_SUSPENDED)
	}

	actorUID := getResp.GetMetadata().GetUid()
	if actorUID == "" {
		t.Fatalf("expected non-empty UID on actor")
	}

	// Verify that succ-vol1 was updated to CREATED with a storageVolumeId, and fail-vol2 is still PENDING
	if len(getResp.GetActorVolumes()) != 2 {
		t.Fatalf("expected 2 volumes on actor, got %d", len(getResp.GetActorVolumes()))
	}
	volsByName := make(map[string]*ateapipb.ExternalVolume)
	for _, v := range getResp.GetActorVolumes() {
		volsByName[v.GetVolumeName()] = v
	}
	if v1, ok := volsByName["succ-vol1"]; !ok || v1.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || v1.GetStorageVolumeId() == "" {
		t.Errorf("succ-vol1 unexpected state: %v", v1)
	}
	if v2, ok := volsByName["fail-vol2"]; !ok || v2.GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("fail-vol2 unexpected state: %v", v2)
	}

	// Call DeleteActor on the actor in STATUS_SUSPENDED
	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	// Verify both volumes were deleted (succ-vol1 via storageID, fail-vol2 via fallback actorVolumeID)
	wantDeleted := []string{
		"storage-substrate-" + actorUID + "-succ-vol1",
		"substrate-" + actorUID + "-fail-vol2",
	}
	if diff := cmp.Diff(wantDeleted, plugin.deleted); diff != "" {
		t.Errorf("deleted volume IDs mismatch (-want +got):\n%s", diff)
	}

	// Confirm GetActor returns NotFound after deletion
	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "fail-actor"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after DeleteActor err = %v, want NotFound", err)
	}
}

type retrySuccessVolumePlugin struct {
	volume.VolumePluginControlPlane
	mu       sync.Mutex
	attempts int
	deleted  []string
}

func (r *retrySuccessVolumePlugin) CreateVolume(ctx context.Context, name, capacity, driverName string, parameters map[string]string) (string, map[string]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.HasSuffix(name, "retry-vol2") {
		r.attempts++
		if r.attempts == 1 {
			return "", nil, fmt.Errorf("simulated temporary volume creation failure")
		}
	}
	return "storage-" + name, parameters, nil
}

func (r *retrySuccessVolumePlugin) AttachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (r *retrySuccessVolumePlugin) DetachVolume(ctx context.Context, volumeID, node string) error {
	return nil
}

func (r *retrySuccessVolumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, volumeID)
	return nil
}

// TestResumeActor_VolumeCreationRetrySuccess tests that when volume provisioning fails on the first ResumeActor call,
// a subsequent call to ResumeActor retries provisioning only the pending volumes and succeeds.
func TestResumeActor_VolumeCreationRetrySuccess(t *testing.T) {
	ns := namespaceForTest("ns-resume-vol-retry")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	volumes := []atev1alpha1.Volume{
		{
			Name: "succ-vol1",
			VolumeSource: atev1alpha1.VolumeSource{
				ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					StorageClassName: "standard",
					Capacity:         resource.MustParse("10Gi"),
				},
			},
		},
		{
			Name: "retry-vol2",
			VolumeSource: atev1alpha1.VolumeSource{
				ExternalVolumeTemplate: &atev1alpha1.ExternalVolumeTemplate{
					StorageClassName: "standard",
					Capacity:         resource.MustParse("10Gi"),
				},
			},
		},
	}
	retryMounts := []atev1alpha1.VolumeMount{
		{Name: "succ-vol1", MountPath: "/mnt/vol1"},
		{Name: "retry-vol2", MountPath: "/mnt/vol2"},
	}
	createTemplateWithVolumes(t, tc, ns, volumes, retryMounts)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	plugin := &retrySuccessVolumePlugin{}
	tc.service.volumePlugins = map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	}

	// Call CreateActor RPC directly
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "retry-actor"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
	})
	if err != nil {
		t.Fatalf("expected CreateActor to succeed, got: %v", err)
	}

	// First call to ResumeActor RPC, which should fail on retry-vol2 (attempt 1)
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err == nil {
		t.Fatalf("expected first ResumeActor to fail due to temporary volume creation error, but it succeeded")
	}

	// Verify GetActor returns the actor in STATUS_SUSPENDED status with succ-vol1 created and retry-vol2 pending
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("GetActor after first resume failed: %v", err)
	}
	if getResp.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("actor status after first resume = %v, want %v", getResp.GetStatus(), ateapipb.Actor_STATUS_SUSPENDED)
	}

	volsByName := make(map[string]*ateapipb.ExternalVolume)
	for _, v := range getResp.GetActorVolumes() {
		volsByName[v.GetVolumeName()] = v
	}
	if v1, ok := volsByName["succ-vol1"]; !ok || v1.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || v1.GetStorageVolumeId() == "" {
		t.Errorf("succ-vol1 unexpected state after first resume: %v", v1)
	}
	if v2, ok := volsByName["retry-vol2"]; !ok || v2.GetStatus() != ateapipb.ExternalVolume_STATUS_PENDING {
		t.Errorf("retry-vol2 unexpected state after first resume: %v", v2)
	}

	// Second call to ResumeActor RPC, which should succeed on retry-vol2 (attempt 2)
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("expected second ResumeActor to succeed, got: %v", err)
	}

	// Verify GetActor returns the actor in STATUS_RUNNING status with both volumes CREATED
	getResp, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("GetActor after second resume failed: %v", err)
	}
	if getResp.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("actor status after second resume = %v, want %v", getResp.GetStatus(), ateapipb.Actor_STATUS_RUNNING)
	}
	for _, v := range getResp.GetActorVolumes() {
		if v.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || v.GetStorageVolumeId() == "" {
			t.Errorf("volume %s unexpected state after second resume: %v", v.GetVolumeName(), v)
		}
	}

	// Clean up by suspending and deleting the actor
	_, err = tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retry-actor"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
}

// TestCreateActor_TemplateNotFound tests that creating an actor with a non-existent template fails with FailedPrecondition.
func TestCreateActor_TemplateNotFound(t *testing.T) {
	ns := namespaceForTest("ns-create-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "non-existent",
	}})
	assertGrpcError(t, err, codes.FailedPrecondition, fmt.Sprintf("ActorTemplate %s/non-existent not found", ns))
}

// TestCreateActor_Duplicate tests that creating an actor with an existing ID fails.
func TestCreateActor_Duplicate(t *testing.T) {
	ns := namespaceForTest("ns-create-dup")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("first CreateActor failed: %v", err)
	}

	_, err = tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	assertGrpcError(t, err, codes.AlreadyExists, "Actor id1 already exists")
}

// TestGetActor_Found tests that an existing actor can be retrieved.
func TestGetActor_Found(t *testing.T) {
	ns := namespaceForTest("ns-get-found")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	name := "id1"

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}

	want := createResp

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
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "non-existent"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/non-existent not found")
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

	resp1, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor 1 failed: %v", err)
	}
	resp2, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id2"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
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
		resp1,
		resp2,
	}

	opts := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool {
			return a.GetMetadata().GetName() < b.GetMetadata().GetName()
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

	create := func(atespace, name string) *ateapipb.Actor {
		resp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		}})
		if err != nil {
			t.Fatalf("CreateActor(%s, atespace=%q) failed: %v", name, atespace, err)
		}
		return resp
	}
	a1 := create("team-a", "id1")
	a2 := create("team-a", "id2")
	b1 := create("team-b", "id3")

	sortByID := []cmp.Option{
		protocmp.Transform(),
		cmpopts.SortSlices(func(a, b *ateapipb.Actor) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() }),
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
	if _, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "id1"}}); err != nil {
		t.Errorf("GetActor(id1, team-a) failed: %v", err)
	}
	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/id1 not found")
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

	create := func(atespace, name string) {
		if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		}}); err != nil {
			t.Fatalf("CreateActor(%s, atespace=%q) failed: %v", name, atespace, err)
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
		got[a.GetMetadata().GetName()] = a.GetMetadata().GetAtespace()
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
		resp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("name%d", i)},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		}})
		if err != nil {
			t.Fatalf("CreateActor %d failed: %v", i, err)
		}
		want = append(want, resp)
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
			return a.GetMetadata().GetName() < b.GetMetadata().GetName()
		}),
	}

	if diff := cmp.Diff(want, allActors, opts...); diff != "" {
		t.Errorf("ListActors pagination response mismatch (-want +got):\n%s", diff)
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
			State:           ateapipb.Worker_STATE_ACTIVE,
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

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	if !tc.fakeAtelet.RestoreCalled {
		t.Errorf("expected Restore to be called")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: name, Atespace: testAtespace},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_RUNNING,
		WorkerAssignment: &ateapipb.WorkerAssignment{
			WorkerNamespace: ns,
			WorkerPool:      "pool1",
			WorkerPod:       "worker-1",
			WorkerPodIp:     "127.0.0.1",
		},
	}
	if diff := cmp.Diff(want, getResp, protocmp.Transform(), ignoreUID, ignoreVersion, ignoreTimestamps, protocmp.IgnoreFields(&ateapipb.WorkerAssignment{}, "worker_pod_uid")); diff != "" {
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
			Actor: &ateapipb.ObjectRef{
				Name:     name,
				Atespace: testAtespace,
			},
			ActorUid: getResp.GetMetadata().GetUid(),
		},
		Ip:           "127.0.0.1",
		NodeName:     "node1",
		SandboxClass: "gvisor",
		Labels:       map[string]string{poolLabelKey: ns},
		State:        ateapipb.Worker_STATE_ACTIVE,
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

	_, err = tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
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

	createResp, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	name := createResp.GetMetadata().GetName()

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
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

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "b"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPod(); got != "worker-b" {
		t.Errorf("expected actor to be assigned to worker-b (pool-b, matching narrowed selector), got %q", got)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPool(); got != "pool-b" {
		t.Errorf("expected actor's worker_assignment.worker_pool to be pool-b, got %q", got)
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

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "b"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPool(); got != "pool-both" {
		t.Errorf("expected actor to be assigned to pool-both (the only pool matching both selectors), got worker_assignment.worker_pool=%q", got)
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

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// STEP 1: Make Atelet FAIL on Restore!
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to atelet error")
	}

	// Verify actor state is RESUMING in Redis!
	actor, err := tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
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
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed on retry: %v", err)
	}

	if !tc.fakeAtelet.RestoreCalled {
		t.Errorf("expected Restore to be called on retry")
	}

	// Verify actor state is RUNNING!
	actor, err = tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
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
	name := "id1"

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Resume first to make it running
	running, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	// Suspend
	suspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	if !tc.fakeAtelet.CheckpointCalled {
		t.Errorf("expected atelet Checkpoint to be called")
	}
	ref := suspended.GetActor().GetLatestSnapshot()
	if ref.GetName() == "" {
		t.Fatalf("SuspendActor returned no ActorSnapshot reference: %v", suspended)
	}
	snapshotRef := &ateapipb.ActorSnapshotRef{Reference: &ateapipb.ActorSnapshotRef_Snapshot{Snapshot: ref}}
	snapshot, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{Snapshot: snapshotRef})
	if err != nil {
		t.Fatalf("GetActorSnapshot failed: %v", err)
	}
	if got := snapshot.GetSourceActorVersion(); got != running.GetActor().GetMetadata().GetVersion() {
		t.Errorf("snapshot source version = %d, want %d", got, running.GetActor().GetMetadata().GetVersion())
	}
	listed, err := tc.client.ListActorSnapshots(context.Background(), &ateapipb.ListActorSnapshotsRequest{Atespace: testAtespace, PageSize: 1})
	if err != nil || len(listed.GetSnapshots()) != 1 {
		t.Fatalf("ListActorSnapshots = (%v, %v), want one", listed, err)
	}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "untagged-clone"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
		SourceSnapshot: snapshotRef,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("untagged CreateActor status = %v, want FailedPrecondition", status.Code(err))
	}
	tagRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "before-upgrade"}
	tagged, err := tc.client.TagActorSnapshot(context.Background(), &ateapipb.TagActorSnapshotRequest{
		Snapshot: snapshotRef,
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "before-upgrade"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if err != nil || !proto.Equal(tagged.GetSnapshot(), ref) {
		t.Fatalf("TagActorSnapshot = (%v, %v), want tag for snapshot", tagged, err)
	}
	if _, err := tc.client.TagActorSnapshot(context.Background(), &ateapipb.TagActorSnapshotRequest{
		Snapshot: snapshotRef,
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "other", Name: "cross-atespace"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-atespace TagActorSnapshot status = %v, want FailedPrecondition", status.Code(err))
	}
	snapshotTagRef := &ateapipb.ActorSnapshotRef{Reference: &ateapipb.ActorSnapshotRef_Tag{Tag: tagRef}}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: "other", Name: "cross-atespace"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
		SourceSnapshot: snapshotTagRef,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cross-atespace CreateActor status = %v, want FailedPrecondition", status.Code(err))
	}
	updated, err := tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: tagRef.GetAtespace(), Name: tagRef.GetName()},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
	if err != nil || updated.GetScope() != ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED {
		t.Fatalf("UpdateActorSnapshotTag = (%v, %v), want published", updated, err)
	}
	if got, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{Snapshot: snapshotTagRef}); err != nil || got.GetMetadata().GetUid() != snapshot.GetMetadata().GetUid() {
		t.Fatalf("tag after publication = (%v, %v), want same address and snapshot", got, err)
	}
	createAtespace(t, tc, "other")
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: "other", Name: "cross-atespace"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
		SourceSnapshot: snapshotTagRef,
	}); err != nil {
		t.Fatalf("CreateActor from published tag failed: %v", err)
	}

	clone, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "clone"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
		SourceSnapshot: snapshotTagRef,
	})
	if err != nil {
		t.Fatalf("CreateActor from snapshot failed: %v", err)
	}
	if !proto.Equal(clone.GetLatestSnapshot(), ref) {
		t.Fatalf("clone latest snapshot = %v, want %v", clone.GetLatestSnapshot(), ref)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "clone"}}); err != nil {
		t.Fatalf("ResumeActor clone failed: %v", err)
	}
	if !tc.fakeAtelet.RestoreCalled {
		t.Error("resuming clone did not restore its source ActorSnapshot")
	}
	cloneSuspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "clone"}})
	if err != nil {
		t.Fatalf("SuspendActor clone failed: %v", err)
	}
	if cloneSuspended.GetActor().GetLatestSnapshot().GetName() == ref.GetName() {
		t.Fatal("clone suspension reused its source snapshot")
	}
	listed, err = tc.client.ListActorSnapshots(context.Background(), &ateapipb.ListActorSnapshotsRequest{Atespace: testAtespace})
	if err != nil || len(listed.GetSnapshots()) != 2 {
		t.Fatalf("ListActorSnapshots after clone suspension = (%v, %v), want two", listed, err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: name, Atespace: testAtespace},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
	}

	if diff := cmp.Diff(want, getResp,
		protocmp.Transform(),
		ignoreUID,
		ignoreVersion,
		ignoreTimestamps,
		protocmp.IgnoreFields(&ateapipb.Actor{}, "latest_snapshot"),
	); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
	if _, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("DeleteActor source failed: %v", err)
	}
	if _, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{Snapshot: snapshotTagRef}); err != nil {
		t.Fatalf("source snapshot disappeared with source Actor: %v", err)
	}
	if deleted, err := tc.client.DeleteActorSnapshotTag(context.Background(), &ateapipb.DeleteActorSnapshotTagRequest{Tag: tagRef}); err != nil || deleted.GetMetadata().GetName() != tagRef.GetName() {
		t.Fatalf("DeleteActorSnapshotTag = (%v, %v)", deleted, err)
	}
	if _, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{Snapshot: snapshotTagRef}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted tag status = %v, want NotFound", status.Code(err))
	}
	if _, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{Snapshot: snapshotRef}); err != nil {
		t.Fatalf("snapshot metadata disappeared after tag deletion: %v", err)
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

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Resume first to make it running
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	// Pause
	_, err = tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}

	if !tc.fakeAtelet.CheckpointCalled {
		t.Errorf("expected atelet Checkpoint to be called")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	want := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: name, Atespace: testAtespace},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_PAUSED,
		LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
			NodeVmsWithLocalSnapshots: []string{"node1"},
			ContentScope:              ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		},
	}

	if diff := cmp.Diff(want, getResp,
		protocmp.Transform(),
		ignoreUID,
		ignoreVersion,
		ignoreTimestamps,
		protocmp.IgnoreFields(&ateapipb.WorkerAssignment{}, "worker_pod_uid"),
		protocmp.IgnoreFields(&ateapipb.LocalSnapshotInfo{}, "snapshot_name"),
	); diff != "" {
		t.Errorf("GetActor response mismatch (-want +got):\n%s", diff)
	}
	if getResp.GetLocalSnapshotInfo().GetSnapshotName() == "" {
		t.Error("LocalSnapshotInfo.SnapshotName is empty, want the name the pause checkpointed under")
	}
}

// TestUpdateActor_Success verifies UpdateActor replaces the actor's
// worker_selector and that the change is durably persisted.
func TestUpdateActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-update-actor")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "free"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	updateResp, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
			WorkerSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			// Output-only fields outside the mask are ignored.
			Status: ateapipb.Actor_STATUS_RUNNING,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	})
	if err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	wantActor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Name: "id1", Atespace: testAtespace, Version: 2},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "paid"},
		},
	}
	if diff := cmp.Diff(wantActor, updateResp, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	wantGetResp := wantActor
	if diff := cmp.Diff(wantGetResp, getResp, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("GetActor response mismatch after UpdateActor (-want +got):\n%s", diff)
	}
}

// TestUpdateActor_Preconditions verifies the optional version and uid guards
// carried in the embedded resource's metadata.
func TestUpdateActor_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-update-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	ctx := context.Background()
	createActor := func() *ateapipb.Actor {
		t.Helper()
		actor, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		}})
		if err != nil {
			t.Fatalf("CreateActor failed: %v", err)
		}
		return actor
	}

	update := func(meta *ateapipb.ResourceMetadata, tier string) (*ateapipb.Actor, error) {
		meta.Atespace, meta.Name = testAtespace, testActorID
		return tc.client.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata:       meta,
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": tier}},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
		})
	}

	// Delete and recreate the same atespace/name actor, so the first lifecycle's uid
	// becomes stale.
	staleUID := createActor().GetMetadata().GetUid()
	if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: testActorID},
	}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	created := createActor()
	staleVersion := created.GetMetadata().GetVersion()
	uid := created.GetMetadata().GetUid()
	if uid == staleUID {
		t.Fatalf("recreated actor reused uid %s, want a fresh one", uid)
	}
	// The uid from the deleted lifecycle must be rejected, even though the
	// atespace/name it was observed under still resolves.
	_, err := update(&ateapipb.ResourceMetadata{Uid: staleUID}, "other-lifecycle")
	assertGrpcError(t, err, codes.Aborted, fmt.Sprintf("actor %s/%s not found with uid %s", testAtespace, testActorID, staleUID))

	// An unguarded update is last-writer-wins, and moves the resource past the
	// version observed above.
	unguarded, err := update(&ateapipb.ResourceMetadata{}, "free")
	if err != nil {
		t.Fatalf("UpdateActor(no guards) failed: %v", err)
	}
	currentVersion := unguarded.GetMetadata().GetVersion()
	if currentVersion <= staleVersion {
		t.Fatalf("version = %d, want greater than %d after an update", currentVersion, staleVersion)
	}
	if got := unguarded.GetWorkerSelector().GetMatchLabels()["tier"]; got != "free" {
		t.Errorf("worker_selector[tier] = %q, want free", got)
	}

	// The version observed before that write is now stale: rejected rather than
	// silently overwriting the concurrent change.
	_, err = update(&ateapipb.ResourceMetadata{Version: staleVersion}, "stale")
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	// Both uid and version matching the observed state: the update goes through.
	updated, err := update(&ateapipb.ResourceMetadata{Uid: uid, Version: currentVersion}, "paid")
	if err != nil {
		t.Fatalf("UpdateActor(matching guards) failed: %v", err)
	}
	if got := updated.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker_selector[tier] = %q, want paid", got)
	}
	if updated.GetMetadata().GetVersion() <= currentVersion {
		t.Errorf("version = %d, want greater than %d", updated.GetMetadata().GetVersion(), currentVersion)
	}

	// The guard the client just satisfied is now stale in turn.
	_, err = update(&ateapipb.ResourceMetadata{Version: currentVersion}, "free")
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
}

func TestUpdateActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-update-actor-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor:      &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "does-not-exist"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	})
	assertGrpcError(t, err, codes.NotFound, "actor test-atespace/does-not-exist not found")
}

func TestUpdateActorSnapshotTag_Success(t *testing.T) {
	ns := namespaceForTest("ns-update-tag")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	ctx := context.Background()
	const snapshotName, tagName = "snapshot-1", "before-upgrade"
	snapshotRef := createActorSnapshot(t, tc, snapshotName)
	tagActorSnapshot(t, tc, snapshotRef, tagName)

	updateResp, err := tc.client.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
			Snapshot: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "some-other-snapshot"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag failed: %v", err)
	}

	wantTag := &ateapipb.ActorSnapshotTag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName, Version: 2},
		Snapshot: snapshotRef,
		Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
	}
	if diff := cmp.Diff(wantTag, updateResp, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("UpdateActorSnapshotTag response mismatch (-want +got):\n%s", diff)
	}

	_, storedTag, err := tc.persistence.GetActorSnapshotByTag(ctx, testAtespace, tagName)
	if err != nil {
		t.Fatalf("GetActorSnapshotByTag failed: %v", err)
	}
	if diff := cmp.Diff(wantTag, storedTag, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
		t.Errorf("stored tag mismatch after UpdateActorSnapshotTag (-want +got):\n%s", diff)
	}
}

// TestUpdateActorSnapshotTag_Preconditions verifies the optional version and uid
// guards carried in the tag's metadata.
func TestUpdateActorSnapshotTag_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	ctx := context.Background()
	const snapshotName, tagName = "snapshot-1", "before-upgrade"
	snapshotRef := createActorSnapshot(t, tc, snapshotName)

	// Each call to update() flips the scope, so every accepted update is an
	// observable write that bumps the version.
	update := func(meta *ateapipb.ResourceMetadata, scope ateapipb.ActorSnapshotTagScope) (*ateapipb.ActorSnapshotTag, error) {
		return updateActorSnapshotTagScope(tc, tagName, meta, scope)
	}

	// Delete and recreate the same atespace/name tag, so the first lifecycle's
	// uid becomes stale.
	staleUID := tagActorSnapshot(t, tc, snapshotRef, tagName).GetMetadata().GetUid()
	if _, err := tc.client.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{
		Tag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: tagName},
	}); err != nil {
		t.Fatalf("DeleteActorSnapshotTag failed: %v", err)
	}

	tagged := tagActorSnapshot(t, tc, snapshotRef, tagName)
	staleVersion := tagged.GetMetadata().GetVersion()
	uid := tagged.GetMetadata().GetUid()
	if uid == staleUID {
		t.Fatalf("recreated tag reused uid %s, want a fresh one", uid)
	}
	// The uid from the deleted lifecycle must be rejected, even though the
	// atespace/name it was observed under still resolves.
	_, err := update(&ateapipb.ResourceMetadata{Uid: staleUID}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, fmt.Sprintf("ActorSnapshot tag %s/%s has uid %s, not %s", testAtespace, tagName, uid, staleUID))

	// An unguarded update is last-writer-wins, and moves the tag past the
	// version observed above.
	unguarded, err := update(&ateapipb.ResourceMetadata{}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(no guards) failed: %v", err)
	}
	currentVersion := unguarded.GetMetadata().GetVersion()
	if currentVersion <= staleVersion {
		t.Fatalf("version = %d, want greater than %d after an update", currentVersion, staleVersion)
	}
	if got, want := unguarded.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}

	// The version observed before that write is now stale: rejected rather than
	// silently overwriting the concurrent change.
	_, err = update(&ateapipb.ResourceMetadata{Version: staleVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	// Both uid and version matching the observed state: the update goes through.
	updated, err := update(&ateapipb.ResourceMetadata{Uid: uid, Version: currentVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE)
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(matching guards) failed: %v", err)
	}
	if got, want := updated.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}
	if updated.GetMetadata().GetVersion() <= currentVersion {
		t.Errorf("version = %d, want greater than %d", updated.GetMetadata().GetVersion(), currentVersion)
	}

	// The guard the client just satisfied is now stale in turn.
	_, err = update(&ateapipb.ResourceMetadata{Version: currentVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
}

// TestUpdateActorSnapshotTag_ClearsMaskedField verifies that a masked field
// left unset on the request resets to its default. ATESPACE is the zero value
// of ActorSnapshotTagScope, so masking scope without setting it unpublishes the
// tag.
func TestUpdateActorSnapshotTag_ClearsMaskedField(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-clear")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	const tagName = "before-upgrade"
	snapshotRef := createActorSnapshot(t, tc, "snapshot-1")
	tagActorSnapshot(t, tc, snapshotRef, tagName)

	published, err := updateActorSnapshotTagScope(tc, tagName, &ateapipb.ResourceMetadata{}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(publish) failed: %v", err)
	}
	if got, want := published.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Fatalf("scope = %v, want %v", got, want)
	}

	cleared, err := tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
		Tag:        &ateapipb.ActorSnapshotTag{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: tagName}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(masked clear) failed: %v", err)
	}
	if got, want := cleared.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("scope = %v, want %v after masked clear", got, want)
	}
}

func TestUpdateActorSnapshotTag_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "does-not-exist"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
	assertGrpcError(t, err, codes.NotFound, "ActorSnapshot tag test-atespace/does-not-exist not found")
}

// TestResumeActor_ReleasesStaleWorkerWhenPoolBecomesIneligible verifies that
// a worker claimed by a failed resume attempt is released back to the free
// pool if, by the next resume attempt, the actor's worker_selector has
// changed such that the worker's pool is no longer eligible. The actor
// itself is crashed rather than transparently migrated to another pool.
// Workflow:
//  1. Creates pool-a (tier=a) and pool-b (tier=b), and an actor narrowed to
//     tier=a.
//  2. Makes the fake atelet fail Run, then resumes: the actor gets assigned
//     to worker-a (the only eligible pool) and the resume fails after the
//     worker is claimed, leaving worker-a's actor assignment set and the actor
//     stuck in RESUMING.
//  3. Updates the actor's selector to tier=b, making pool-a ineligible.
//  4. Resumes again; asserts it fails and the actor is CRASHED, that worker-a
//     has been released (actor assignment cleared) rather than left dangling,
//     and that worker-b remains free (the crashed actor must not claim it).
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

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector:         &ateapipb.Selector{MatchLabels: map[string]string{"tier": "a"}},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	tc.fakeAtelet.FailRun = fmt.Errorf("mock atelet failure")
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err == nil {
		t.Fatalf("expected first ResumeActor (onto worker-a) to fail")
	}
	tc.fakeAtelet.FailRun = nil

	if _, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "b"}},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err == nil {
		t.Fatalf("expected second ResumeActor to fail: the assigned worker's pool is no longer eligible")
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus(); got != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("expected actor status CRASHED, got %v", got)
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
				t.Errorf("expected worker-a (now-ineligible pool-a) to be released, got actor name=%q", got)
			}
		case "pool-b":
			if wass := w.Assignment; wass != nil {
				got := "<nil-actor>"
				if wass.Actor != nil {
					got = wass.Actor.Name
				}
				t.Errorf("expected worker-b to stay free (actor crashed, not migrated), got actor name=%q", got)
			}
		}
	}
}

// TestResumeActor_ReleasesDrainingWorkerFromPriorAttempt exercises the reuse-loop
// change in AssignWorkerStep.Execute: a worker still assigned to the actor from a
// previous (failed) attempt that has since entered DRAINING must not be reused —
// it is released and the actor is crashed.
func TestResumeActor_CrashesIfAssignedWorkerIsDraining(t *testing.T) {
	ns := namespaceForTest("ns-resume-release-draining")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	// createTemplate sets up pool1 (labeled pool=<ns>) + tmpl1 (selecting it) with
	// a golden snapshot, so resume drives Restore. Two workers share the pool.
	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-a", "node1", "pool1")
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool1")

	id := "id1"
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: testAtespace,
				Name:     id,
			},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		},
	}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// First resume fails after a worker is assigned, leaving the actor bound to
	// that worker from a prior attempt.
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}}); err == nil {
		t.Fatalf("expected first ResumeActor to fail")
	}
	tc.fakeAtelet.FailRestore = nil

	// Learn which worker got assigned (findFreeWorker shuffles), then mark it
	// DRAINING as the syncer would when its pod enters Terminating.
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	assignedPod := getResp.GetWorkerAssignment().GetWorkerPod()
	if assignedPod == "" {
		t.Fatalf("expected actor to be bound to a worker after the failed attempt")
	}

	assigned, err := tc.persistence.GetWorker(context.Background(), ns, "pool1", assignedPod)
	if err != nil {
		t.Fatalf("GetWorker(%s) failed: %v", assignedPod, err)
	}
	assigned.State = ateapipb.Worker_STATE_DRAINING
	if err := tc.persistence.UpdateWorker(context.Background(), assigned, assigned.GetVersion()); err != nil {
		t.Fatalf("marking worker %s draining failed: %v", assignedPod, err)
	}

	// Wait until the DRAINING state is observable, which also gives the store
	// watch time to propagate it into the scheduler's worker cache.
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		resp, err := tc.client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
		if err != nil {
			return false, nil
		}
		for _, w := range resp.GetWorkers() {
			if w.GetWorkerNamespace() == ns && w.GetWorkerPod() == assignedPod {
				return w.GetState() == ateapipb.Worker_STATE_DRAINING, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatalf("worker %s did not reach DRAINING: %v", assignedPod, err)
	}

	// Second resume must fail and crash the actor because its worker is draining.
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}})
	if err == nil {
		t.Fatalf("expected second ResumeActor to fail")
	}
	if status.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "crashed") {
		t.Errorf("expected Aborted/crashed error, got %v", err)
	}

	getResp, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetStatus(); got != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("expected actor status CRASHED, got %v", got)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPod(); got != "" {
		t.Errorf("expected actor pod name to be empty, got %q", got)
	}

	// The draining worker must have been released.
	listResp, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("ListWorkers failed: %v", err)
	}
	for _, w := range listResp.GetWorkers() {
		if w.GetWorkerNamespace() != ns {
			continue
		}
		if w.GetWorkerPod() == assignedPod {
			if w.GetAssignment() != nil {
				t.Errorf("expected draining worker %q to be released, still assigned to %q", assignedPod, w.GetAssignment().GetActor().GetName())
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

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
		WorkerSelector: &ateapipb.Selector{
			MatchLabels: map[string]string{"tier": "a"},
		},
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("first ResumeActor failed: %v", err)
	}

	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPool(); got != "pool-a" {
		t.Fatalf("expected actor to first resume onto pool-a, got worker_assignment.worker_pool=%q", got)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPod(); got != "worker-a" {
		t.Fatalf("expected actor to first resume onto worker-a, got worker_assignment.worker_pod=%q", got)
	}

	if _, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			WorkerSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "b"},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"worker_selector"}},
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	if _, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
		t.Fatalf("second ResumeActor failed: %v", err)
	}

	getResp, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPool(); got != "pool-b" {
		t.Errorf("expected actor to resume onto pool-b after selector update, got worker_assignment.worker_pool=%q", got)
	}
	if got := getResp.GetWorkerAssignment().GetWorkerPod(); got != "worker-b" {
		t.Errorf("expected actor to resume onto worker-b after selector update, got worker_assignment.worker_pod=%q", got)
	}
	if got := getResp.GetStatus(); got != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("expected actor status RUNNING after second resume, got %v", got)
	}
}

func TestValidation(t *testing.T) {
	ns := namespaceForTest("ns-validation")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	t.Run("CreateActor", func(t *testing.T) {
		_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("GetActor", func(t *testing.T) {
		_, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("ResumeActor", func(t *testing.T) {
		_, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("PauseActor", func(t *testing.T) {
		_, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("SuspendActor", func(t *testing.T) {
		_, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("UpdateActor", func(t *testing.T) {
		_, err := tc.client.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("DeleteActor", func(t *testing.T) {
		_, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "actor: Required value")
	})

	t.Run("ListActors", func(t *testing.T) {
		_, err := tc.client.ListActors(context.Background(), &ateapipb.ListActorsRequest{PageSize: -1})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})

	t.Run("ListWorkers", func(t *testing.T) {
		_, err := tc.client.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{PageSize: -1})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})

	t.Run("ListAtespaces", func(t *testing.T) {
		_, err := tc.client.ListAtespaces(context.Background(), &ateapipb.ListAtespacesRequest{PageSize: -1})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})

	t.Run("CreateAtespace", func(t *testing.T) {
		_, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Required value")
	})

	t.Run("GetAtespace", func(t *testing.T) {
		_, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Required value")
	})

	t.Run("DeleteAtespace", func(t *testing.T) {
		_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Required value")
	})
}

func TestResumeActor_LockConflict(t *testing.T) {
	ns := namespaceForTest("ns-resume-conflict")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Set a delay on the fake Atelet to hold the lock
	tc.fakeAtelet.RestoreDelay = 1 * time.Second

	// Launch Request A in a goroutine
	errChan := make(chan error, 1)
	go func() {
		_, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
		})
		errChan <- err
	}()

	// Sleep a bit to ensure Request A acquired the lock
	time.Sleep(200 * time.Millisecond)

	// Launch Request B (should fail due to lock conflict)
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
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

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// 2. Configure fake Atelet to FAIL on Restore!
	tc.fakeAtelet.FailRestore = fmt.Errorf("mock atelet failure")

	// 3. Call ResumeActor -> Expect failure
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail due to atelet error")
	}

	// Verify actor state is RESUMING with worker A assigned
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	actor := getResp
	if actor.GetStatus() != ateapipb.Actor_STATUS_RESUMING {
		t.Fatalf("expected status RESUMING, got %v", actor.GetStatus())
	}
	if actor.GetWorkerAssignment().GetWorkerPod() != "worker-a" {
		t.Fatalf("expected worker-a assigned, got %v", actor.GetWorkerAssignment().GetWorkerPod())
	}

	deleteWorkerPod(t, tc, ns, "worker-a")

	// 6. Create Worker Pod B
	createWorkerPod(t, tc, ns, "worker-b", "node1", "pool1")

	// 7. Configure fake Atelet to SUCCEED on Restore
	tc.fakeAtelet.FailRestore = nil
	tc.fakeAtelet.RestoreCalled = false // reset

	// 8. Call ResumeActor again -> Expect it to fail because it is already CRASHED by background syncer.
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected ResumeActor to fail because worker is gone")
	}
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "STATUS_CRASHED") {
		t.Errorf("expected FailedPrecondition/STATUS_CRASHED error, got %v", err)
	}

	// Verify actor state is CRASHED and worker assignment is empty
	actor, err = tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
	if err != nil {
		t.Fatalf("failed to get actor from store: %v", err)
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("expected status CRASHED, got %v", actor.GetStatus())
	}
	if actor.GetWorkerAssignment().GetWorkerPod() != "" {
		t.Errorf("expected worker to be unassigned, got %v", actor.GetWorkerAssignment().GetWorkerPod())
	}
}

func TestSuspendActor_DanglingWorker(t *testing.T) {
	ns := namespaceForTest("ns-sd")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	// 1. Create Worker Pod
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Resume first to make it running
	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	deleteWorkerPod(t, tc, ns, "worker-1")

	// 3. Call SuspendActor -> Expect it to fail because it is already CRASHED by background syncer
	_, err = tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err == nil {
		t.Fatalf("expected SuspendActor to fail because worker is gone")
	}
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "STATUS_CRASHED") {
		t.Errorf("expected FailedPrecondition error, got %v", err)
	}

	// 4. Verify it becomes CRASHED in Redis
	getResp, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if getResp.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("expected status CRASHED, got %v", getResp.GetStatus())
	}
	if getResp.GetWorkerAssignment() != nil {
		t.Errorf("expected worker_assignment to be cleared, got %v", getResp.GetWorkerAssignment())
	}
}

func TestDeleteActor_Success(t *testing.T) {
	ns := namespaceForTest("ns-delete-success")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	deleted, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	// DeleteActor returns the deleted resource.
	if got := deleted.GetMetadata().GetName(); got != "id1" {
		t.Errorf("deleted actor name = %q, want id1", got)
	}
	if got := deleted.GetMetadata().GetAtespace(); got != testAtespace {
		t.Errorf("deleted actor atespace = %q, want %q", got, testAtespace)
	}

	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/id1 not found")
}

func TestDeleteActor_NotSuspended(t *testing.T) {
	ns := namespaceForTest("ns-delete-notsuspended")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}

	_, err = tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "Actor test-atespace/id1 is not in a deletable status (status: STATUS_RUNNING)")
}

func TestDeleteActor_Crashed(t *testing.T) {
	ns := namespaceForTest("ns-delete-crashed")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: "id1"}
	if _, err := tc.persistence.UpdateActor(context.Background(), actorRef, func(dbActor *ateapipb.Actor) error {
		dbActor.Status = ateapipb.Actor_STATUS_CRASHED
		return nil
	}); err != nil {
		t.Fatalf("UpdateActor failed: %v", err)
	}

	deleted, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	if err != nil {
		t.Fatalf("DeleteActor of crashed actor failed: %v", err)
	}
	if got := deleted.GetStatus(); got != ateapipb.Actor_STATUS_DELETING {
		t.Errorf("deleted actor status = %v, want %v", got, ateapipb.Actor_STATUS_DELETING)
	}

	_, err = tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id1"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/id1 not found")
}

func TestDeleteActor_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-delete-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "non-existent"},
	})
	assertGrpcError(t, err, codes.NotFound, "Actor test-atespace/non-existent not found")
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
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "missing-as", Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace missing-as not found")
}

func TestCreateAtespace_Success(t *testing.T) {
	ns := namespaceForTest("ns-create-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	resp, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{
				Name:       "team-a",
				Uid:        "caller-supplied-uid",
				Version:    999,
				CreateTime: timestamppb.New(time.Unix(1, 0)),
				UpdateTime: timestamppb.New(time.Unix(1, 0)),
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	md := resp.GetMetadata()
	if md.GetName() != "team-a" {
		t.Errorf("Name = %q, want team-a", md.GetName())
	}
	if md.GetAtespace() != "" {
		t.Errorf("Atespace = %q, want empty (global-scoped)", md.GetAtespace())
	}
	if md.GetVersion() != 1 {
		t.Errorf("Version = %d, want 1 (caller-set 999 must be ignored)", md.GetVersion())
	}
	if md.GetUid() == "" || md.GetUid() == "caller-supplied-uid" {
		t.Errorf("uid = %q, want a server-generated value", md.GetUid())
	}

	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "team-a",
				Name:     "id1"},
			ActorTemplateNamespace: ns,
			ActorTemplateName:      "tmpl1",
		}}); err != nil {
		t.Errorf("CreateActor into freshly created atespace failed: %v", err)
	}
}

func TestCreateAtespace_AlreadyExists(t *testing.T) {
	ns := namespaceForTest("ns-create-atespace-dup")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}}); err != nil {
		t.Fatalf("first CreateAtespace failed: %v", err)
	}
	_, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}})
	assertGrpcError(t, err, codes.AlreadyExists, "Atespace team-a already exists")
}

func TestGetAtespace_Found(t *testing.T) {
	ns := namespaceForTest("ns-get-atespace")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	created, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	resp, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}})
	if err != nil {
		t.Fatalf("GetAtespace failed: %v", err)
	}
	if diff := cmp.Diff(created, resp, protocmp.Transform()); diff != "" {
		t.Errorf("GetAtespace mismatch (-created +got):\n%s", diff)
	}
}

func TestGetAtespace_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-get-atespace-missing")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "nope"}})
	assertGrpcError(t, err, codes.NotFound, "Atespace nope not found")
}

func TestListAtespaces(t *testing.T) {
	ns := namespaceForTest("ns-list-atespaces")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	for _, n := range []string{"team-a", "team-b"} {
		if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: n}}}); err != nil {
			t.Fatalf("CreateAtespace(%s) failed: %v", n, err)
		}
	}
	resp, err := tc.client.ListAtespaces(context.Background(), &ateapipb.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("ListAtespaces failed: %v", err)
	}
	got := map[string]bool{}
	for _, a := range resp.GetAtespaces() {
		got[a.GetMetadata().GetName()] = true
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

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	deleted, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}})
	if err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}
	// DeleteAtespace returns the deleted resource.
	if got := deleted.GetMetadata().GetName(); got != "team-a" {
		t.Errorf("deleted atespace name = %q, want team-a", got)
	}

	_, err = tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}})
	assertGrpcError(t, err, codes.NotFound, "Atespace team-a not found")
}

func TestDeleteAtespace_NonEmpty_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-nonempty")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	createTemplate(t, tc, ns)

	if _, err := tc.client.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}}}); err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace team-a is not empty")
	// The atespace must survive a rejected delete.
	if _, err := tc.client.GetAtespace(context.Background(), &ateapipb.GetAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}}); err != nil {
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
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-b", Name: "id1"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// Empty team-a deletes fine despite team-b holding an actor.
	if _, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-a"}}); err != nil {
		t.Errorf("DeleteAtespace(team-a, empty) failed: %v", err)
	}
	// team-b is still non-empty → rejected.
	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "team-b"}})
	assertGrpcError(t, err, codes.FailedPrecondition, "Atespace team-b is not empty")
}

func TestDeleteAtespace_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-delete-atespace-missing")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: "nope"}})
	assertGrpcError(t, err, codes.NotFound, "Atespace nope not found")
}

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByValue().Test(t, want, got)
}

// TestSuspendActor_FromPaused suspends a PAUSED actor end-to-end: instead of
// checkpointing a running workload, ateapi asks the atelet on the node
// holding the pause snapshot to upload it, then finalizes the actor with a
// durable ActorSnapshot and no node pinning left behind.
func TestSuspendActor_FromPaused(t *testing.T) {
	ns := namespaceForTest("ns-suspend-paused")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if _, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}
	paused, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	// Drop the pause's Checkpoint call so the suspend's atelet traffic is
	// observable in isolation.
	tc.fakeAtelet.Reset()

	suspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	if !tc.fakeAtelet.UploadCalled {
		t.Fatal("expected atelet UploadPausedCheckpoint to be called")
	}
	if tc.fakeAtelet.CheckpointCalled {
		t.Error("atelet Checkpoint called for a paused actor; there is no workload to checkpoint")
	}
	upload := tc.fakeAtelet.UploadRequest
	if got, want := upload.GetLocalSnapshotName(), paused.GetLocalSnapshotInfo().GetSnapshotName(); got != want {
		t.Errorf("upload local_snapshot_name = %q, want the pause snapshot %q", got, want)
	}
	if got, want := upload.GetAtespace(), testAtespace; got != want {
		t.Errorf("upload atespace = %q, want %q", got, want)
	}
	if got := upload.GetDesiredScope(); got != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		t.Errorf("upload desired_scope = %v, want FULL (template default)", got)
	}

	actor := suspended.GetActor()
	if actor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("status = %v, want SUSPENDED", actor.GetStatus())
	}
	if actor.GetLocalSnapshotInfo() != nil {
		t.Errorf("LocalSnapshotInfo = %v, want cleared (node pinning must not survive suspend)", actor.GetLocalSnapshotInfo())
	}
	ref := actor.GetLatestSnapshot()
	if ref.GetName() == "" {
		t.Fatalf("SuspendActor returned no ActorSnapshot reference: %v", suspended)
	}
	snapshot, err := tc.client.GetActorSnapshot(context.Background(), &ateapipb.GetActorSnapshotRequest{
		Snapshot: &ateapipb.ActorSnapshotRef{Reference: &ateapipb.ActorSnapshotRef_Snapshot{Snapshot: ref}},
	})
	if err != nil {
		t.Fatalf("GetActorSnapshot failed: %v", err)
	}
	if got, want := snapshot.GetSnapshotUri(), upload.GetDestinationSnapshotUri(); got != want {
		t.Errorf("snapshot URI = %q, want the upload destination %q", got, want)
	}
	if got := snapshot.GetContentScope(); got != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		t.Errorf("snapshot ContentScope = %v, want FULL", got)
	}
}

// TestSuspendActor_FromPaused_RetryAfterUploadFailure exercises client-driven
// forward recovery on the paused path: a failed upload leaves the actor
// SUSPENDING with its local snapshot record intact, and a retry completes the
// suspend against the same destination.
func TestSuspendActor_FromPaused_RetryAfterUploadFailure(t *testing.T) {
	ns := namespaceForTest("ns-suspend-paused-retry")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	name := "id1"
	_, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if _, err := tc.client.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("PauseActor failed: %v", err)
	}

	tc.fakeAtelet.Reset()
	tc.fakeAtelet.FailUpload = status.Error(codes.Unavailable, "injected upload failure")
	if _, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err == nil {
		t.Fatal("SuspendActor succeeded despite failing upload")
	}

	stuck, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor failed: %v", err)
	}
	if stuck.GetStatus() != ateapipb.Actor_STATUS_SUSPENDING {
		t.Fatalf("status after failed upload = %v, want SUSPENDING (retryable)", stuck.GetStatus())
	}
	if stuck.GetLocalSnapshotInfo() == nil {
		t.Fatal("LocalSnapshotInfo cleared by a failed upload; the retry could never find the snapshot")
	}
	firstDestination := tc.fakeAtelet.UploadRequest.GetDestinationSnapshotUri()

	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.FailUpload = nil
	tc.fakeAtelet.Lock.Unlock()
	retried, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("SuspendActor retry failed: %v", err)
	}
	if got := retried.GetActor().GetStatus(); got != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("status after retry = %v, want SUSPENDED", got)
	}
	if got := tc.fakeAtelet.UploadRequest.GetDestinationSnapshotUri(); got != firstDestination {
		t.Errorf("retry destination = %q, want the original %q (idempotent upload target)", got, firstDestination)
	}
}
