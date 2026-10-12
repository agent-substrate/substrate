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
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/testenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	cfg        *rest.Config
	fakeAtelet = &FakeAteletServer{}
)

func TestMain(m *testing.M) {
	// The in-process server logs RPC bodies through ateinterceptors, which
	// relies on this handler to mask debug_redact fields.
	serverboot.InitLoggerWithWriter(os.Stderr)
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

	// Create shared Atelet Pod. Tests that place workers on another node add
	// their own atelet there via setupAteletOnNode.
	if err := createAteletPod(k8sClient, "atelet-shared", "node1"); err != nil {
		log.Fatalf("%v", err)
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

	storetest.Shutdown()

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
	FailCheckpoint    error

	RestoreCalled  bool
	RestoreRequest *ateletpb.RestoreRequest
	FailRestore    error
	RestoreDelay   time.Duration

	PromoteCalled  bool
	PromoteRequest *ateletpb.PromoteSnapshotRequest
	// PromoteRequests holds every PromoteSnapshot request, in order.
	PromoteRequests []*ateletpb.PromoteSnapshotRequest
	FailPromote     error
	// FailPromoteAfterCommit is returned once the upload has committed, as
	// when the response to a successful promote is lost.
	FailPromoteAfterCommit error
	// promoted holds the fidelity committed at each promote destination.
	promoted map[string]ateletpb.SnapshotFidelity

	TerminateCalled  bool
	TerminateRequest *ateletpb.TerminateRequest
	FailTerminate    error

	// objectStore, when set, receives the objects a checkpoint or an upload
	// writes, so the control plane's copy and release steps have real external
	// snapshots to act on. setupTest points it at the test's own store.
	objectStore *objectstoretest.Fake
}

// snapshotObjects names the objects a checkpoint leaves behind. The names are
// arbitrary: nothing in the control plane reads them, it only copies and
// deletes whatever shares the snapshot's prefix.
var snapshotObjects = []string{"manifest.json", "memory.zst"}

// SetObjectStore points the fake at the store a checkpoint should write to.
func (f *FakeAteletServer) SetObjectStore(store *objectstoretest.Fake) {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.objectStore = store
}

// writeSnapshot records the external snapshot a checkpoint or an upload would
// have written at snapshotURI. Called with Lock held.
func (f *FakeAteletServer) writeSnapshot(snapshotURI string) error {
	if f.objectStore == nil || snapshotURI == "" {
		return nil
	}
	return f.objectStore.WriteSnapshot(snapshotURI, snapshotObjects...)
}

func (f *FakeAteletServer) Reset() {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.RunCalled = false
	f.RunRequest = nil
	f.FailRun = nil

	f.CheckpointCalled = false
	f.CheckpointRequest = nil
	f.FailCheckpoint = nil

	f.RestoreCalled = false
	f.RestoreRequest = nil
	f.FailRestore = nil
	f.RestoreDelay = 0

	f.PromoteCalled = false
	f.PromoteRequest = nil
	f.PromoteRequests = nil
	f.FailPromote = nil
	f.FailPromoteAfterCommit = nil
	f.promoted = nil

	f.TerminateCalled = false
	f.TerminateRequest = nil
	f.FailTerminate = nil

	f.objectStore = nil
}

func (f *FakeAteletServer) PromoteSnapshot(ctx context.Context, req *ateletpb.PromoteSnapshotRequest) (*ateletpb.PromoteSnapshotResponse, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.PromoteCalled = true
	f.PromoteRequest = proto.Clone(req).(*ateletpb.PromoteSnapshotRequest)
	f.PromoteRequests = append(f.PromoteRequests, f.PromoteRequest)
	if f.FailPromote != nil {
		return nil, f.FailPromote
	}
	// Like atelet, leave a destination already holding the snapshot at the
	// requested fidelity alone, and refuse one holding another fidelity.
	uri := req.GetSnapshot().GetObject().GetSnapshotUri()
	if committed, ok := f.promoted[uri]; ok {
		if committed != req.GetSnapshot().GetFidelity() {
			return nil, status.Errorf(codes.FailedPrecondition, "%s already holds the snapshot as %v", uri, committed)
		}
		return &ateletpb.PromoteSnapshotResponse{}, nil
	}
	if err := f.writeSnapshot(uri); err != nil {
		return nil, err
	}
	if f.promoted == nil {
		f.promoted = map[string]ateletpb.SnapshotFidelity{}
	}
	f.promoted[uri] = req.GetSnapshot().GetFidelity()
	if f.FailPromoteAfterCommit != nil {
		return nil, f.FailPromoteAfterCommit
	}
	return &ateletpb.PromoteSnapshotResponse{}, nil
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
	if f.FailCheckpoint != nil {
		return nil, f.FailCheckpoint
	}

	if err := f.writeSnapshot(req.GetSnapshot().GetObject().GetSnapshotUri()); err != nil {
		return nil, err
	}
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

func (f *FakeAteletServer) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	f.TerminateCalled = true
	f.TerminateRequest = proto.Clone(req).(*ateletpb.TerminateRequest)
	if f.FailTerminate != nil {
		return nil, f.FailTerminate
	}
	return &ateletpb.TerminateResponse{}, nil
}

func (f *FakeAteletServer) lastRestoreRequest() *ateletpb.RestoreRequest {
	f.Lock.Lock()
	defer f.Lock.Unlock()

	if f.RestoreRequest == nil {
		return nil
	}
	return proto.Clone(f.RestoreRequest).(*ateletpb.RestoreRequest)
}
