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
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/testenv"
	"google.golang.org/grpc"
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
