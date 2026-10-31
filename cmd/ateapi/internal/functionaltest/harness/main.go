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

// Package harness runs ate-api against fakes of everything it talks to, for
// functional tests that drive it only through its public API.
//
// Each test gets its own ate-api, database, k8s namespace, nodes, atelets,
// object store and volume plugin, so tests run in parallel. A test package
// using the harness calls Main from its TestMain.
package harness

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/testenv"
)

// restConfig reaches the envtest cluster Main starts. It is shared by every
// test in the package; what each test creates in it is scoped to the test.
var restConfig *rest.Config

// Main starts the envtest cluster shared by the package's tests, runs them,
// and tears the cluster and the PostgreSQL container down.
func Main(m *testing.M) {
	// Process-wide, like the server flag. Every gate is on, and a test uses a
	// preview feature only through a harness WithPreviewClient, whose clients
	// opt in.
	//
	// TODO: Start ate-api with no gates, as ate-api-server does, and let each
	// test enable the ones it needs when it creates the harness, such as
	// New(t, WithPreviewServer(preview.GateExternalVolumes)). That needs
	// ate-api to take its gates per instance instead of from preview's
	// process-wide state.
	if err := preview.Init(preview.All); err != nil {
		log.Fatalf("enabling preview gates: %v", err)
	}
	cfg, stopEnv := testenv.Start()
	restConfig = cfg
	if err := setupCluster(cfg); err != nil {
		stopEnv()
		log.Fatalf("setting up the harness cluster: %v", err)
	}
	code := m.Run()
	stopEnv()
	storetest.Shutdown()
	os.Exit(code)
}

// setupCluster creates the cluster-scoped objects every test reads and none
// changes.
func setupCluster(cfg *rest.Config) error {
	ctx := context.Background()
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	if _, err := kc.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: installdefaults.SystemNamespace},
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating namespace %s: %w", installdefaults.SystemNamespace, err)
	}

	if _, err := kc.StorageV1().StorageClasses().Create(ctx, &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: StorageClass},
		Provisioner: VolumeDriver,
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating StorageClass %s: %w", StorageClass, err)
	}

	return nil
}
