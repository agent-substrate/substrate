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

package dns

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type mockConfigReloader struct {
	reloaded bool
}

func (m *mockConfigReloader) Reload(ctx context.Context) error {
	m.reloaded = true
	return nil
}

func TestReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// 1. Create mock services
	routerSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "atenet-router",
			Namespace: "ate-system",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.1",
		},
	}

	dnsSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dns",
			Namespace: "ate-system",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.2",
		},
	}

	initialCorefile := `
.:53 {
    errors
}
`

	// 2. Set up a temporary local Corefile on disk
	tempDir := t.TempDir()
	corefilePath := filepath.Join(tempDir, "Corefile")
	if err := os.WriteFile(corefilePath, []byte(initialCorefile), 0644); err != nil {
		t.Fatalf("failed to write initial Corefile: %v", err)
	}

	kubeDNSCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-dns",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"stubDomains": `{"other-domain.com":["8.8.8.8"]}`,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(routerSvc, dnsSvc, kubeDNSCM).
		Build()

	reloader := &mockConfigReloader{}
	controller := &Controller{
		Client:       client,
		Interval:     1 * time.Second,
		CorefilePath: corefilePath,
		Reloader:     reloader,
	}

	// Run one reconciliation loop
	ctx := context.Background()
	err := controller.reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if !reloader.reloaded {
		t.Errorf("expected ConfigReloader to be invoked, but it was not")
	}

	// Verify the Corefile on disk has been updated with the router IP
	updatedCorefileBytes, err := os.ReadFile(corefilePath)
	if err != nil {
		t.Fatalf("failed to read updated Corefile from disk: %v", err)
	}
	updatedCorefile := string(updatedCorefileBytes)
	if !strings.Contains(updatedCorefile, `answer "{{ .Name }} 60 IN A 10.0.0.1"`) {
		t.Errorf("expected Corefile on disk to contain updated answer line, but got: %s", updatedCorefile)
	}

	// Verify kube-system:kube-dns ConfigMap contains the new stub domain without wiping out other-domain
	updatedKubeDNSCM := &corev1.ConfigMap{}
	err = client.Get(ctx, types.NamespacedName{Name: "kube-dns", Namespace: "kube-system"}, updatedKubeDNSCM)
	if err != nil {
		t.Fatalf("failed to get updated kube-dns ConfigMap: %v", err)
	}

	stubDomainsStr := updatedKubeDNSCM.Data["stubDomains"]
	var stubDomains map[string][]string
	if err := json.Unmarshal([]byte(stubDomainsStr), &stubDomains); err != nil {
		t.Fatalf("failed to unmarshal updated stubDomains: %v", err)
	}

	ips, exists := stubDomains["actors.resources.substrate.ate.dev"]
	if !exists || len(ips) != 1 || ips[0] != "10.0.0.2" {
		t.Errorf("expected stubDomains to map actors.resources.substrate.ate.dev to [10.0.0.2], but got: %v", stubDomains)
	}

	otherIPs, exists := stubDomains["other-domain.com"]
	if !exists || len(otherIPs) != 1 || otherIPs[0] != "8.8.8.8" {
		t.Errorf("expected stubDomains to preserve other-domain.com mapping, but got: %v", stubDomains)
	}
}

func TestReconcileKubeDNSNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	routerSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "atenet-router",
			Namespace: "ate-system",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.1",
		},
	}

	dnsSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dns",
			Namespace: "ate-system",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.0.0.2",
		},
	}

	// Set up local Corefile on disk
	tempDir := t.TempDir()
	corefilePath := filepath.Join(tempDir, "Corefile")
	initialCorefile := `answer "{{ .Name }} 60 IN A <router service address>"`
	if err := os.WriteFile(corefilePath, []byte(initialCorefile), 0644); err != nil {
		t.Fatalf("failed to write initial Corefile: %v", err)
	}

	// kube-dns ConfigMap is omitted to test gracefulness

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(routerSvc, dnsSvc).
		Build()

	controller := &Controller{
		Client:       client,
		Interval:     1 * time.Second,
		CorefilePath: corefilePath,
		Reloader:     &mockConfigReloader{},
	}

	ctx := context.Background()
	err := controller.reconcile(ctx)
	if err != nil {
		t.Fatalf("reconcile should handle missing kube-dns configmap gracefully but failed with: %v", err)
	}
}
