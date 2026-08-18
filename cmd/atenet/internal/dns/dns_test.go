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
	reloads  int
}

func (m *mockConfigReloader) Reload(ctx context.Context) error {
	m.reloaded = true
	m.reloads++
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

// TestReconcileRouterIPFamilies covers what the controller publishes for each
// shape the atenet-router Service takes. The v6-only row is the one that used
// to be broken: the sole ClusterIP was read out of Spec.ClusterIP and written
// into an `IN A` answer, which loads as a valid Corefile and then SERVFAILs
// every query in the zone.
func TestReconcileRouterIPFamilies(t *testing.T) {
	tests := []struct {
		name string
		// routerSpec is the atenet-router Service's spec; the dns Service is
		// always a plain single-stack v4 one, since it feeds kube-dns rather
		// than the zone under test.
		routerSpec corev1.ServiceSpec
		// wantAnswers are the answer lines the rendered Corefile must have.
		wantAnswers []string
		// wantNoAnswer, when true, means reconcile should leave the Corefile
		// untouched rather than publish anything.
		wantNoAnswer bool
	}{
		{
			name:        "single stack IPv4",
			routerSpec:  corev1.ServiceSpec{ClusterIP: "10.0.0.1", ClusterIPs: []string{"10.0.0.1"}},
			wantAnswers: []string{`answer "{{ .Name }} 60 IN A 10.0.0.1"`},
		},
		{
			name:        "single stack IPv6",
			routerSpec:  corev1.ServiceSpec{ClusterIP: "fd00:10:96::8857", ClusterIPs: []string{"fd00:10:96::8857"}},
			wantAnswers: []string{`answer "{{ .Name }} 60 IN AAAA fd00:10:96::8857"`},
		},
		{
			name:       "dual stack",
			routerSpec: corev1.ServiceSpec{ClusterIP: "10.0.0.1", ClusterIPs: []string{"10.0.0.1", "fd00:10:96::8857"}},
			wantAnswers: []string{
				`answer "{{ .Name }} 60 IN A 10.0.0.1"`,
				`answer "{{ .Name }} 60 IN AAAA fd00:10:96::8857"`,
			},
		},
		{
			name:         "not yet allocated",
			routerSpec:   corev1.ServiceSpec{},
			wantNoAnswer: true,
		},
		{
			name:         "headless",
			routerSpec:   corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, ClusterIPs: []string{corev1.ClusterIPNone}},
			wantNoAnswer: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)

			routerSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "atenet-router", Namespace: "ate-system"},
				Spec:       tc.routerSpec,
			}
			dnsSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "dns", Namespace: "ate-system"},
				Spec:       corev1.ServiceSpec{ClusterIP: "10.0.0.2"},
			}

			const placeholder = "# not written yet\n"
			corefilePath := filepath.Join(t.TempDir(), "Corefile")
			if err := os.WriteFile(corefilePath, []byte(placeholder), 0644); err != nil {
				t.Fatalf("failed to write initial Corefile: %v", err)
			}

			reloader := &mockConfigReloader{}
			controller := &Controller{
				Client:       fake.NewClientBuilder().WithScheme(scheme).WithObjects(routerSvc, dnsSvc).Build(),
				Interval:     1 * time.Second,
				CorefilePath: corefilePath,
				Reloader:     reloader,
			}

			ctx := context.Background()
			if err := controller.reconcile(ctx); err != nil {
				t.Fatalf("reconcile failed: %v", err)
			}

			corefileBytes, err := os.ReadFile(corefilePath)
			if err != nil {
				t.Fatalf("failed to read Corefile: %v", err)
			}
			got := string(corefileBytes)

			if tc.wantNoAnswer {
				if got != placeholder {
					t.Errorf("reconcile() rewrote the Corefile for a Service with no usable ClusterIP; want it left alone\nGot:\n%s", got)
				}
				if reloader.reloads != 0 {
					t.Errorf("reconcile() reloaded CoreDNS %d times for a Service with no usable ClusterIP, want 0", reloader.reloads)
				}
				return
			}

			for _, want := range tc.wantAnswers {
				if !strings.Contains(got, want) {
					t.Errorf("reconcile() wrote a Corefile missing %q\nGot:\n%s", want, got)
				}
			}
			// Exactly the expected answers and no others: an address template for
			// a family the Service does not have would publish an unreachable
			// address, and on the A side would not even parse as an RR.
			if answers := strings.Count(got, `answer "`); answers != len(tc.wantAnswers) {
				t.Errorf("reconcile() wrote %d answer directives, want %d\nGot:\n%s", answers, len(tc.wantAnswers), got)
			}
			if reloader.reloads != 1 {
				t.Errorf("reconcile() reloaded CoreDNS %d times, want 1", reloader.reloads)
			}

			// A second pass must be a no-op. The controller reconciles on a
			// ticker, so anything unstable in the render -- a timestamp, most
			// easily -- would rewrite the file and signal CoreDNS every interval.
			if err := controller.reconcile(ctx); err != nil {
				t.Fatalf("second reconcile failed: %v", err)
			}
			if reloader.reloads != 1 {
				t.Errorf("second reconcile() reloaded CoreDNS again (%d total), want it to recognise the Corefile as up to date", reloader.reloads)
			}
		})
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
