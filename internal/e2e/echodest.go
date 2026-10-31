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

package e2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// DeployEchoDest installs the echodest backend, suffixed for the calling suite
// so concurrent suites never share one, and returns its namespace.
func DeployEchoDest(t *testing.T, name string) string {
	t.Helper()

	root, err := FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	manifest := RenderFixtureManifest(t, "internal/e2e/fixtures/echodest/echodest.yaml.tmpl", "", name)

	applyArgs := []string{"ko", "apply", "-f", manifest}
	if KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+KubeContext)
	}
	RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	t.Cleanup(func() {
		delArgs := []string{"delete", "--ignore-not-found", "-f", manifest}
		if KubeContext != "" {
			delArgs = append([]string{"--context=" + KubeContext}, delArgs...)
		}
		RunCmd(t, "kubectl", delArgs...)
	})

	return FixtureName("ate-e2e-echodest") + "-" + name
}

// ClusterIPFamilies reports which address families the cluster can assign,
// read from a node's podCIDRs. A Service pinned to a family the cluster does
// not have is rejected at creation, so callers gate on this rather than
// letting the apply fail.
func ClusterIPFamilies(t *testing.T, ctx context.Context) map[corev1.IPFamily]bool {
	t.Helper()

	nodes, err := GetClients().K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("listing nodes to determine the cluster's address families: %v", err)
	}
	if len(nodes.Items) == 0 {
		t.Fatal("the cluster reports no nodes, so its address families cannot be determined")
	}

	families := map[corev1.IPFamily]bool{}
	for _, cidr := range nodes.Items[0].Spec.PodCIDRs {
		if strings.Contains(cidr, ":") {
			families[corev1.IPv6Protocol] = true
		} else {
			families[corev1.IPv4Protocol] = true
		}
	}
	if len(families) == 0 {
		t.Fatalf("node %q has no podCIDRs, so the cluster's address families cannot be determined", nodes.Items[0].Name)
	}
	return families
}

// CreateEchoDestService fronts the echodest backend with a Service pinned to
// families, so the name resolves to exactly the records the caller asked for.
// Returns the Service name.
func CreateEchoDestService(t *testing.T, ctx context.Context, namespace, name string, families []corev1.IPFamily) string {
	t.Helper()

	// SingleStack for one family, RequireDualStack for two: Prefer would
	// silently hand back a single-stack Service on a cluster missing a family,
	// and a test that then passed would be asserting nothing.
	policy := corev1.IPFamilyPolicySingleStack
	if len(families) > 1 {
		policy = corev1.IPFamilyPolicyRequireDualStack
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector:       map[string]string{"app": "echodest"},
			IPFamilies:     families,
			IPFamilyPolicy: &policy,
			Ports:          []corev1.ServicePort{{Port: 8080, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	created, err := GetClients().K8s.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating the %s echodest Service: %v", name, err)
	}
	t.Cleanup(func() {
		//nolint:errcheck // best-effort teardown; the namespace goes too
		GetClients().K8s.CoreV1().Services(namespace).Delete(context.Background(), name, metav1.DeleteOptions{})
	})
	t.Logf("echodest Service %s has clusterIPs %v", name, created.Spec.ClusterIPs)
	return name
}
