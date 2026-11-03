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

package v1alpha1

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

const vapManifestPath = "../../../../../manifests/ate-install/authbinding-validation.yaml"

const identityNamespaceMsg = "must name this binding's namespace"

const testNamespace = "team-a"

func validBinding(name string) *AuthBinding {
	return &AuthBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: AuthBindingSpec{
			Source: AuthBindingSource{Identity: "spiffe://cluster.local/ns/team-a/sa/agent-x"},
			Target: AuthBindingTarget{Hostname: "api.github.com"},
			Credentials: []AuthBindingCredential{{
				Header:    "Authorization",
				Prefix:    ptr.To("Bearer "),
				SecretRef: SecretKeyRef{Name: "github-token", Key: "token"},
			}},
		},
	}
}

func credential(header string) AuthBindingCredential {
	return AuthBindingCredential{Header: header, SecretRef: SecretKeyRef{Name: "s", Key: "k"}}
}

func createNamespace(t *testing.T, ctx context.Context) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}
	if err := k8sClient.Create(ctx, ns); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create namespace: %v", err)
	}
}

// applyVAP installs the shipped ValidatingAdmissionPolicy and binding into the
// envtest API server and waits until the API server enforces it, which happens
// asynchronously, by retrying a binding only the policy rejects.
func applyVAP(t *testing.T, ctx context.Context) {
	t.Helper()
	raw, err := os.ReadFile(vapManifestPath)
	if err != nil {
		t.Fatalf("read VAP manifest: %v", err)
	}
	for _, doc := range strings.Split(string(raw), "\n---") {
		obj := map[string]any{}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("decode VAP doc: %v", err)
		}
		if len(obj) == 0 {
			continue
		}
		u := &unstructured.Unstructured{Object: obj}
		if err := k8sClient.Create(ctx, u); err != nil && !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("create %s %q: %v", u.GetKind(), u.GetName(), err)
		}
	}

	i := 0
	err = wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		i++
		b := validBinding(fmt.Sprintf("vap-warmup-%d", i))
		b.Spec.Source.Identity = "spiffe://cluster.local/ns/team-b/sa/agent-x"
		createErr := k8sClient.Create(ctx, b)
		if createErr == nil {
			_ = k8sClient.Delete(ctx, b)
			return false, nil
		}
		return strings.Contains(createErr.Error(), identityNamespaceMsg), nil
	})
	if err != nil {
		t.Fatalf("VAP did not become active: %v", err)
	}
}

func TestAuthBindingValidation(t *testing.T) {
	ctx := t.Context()
	createNamespace(t, ctx)
	applyVAP(t, ctx)

	tests := []struct {
		name   string
		mutate func(b *AuthBinding)
		errMsg string // empty means the binding must be accepted
	}{{
		name:   "design example",
		mutate: func(b *AuthBinding) {},
	}, {
		name: "wildcard hostname and several ports",
		mutate: func(b *AuthBinding) {
			b.Spec.Target.Hostname = "*.example.com"
			b.Spec.Target.Ports = []int32{443, 8443}
		},
	}, {
		name:   "identity in another namespace",
		mutate: func(b *AuthBinding) { b.Spec.Source.Identity = "spiffe://cluster.local/ns/team-b/sa/agent-x" },
		errMsg: identityNamespaceMsg,
	}, {
		name: "substrate actor identity",
		mutate: func(b *AuthBinding) {
			b.Spec.Source.Identity = "spiffe://substrate-actor.local/worker-for-actor/space-1/agent-x"
		},
	}, {
		name: "substrate actor identity with extra segment",
		mutate: func(b *AuthBinding) {
			b.Spec.Source.Identity = "spiffe://substrate-actor.local/worker-for-actor/space-1/agent-x/extra"
		},
		errMsg: "spec.source.identity",
	}, {
		name:   "identity without sa segment",
		mutate: func(b *AuthBinding) { b.Spec.Source.Identity = "spiffe://cluster.local/ns/team-a/agent-x" },
		errMsg: "spec.source.identity",
	}, {
		name:   "identity with the namespace elsewhere",
		mutate: func(b *AuthBinding) { b.Spec.Source.Identity = "spiffe://team-a/ns/team-b/sa/team-a" },
		errMsg: identityNamespaceMsg,
	}, {
		name:   "uppercase hostname",
		mutate: func(b *AuthBinding) { b.Spec.Target.Hostname = "API.github.com" },
		errMsg: "spec.target.hostname",
	}, {
		name:   "hostname with trailing dot",
		mutate: func(b *AuthBinding) { b.Spec.Target.Hostname = "api.github.com." },
		errMsg: "spec.target.hostname",
	}, {
		name:   "IPv4 literal hostname",
		mutate: func(b *AuthBinding) { b.Spec.Target.Hostname = "10.0.0.1" },
		errMsg: "must not be an IP address",
	}, {
		name:   "IPv6 literal hostname",
		mutate: func(b *AuthBinding) { b.Spec.Target.Hostname = "2001:db8::1" },
		errMsg: "spec.target.hostname",
	}, {
		name:   "star alone",
		mutate: func(b *AuthBinding) { b.Spec.Target.Hostname = "*" },
		errMsg: "spec.target.hostname",
	}, {
		name:   "star not leftmost",
		mutate: func(b *AuthBinding) { b.Spec.Target.Hostname = "api.*.com" },
		errMsg: "spec.target.hostname",
	}, {
		name:   "star as part of a label",
		mutate: func(b *AuthBinding) { b.Spec.Target.Hostname = "*api.github.com" },
		errMsg: "spec.target.hostname",
	}, {
		name:   "port zero",
		mutate: func(b *AuthBinding) { b.Spec.Target.Ports = []int32{0} },
		errMsg: "spec.target.ports",
	}, {
		name:   "port too large",
		mutate: func(b *AuthBinding) { b.Spec.Target.Ports = []int32{65536} },
		errMsg: "spec.target.ports",
	}, {
		name:   "duplicate port",
		mutate: func(b *AuthBinding) { b.Spec.Target.Ports = []int32{443, 443} },
		errMsg: "spec.target.ports",
	}, {
		name: "17 ports",
		mutate: func(b *AuthBinding) {
			for i := int32(1); i <= 17; i++ {
				b.Spec.Target.Ports = append(b.Spec.Target.Ports, i)
			}
		},
		errMsg: "spec.target.ports",
	}, {
		name:   "no credentials",
		mutate: func(b *AuthBinding) { b.Spec.Credentials = nil },
		errMsg: "spec.credentials",
	}, {
		name: "17 credentials",
		mutate: func(b *AuthBinding) {
			b.Spec.Credentials = nil
			for i := 0; i < 17; i++ {
				b.Spec.Credentials = append(b.Spec.Credentials, credential(fmt.Sprintf("X-Key-%d", i)))
			}
		},
		errMsg: "spec.credentials",
	}, {
		name: "header names differing only in case",
		mutate: func(b *AuthBinding) {
			b.Spec.Credentials = []AuthBindingCredential{credential("X-Api-Key"), credential("x-api-key")}
		},
		errMsg: "unique, ignoring case",
	}, {
		name:   "pseudo-header",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].Header = ":authority" },
		errMsg: "header",
	}, {
		name:   "x-envoy header",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].Header = "X-Envoy-Force-Trace" },
		errMsg: "must not start with x-envoy",
	}, {
		name:   "host header",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].Header = "Host" },
		errMsg: "must not be host",
	}, {
		name:   "content-length header",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].Header = "Content-Length" },
		errMsg: "must not be host",
	}, {
		name:   "header with a space",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].Header = "X Key" },
		errMsg: "header",
	}, {
		name:   "prefix with a tab",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].Prefix = ptr.To("Bearer\t") },
		errMsg: "prefix",
	}, {
		name:   "prefix with non-ASCII",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].Prefix = ptr.To("Bearer\u00a0") },
		errMsg: "prefix",
	}, {
		name:   "missing secretRef key",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].SecretRef.Key = "" },
		errMsg: "key",
	}, {
		name:   "secretRef name with uppercase",
		mutate: func(b *AuthBinding) { b.Spec.Credentials[0].SecretRef.Name = "GitHub" },
		errMsg: "name",
	}}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := validBinding(fmt.Sprintf("case-%d", i))
			tc.mutate(b)
			err := k8sClient.Create(ctx, b)
			if tc.errMsg == "" {
				if err != nil {
					t.Fatalf("create: %v", err)
				}
				t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), b) })
				return
			}
			if err == nil {
				_ = k8sClient.Delete(ctx, b)
				t.Fatalf("binding was accepted, want an error containing %q", tc.errMsg)
			}
			if !strings.Contains(err.Error(), tc.errMsg) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.errMsg)
			}
		})
	}
}

func TestAuthBindingPortsDefault(t *testing.T) {
	ctx := t.Context()
	createNamespace(t, ctx)

	b := validBinding("ports-default")
	if err := k8sClient.Create(ctx, b); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), b) })

	if got := b.Spec.Target.Ports; len(got) != 1 || got[0] != 443 {
		t.Errorf("ports = %v, want [443]", got)
	}
}
