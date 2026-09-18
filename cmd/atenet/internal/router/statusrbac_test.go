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

package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/agent-substrate/substrate/internal/testenv"
)

// A fake client does not enforce RBAC. Use the shipped account and permissions
// against a real API server, including the Service lookup made by /statusz.
func TestStatuszServicePermissions(t *testing.T) {
	if testing.Short() {
		t.Skip("requires the envtest API server")
	}
	cfg, stop := testenv.Start()
	t.Cleanup(stop)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	admin, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	deployment, service := installRouterStatusResources(t, ctx, admin)
	adminKube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	token, err := adminKube.CoreV1().ServiceAccounts(deployment.Namespace).CreateToken(ctx,
		deployment.Spec.Template.Spec.ServiceAccountName, &authenticationv1.TokenRequest{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Drop the admin client certificate so only the router's token authenticates.
	routerCfg := rest.AnonymousClientConfig(cfg)
	routerCfg.BearerToken = token.Status.Token
	routerKube, err := kubernetes.NewForConfig(routerCfg)
	if err != nil {
		t.Fatal(err)
	}
	router := &RouterServer{cfg: routerConfig{Namespace: deployment.Namespace}, clientset: routerKube}

	// Role/RoleBinding updates reach the authorizer asynchronously.
	if err := wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 5*time.Second, true,
		func(ctx context.Context) (bool, error) {
			return router.getRouterIP(ctx) == service.Spec.ClusterIP, nil
		}); err != nil {
		t.Fatalf("Router Service IP = %q, want %q: %v", router.getRouterIP(ctx), service.Spec.ClusterIP, err)
	}

	server := httptest.NewServer(http.HandlerFunc(router.handleStatusz))
	t.Cleanup(server.Close)
	for _, format := range []string{"html", "json"} {
		t.Run(format, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/statusz?format="+format, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if format == "json" {
				var dashboard DashboardContext
				if err := json.NewDecoder(resp.Body).Decode(&dashboard); err != nil {
					t.Fatal(err)
				}
				if dashboard.RouterClusterIP != service.Spec.ClusterIP {
					t.Errorf("router_cluster_ip = %q, want %q", dashboard.RouterClusterIP, service.Spec.ClusterIP)
				}
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), service.Spec.ClusterIP) || strings.Contains(string(body), "Lookup Failed:") {
				t.Errorf("HTML does not display Router Service IP %q", service.Spec.ClusterIP)
			}
		})
	}

	for _, tc := range []struct {
		name, namespace, group, resource, resourceName, verb string
		allowed                                              bool
	}{
		{"router Service", deployment.Namespace, "", "services", service.Name, "get", true},
		{"another Service", deployment.Namespace, "", "services", "another-service", "get", false},
		{"another namespace", "default", "", "services", service.Name, "get", false},
		{"list Services", deployment.Namespace, "", "services", "", "list", false},
		{"watch Services", deployment.Namespace, "", "services", "", "watch", false},
		{"create Service", deployment.Namespace, "", "services", "", "create", false},
		{"update Service", deployment.Namespace, "", "services", service.Name, "update", false},
		{"patch Service", deployment.Namespace, "", "services", service.Name, "patch", false},
		{"delete Service", deployment.Namespace, "", "services", service.Name, "delete", false},
		{"get EndpointSlice", deployment.Namespace, "discovery.k8s.io", "endpointslices", "router-endpoints", "get", true},
		{"list EndpointSlices", deployment.Namespace, "discovery.k8s.io", "endpointslices", "", "list", true},
		{"watch EndpointSlices", deployment.Namespace, "discovery.k8s.io", "endpointslices", "", "watch", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			review, err := routerKube.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx,
				&authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
					ResourceAttributes: &authorizationv1.ResourceAttributes{
						Namespace: tc.namespace, Group: tc.group, Resource: tc.resource, Name: tc.resourceName, Verb: tc.verb,
					},
				}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if review.Status.Allowed != tc.allowed {
				t.Errorf("allowed = %t, want %t: %s", review.Status.Allowed, tc.allowed, review.Status.Reason)
			}
		})
	}
}

// Install the actual Service and its RBAC; the Deployment supplies the account
// identity. Pods and the dataplane are not needed for this API/status boundary.
func installRouterStatusResources(t *testing.T, ctx context.Context, admin client.Client) (*appsv1.Deployment, *corev1.Service) {
	t.Helper()
	manifest, err := os.Open("../../../../manifests/ate-install/atenet-router.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer manifest.Close()
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ate-system"}}); err != nil {
		t.Fatal(err)
	}
	var deployment *appsv1.Deployment
	var service *corev1.Service
	decoder := k8syaml.NewYAMLOrJSONDecoder(manifest, 4096)
	for {
		var raw runtime.RawExtension
		if err := decoder.Decode(&raw); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		obj, gvk, err := scheme.Codecs.UniversalDeserializer().Decode(raw.Raw, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		switch gvk.Kind {
		case "ServiceAccount", "Role", "RoleBinding", "Service":
			if err := admin.Create(ctx, obj.(client.Object)); err != nil {
				t.Fatalf("installing %s: %v", gvk.Kind, err)
			}
		}
		switch obj := obj.(type) {
		case *appsv1.Deployment:
			deployment = obj
		case *corev1.Service:
			service = obj
		}
	}
	if deployment == nil || service == nil || service.Spec.ClusterIP == "" || service.Spec.ClusterIP == "None" {
		t.Fatal("router manifest must contain a Deployment and an allocated Service")
	}
	return deployment, service
}
