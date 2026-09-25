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

package k8sresolver

import (
	"context"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type testClientConn struct {
	resolver.ClientConn
	stateChan chan resolver.State
	errChan   chan error
}

func newTestClientConn() *testClientConn {
	return &testClientConn{
		stateChan: make(chan resolver.State, 10),
		errChan:   make(chan error, 10),
	}
}

func (c *testClientConn) UpdateState(state resolver.State) error {
	c.stateChan <- state
	return nil
}

func (c *testClientConn) ReportError(err error) {
	c.errChan <- err
}

func (c *testClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	return nil
}

// waitForAddrs consumes updates until the set matches want: the resolver promises
// eventual convergence, not that the first update after a change is final.
func waitForAddrs(t *testing.T, cc *testClientConn, want []resolver.Address) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	var last []resolver.Address
	for {
		select {
		case state := <-cc.stateChan:
			last = state.Addresses
			if reflect.DeepEqual(last, want) {
				return
			}
		case <-deadline:
			t.Fatalf("state.Addresses never converged: last = %v, want %v", last, want)
		}
	}
}

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name          string
		target        resolver.Target
		wantNamespace string
		wantService   string
		wantPort      string
		wantErr       bool
	}{
		{
			name: "slash format namespace/service:port",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:443"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "443",
		},
		{
			name: "host format host=ate-system path=/api:443",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Host: "ate-system", Path: "/api:443"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "443",
		},
		{
			name: "explicit path namespace preserves dotted service",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/ate-system/api.prod"},
			},
			wantNamespace: "ate-system",
			wantService:   "api.prod",
			wantPort:      "443",
		},
		{
			name: "explicit host namespace preserves dotted service",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Host: "ate-system", Path: "/api.prod:8443"},
			},
			wantNamespace: "ate-system",
			wantService:   "api.prod",
			wantPort:      "8443",
		},
		{
			name: "fqdn format api.ate-system.svc:443",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/api.ate-system.svc:443"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "443",
		},
		{
			name: "dns style infers namespace without explicit namespace or port",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/api.ate-system"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "443",
		},
		{
			name: "full cluster fqdn format api.ate-system.svc.cluster.local:443",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/api.ate-system.svc.cluster.local:443"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "443",
		},
		{
			name: "simple service without port",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/api"},
			},
			wantNamespace: "default",
			wantService:   "api",
			wantPort:      "443",
		},
		{
			name: "localhost host uses path namespace",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Host: "localhost", Path: "/ate-system/api"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "443",
		},
		{
			name: "EndpointSlice port sentinel",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:0"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "0",
		},
		{
			name: "minimum explicit port",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:1"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "1",
		},
		{
			name: "maximum explicit port",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:65535"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "65535",
		},
		{
			name: "explicit port spelling is preserved",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:00443"},
			},
			wantNamespace: "ate-system",
			wantService:   "api",
			wantPort:      "00443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNs, gotSvc, gotPort, err := ParseTarget(tt.target)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseTarget() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotNs != tt.wantNamespace {
				t.Errorf("ParseTarget() gotNs = %v, want %v", gotNs, tt.wantNamespace)
			}
			if gotSvc != tt.wantService {
				t.Errorf("ParseTarget() gotSvc = %v, want %v", gotSvc, tt.wantService)
			}
			if gotPort != tt.wantPort {
				t.Errorf("ParseTarget() gotPort = %v, want %v", gotPort, tt.wantPort)
			}
		})
	}
}

func TestParseTargetRejectsInvalidSchemes(t *testing.T) {
	tests := []struct {
		name   string
		scheme string
	}{
		{name: "missing", scheme: ""},
		{name: "wrong", scheme: "dns"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := resolver.Target{
				URL: url.URL{Scheme: tt.scheme, Path: "/ate-system/api:443"},
			}

			_, _, _, err := ParseTarget(target)
			if err == nil {
				t.Fatal("ParseTarget() error = nil, want an invalid scheme error")
			}
			if !strings.Contains(err.Error(), "invalid scheme") {
				t.Fatalf("ParseTarget() error = %q, want an invalid scheme error", err)
			}
		})
	}
}

func TestParseTargetRejectsEmptyNamespaceOrService(t *testing.T) {
	tests := []struct {
		name   string
		target resolver.Target
	}{
		{
			name:   "empty path namespace",
			target: resolver.Target{URL: url.URL{Scheme: "k8s", Path: "//api:443"}},
		},
		{
			name:   "empty path service",
			target: resolver.Target{URL: url.URL{Scheme: "k8s", Path: "/ate-system/"}},
		},
		{
			name:   "empty host service",
			target: resolver.Target{URL: url.URL{Scheme: "k8s", Host: "ate-system"}},
		},
		{
			name:   "empty inferred namespace",
			target: resolver.Target{URL: url.URL{Scheme: "k8s", Path: "/api."}},
		},
		{
			name:   "empty inferred service",
			target: resolver.Target{URL: url.URL{Scheme: "k8s", Path: "/.ate-system"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := ParseTarget(tt.target)
			if err == nil {
				t.Fatal("ParseTarget() error = nil, want an empty namespace or service error")
			}
			if !strings.Contains(err.Error(), "namespace and service to be non-empty") {
				t.Fatalf("ParseTarget() error = %q, want a non-empty namespace and service error", err)
			}
		})
	}
}

func TestParseTargetRejectsInvalidPorts(t *testing.T) {
	tests := []struct {
		name string
		port string
	}{
		{name: "empty", port: ""},
		{name: "nonnumeric", port: "notaport"},
		{name: "negative", port: "-1"},
		{name: "above 65535", port: "65536"},
		{name: "multiple colons", port: "443:extra"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:" + tt.port},
			}

			_, _, _, err := ParseTarget(target)
			if err == nil {
				t.Fatal("ParseTarget() error = nil, want an invalid port error")
			}
			if !strings.Contains(err.Error(), "invalid port") {
				t.Fatalf("ParseTarget() error = %q, want an invalid port error", err)
			}
		})
	}
}

func TestK8sResolverEndpointSliceUpdates(t *testing.T) {
	readyTrue := true
	port443 := int32(443)

	slice1 := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-slice-1",
			Namespace: "ate-system",
			Labels: map[string]string{
				"kubernetes.io/service-name": "api",
			},
		},
		Ports: []discoveryv1.EndpointPort{
			{Port: &port443},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: &readyTrue,
				},
			},
		},
	}

	fakeClient := fake.NewSimpleClientset(slice1)
	builder := NewBuilder(fakeClient)
	cc := newTestClientConn()

	target := resolver.Target{
		URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:443"},
	}

	res, err := builder.Build(target, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("builder.Build() error = %v", err)
	}
	defer res.Close()

	waitForAddrs(t, cc, []resolver.Address{{Addr: "10.0.0.1:443"}})

	// Add a new EndpointSlice
	slice2 := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-slice-2",
			Namespace: "ate-system",
			Labels: map[string]string{
				"kubernetes.io/service-name": "api",
			},
		},
		Ports: []discoveryv1.EndpointPort{
			{Port: &port443},
		},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.2"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: &readyTrue,
				},
			},
		},
	}

	_, err = fakeClient.DiscoveryV1().EndpointSlices("ate-system").Create(context.Background(), slice2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create slice2: %v", err)
	}

	waitForAddrs(t, cc, []resolver.Address{
		{Addr: "10.0.0.1:443"},
		{Addr: "10.0.0.2:443"},
	})
}

func TestK8sResolverClose(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	builder := NewBuilder(fakeClient)
	cc := newTestClientConn()

	target := resolver.Target{
		URL: url.URL{Scheme: "k8s", Path: "/ate-system/api:443"},
	}

	res, err := builder.Build(target, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("builder.Build() error = %v", err)
	}

	res.Close()

	// Drain any initial state updates that happened before Close
	for len(cc.stateChan) > 0 {
		<-cc.stateChan
	}

	// Trigger a resolution after Close
	res.ResolveNow(resolver.ResolveNowOptions{})

	select {
	case state := <-cc.stateChan:
		t.Fatalf("unexpected state update after Close(): %v", state)
	case err := <-cc.errChan:
		t.Fatalf("unexpected error report after Close(): %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: no calls to ClientConn after Close
	}
}
