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
			name: "fqdn format api.ate-system.svc:443",
			target: resolver.Target{
				URL: url.URL{Scheme: "k8s", Path: "/api.ate-system.svc:443"},
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

	select {
	case state := <-cc.stateChan:
		wantAddrs := []resolver.Address{{Addr: "10.0.0.1:443"}}
		if !reflect.DeepEqual(state.Addresses, wantAddrs) {
			t.Errorf("initial state.Addresses = %v, want %v", state.Addresses, wantAddrs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial state update")
	}

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

	select {
	case state := <-cc.stateChan:
		wantAddrs := []resolver.Address{
			{Addr: "10.0.0.1:443"},
			{Addr: "10.0.0.2:443"},
		}
		if !reflect.DeepEqual(state.Addresses, wantAddrs) {
			t.Errorf("updated state.Addresses = %v, want %v", state.Addresses, wantAddrs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for state update after slice addition")
	}
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
