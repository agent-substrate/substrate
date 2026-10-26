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
	"testing"

	"sigs.k8s.io/yaml"
)

// socketOption is an Envoy SocketOption as the manifest writes it.
type socketOption struct {
	Level    int64  `json:"level"`
	Name     int64  `json:"name"`
	IntValue int64  `json:"int_value"`
	State    string `json:"state"`
}

// notSentLowat is the TCP_NOTSENT_LOWAT value in opts, or 0, checking that it
// is set before the socket binds.
func notSentLowat(t *testing.T, where string, opts []socketOption) int64 {
	t.Helper()
	for _, o := range opts {
		// IPPROTO_TCP and TCP_NOTSENT_LOWAT.
		if o.Level == 6 && o.Name == 25 {
			if o.State != "STATE_PREBIND" {
				t.Errorf("%s sets TCP_NOTSENT_LOWAT in state %q, want STATE_PREBIND", where, o.State)
			}
			return o.IntValue
		}
	}
	return 0
}

// TestEgressManifestSetsTCPNotSentLowat checks that the egress gateway sets
// TCP_NOTSENT_LOWAT on every socket it holds: the egress listener's, from
// atunnel, and every cluster's that dials destinations. A slow reader then
// backs data up behind Envoy's buffer limit rather than in a kernel send
// queue charged to the gateway's container. See
// docs/network-socket-memory.md.
func TestEgressManifestSetsTCPNotSentLowat(t *testing.T) {
	var bootstrap struct {
		StaticResources struct {
			Listeners []struct {
				Name          string         `json:"name"`
				SocketOptions []socketOption `json:"socket_options"`
			} `json:"listeners"`
			Clusters []struct {
				Name        string `json:"name"`
				ClusterType *struct {
					Name string `json:"name"`
				} `json:"cluster_type"`
				UpstreamBindConfig *struct {
					SocketOptions []socketOption `json:"socket_options"`
				} `json:"upstream_bind_config"`
			} `json:"clusters"`
		} `json:"static_resources"`
	}
	if err := yaml.Unmarshal([]byte(envoyConfig(t)), &bootstrap); err != nil {
		t.Fatalf("parsing envoy.yaml: %v", err)
	}
	listenerFound := false
	for _, l := range bootstrap.StaticResources.Listeners {
		if l.Name != "egress" {
			continue
		}
		listenerFound = true
		if got := notSentLowat(t, "listener egress", l.SocketOptions); got != 16384 {
			t.Errorf("listener egress sets TCP_NOTSENT_LOWAT to %d, want 16384", got)
		}
	}
	if !listenerFound {
		t.Error("found no egress listener; the manifest changed shape and this test is checking nothing")
	}
	found := 0
	for _, c := range bootstrap.StaticResources.Clusters {
		if c.ClusterType == nil || c.ClusterType.Name != "envoy.clusters.dynamic_forward_proxy" {
			continue
		}
		found++
		var opts []socketOption
		if c.UpstreamBindConfig != nil {
			opts = c.UpstreamBindConfig.SocketOptions
		}
		if got := notSentLowat(t, "cluster "+c.Name, opts); got != 16384 {
			t.Errorf("cluster %s sets TCP_NOTSENT_LOWAT to %d, want 16384", c.Name, got)
		}
	}
	if found == 0 {
		t.Fatal("found no dynamic_forward_proxy cluster; the manifest changed shape and this test is checking nothing")
	}
}
